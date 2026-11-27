use approval_request_protocol::{
    ApprovalDecideReply, ApprovalListFilter, ApprovalListReply, ApprovalReadReply, RaiseReply,
};
use central_engine_model::CENTRAL_RECORD_RETENTION_MILLIS;
use fleet_approvals::{
    ApprovalApplyFailure, ApprovalCommitProof, ApprovalHolderScope, PersonApprovalStoreOutput,
    PersonApprovalStoreReply, apply_person_command,
};
use fleet_runtime_ports::Clock;
use fleet_signin::{
    admit_person_link_approval,
    approval_person_commands::{
        ApprovalDecideRequest, ApprovalListRequest, ApprovalPersonOperation, ApprovalReadRequest,
    },
    person_proof::PersonSocketAttachment,
};

use super::{
    approval_atomicity_raise_stop::waiting_approval,
    approval_store_atomicity::assert_loan_rollback_each_write,
    approval_store_cases::{fixture_with_owner, raise_and_commit},
    fixtures::{HOUR, answer, key, operation_id, person_id, provider},
    native_approval_store::{ApprovalTransactionEvidence, NativeApprovalStoreFixture},
};

async fn person_attachment(
    fixture: &NativeApprovalStoreFixture,
    holder: &ApprovalHolderScope,
) -> PersonSocketAttachment {
    let organization = &holder.organization_id;
    let person = person_id(11);
    fixture.provision_organization().await;
    fixture.alter_fixture(
        "INSERT INTO signin_authorization_floors VALUES ('019ac19f-aa00-7000-8000-00000000a100', 1, 'active', 'fixture-policy', 0, 1, 1);
         INSERT INTO signin_person_authorization_floors VALUES
           ('019ac19f-aa00-7000-8000-00000000a182', '019ac19f-aa00-7000-8000-00000000a100', '019ac19f-aa00-7000-8000-00000000000b', 1, 'active', 1, 1);").await;
    let now = fixture.clock.now().unix_millis();
    serde_json::from_value(serde_json::json!({
        "personId": person,
        "organizationId": organization,
        "permissions": ["approval:decide"],
        "authorizationSnapshotAtMs": now,
        "authorizationSnapshotValidUntilMs": now + 300_000,
        "personRevision": 1,
        "organizationRevision": 1,
        "sessionExpiresAtMs": now + 300_000,
        "secondFactor": null,
    }))
    .unwrap()
}

async fn apply_person(
    fixture: &NativeApprovalStoreFixture,
    failure: Option<usize>,
    attachment: &PersonSocketAttachment,
    command: &ApprovalPersonOperation,
) -> (
    Result<(ApprovalCommitProof, PersonApprovalStoreOutput), ApprovalApplyFailure>,
    ApprovalTransactionEvidence,
) {
    fixture
        .apply_on_loan(failure, async |loan, commit, ids| {
            let admitted = admit_person_link_approval(attachment, command.clone(), loan)
                .await?
                .expect("the person must be admitted on the applying loan");
            Ok(apply_person_command(admitted, &commit, ids)
                .await?
                .expect("the admitted person command must have a reply"))
        })
        .await
}

fn decision_reply(
    (proof, output): (ApprovalCommitProof, PersonApprovalStoreOutput),
) -> (ApprovalDecideReply, usize) {
    let PersonApprovalStoreReply::Decide(reply) = output.reply else {
        panic!("expected a person decision");
    };
    (
        reply.take_fixture_reply(&proof).unwrap(),
        output.effects.release(&proof).unwrap().endings.len(),
    )
}

async fn decide_with_existing_actor(
    fixture: &NativeApprovalStoreFixture,
    holder: &ApprovalHolderScope,
    request_id: &approval_request_protocol::ApprovalRequestId,
    choice: u8,
    writes: usize,
) -> (ApprovalDecideReply, usize) {
    let attachment = person_attachment(fixture, holder).await;
    // The existing person mapping is a prerequisite. The actor owner's raw
    // executor writes the first mapping outside this fixture's ordinal cuts.
    fixture.alter_fixture(
        "INSERT INTO actor_identities (actor_id, actor_kind, created_at, updated_at)
         VALUES ('019ac19f-aa00-7000-8000-00000000a183', 'person', '1970-01-01T00:00:00+00:00', '1970-01-01T00:00:00+00:00');
         INSERT INTO actor_persons VALUES
         ('019ac19f-aa00-7000-8000-00000000a183', '019ac19f-aa00-7000-8000-00000000000b', '1970-01-01T00:00:00+00:00', '1970-01-01T00:00:00+00:00');").await;
    let command = ApprovalPersonOperation::Decide(ApprovalDecideRequest {
        organization: holder.organization_id.clone(),
        request_id: request_id.clone(),
        operation_id: operation_id(101),
        answer: answer(choice),
    });
    let decided = assert_loan_rollback_each_write(fixture, writes, async |ordinal| {
        apply_person(fixture, ordinal, &attachment, &command).await
    })
    .await;
    let expected = decision_reply(decided);
    let (replay, evidence) = apply_person(fixture, None, &attachment, &command).await;
    assert_eq!(decision_reply(replay.unwrap()), (expected.0.clone(), 0));
    assert!(evidence.sql_writes.is_empty());
    expected
}

#[tokio::test]
async fn a_person_decision_rolls_back_its_ending_result_and_time() {
    let (fixture, holder, _, raise, _) = waiting_approval("People decides").await;
    let (reply, offers) =
        decide_with_existing_actor(&fixture, &holder, raise.request_id(), 1, 7).await;
    assert!(matches!(reply, ApprovalDecideReply::SavedDecision { .. }));
    assert_eq!(offers, 1);
}

#[tokio::test]
async fn a_person_decision_restores_the_pruned_log_row_when_the_transaction_rolls_back() {
    let (fixture, holder, requester) = fixture_with_owner().await;
    fixture
        .clock
        .set_millis(CENTRAL_RECORD_RETENTION_MILLIS + HOUR);
    let request = provider(
        &key(&holder.run, 1),
        &requester,
        "Decision with an old log row",
    );
    assert!(matches!(
        raise_and_commit(&fixture, &holder, &request).await.0,
        RaiseReply::Receipt { .. }
    ));
    fixture.alter_fixture(
        "INSERT INTO runtime_request_log
            (request_id, caller, content_digest, answer, created_at, updated_at)
         VALUES ('019ac19f-aa00-7000-8000-00000000a186',
                 '019ac19f-aa00-7000-8000-00000000a183',
                 'aged-fixture-result', '{}', '1970-01-01T00:00:00.000Z', '1970-01-01T00:00:00.000Z')",
    ).await;
    let log_ids = "SELECT request_id FROM runtime_request_log ORDER BY request_id";
    assert_eq!(
        fixture.inspect_rows(log_ids).await,
        ["019ac19f-aa00-7000-8000-00000000a186"]
    );

    let (reply, offers) =
        decide_with_existing_actor(&fixture, &holder, request.request_id(), 1, 7).await;

    assert!(matches!(reply, ApprovalDecideReply::SavedDecision { .. }));
    assert_eq!(offers, 1);
    assert_eq!(
        fixture.inspect_rows(log_ids).await,
        [operation_id(101).as_str()]
    );
}

#[tokio::test]
async fn a_late_person_decision_rolls_back_its_timeout_and_retained_reply() {
    let (fixture, holder, _, raise, _) = waiting_approval("People decide late").await;
    fixture.clock.set_millis(HOUR);
    let (reply, offers) =
        decide_with_existing_actor(&fixture, &holder, raise.request_id(), 1, 7).await;
    assert_eq!(reply, ApprovalDecideReply::TooLate {});
    assert_eq!(offers, 1);
}

#[tokio::test]
async fn an_invalid_person_answer_rolls_back_its_result_without_ending_the_request() {
    let (fixture, holder, _, raise, _) = waiting_approval("People invalid answer").await;
    let (reply, offers) =
        decide_with_existing_actor(&fixture, &holder, raise.request_id(), 6, 3).await;
    assert!(matches!(reply, ApprovalDecideReply::Invalid { .. }));
    assert_eq!(offers, 0);
    assert_eq!(
        fixture
            .inspect_rows("SELECT record_state FROM approval_requests")
            .await,
        ["waiting"]
    );
}

#[tokio::test]
async fn a_person_read_at_the_deadline_rolls_back_its_timeout() {
    let (fixture, holder, _, raise, _) = waiting_approval("People read late").await;
    fixture.clock.set_millis(HOUR);
    let attachment = person_attachment(&fixture, &holder).await;
    let command = ApprovalPersonOperation::Read(ApprovalReadRequest {
        organization: holder.organization_id,
        request_id: raise.request_id().clone(),
    });
    let (proof, output) = assert_loan_rollback_each_write(&fixture, 4, async |ordinal| {
        apply_person(&fixture, ordinal, &attachment, &command).await
    })
    .await;
    let PersonApprovalStoreReply::Read(reply) = output.reply else {
        panic!("expected a People read");
    };
    assert!(matches!(
        reply.take_fixture_reply(&proof).unwrap(),
        ApprovalReadReply::Request { .. }
    ));
    assert_eq!(output.effects.release(&proof).unwrap().endings.len(), 1);
}

#[tokio::test]
async fn a_people_page_rolls_back_every_timeout_together() {
    let (fixture, holder, requester, first, _) = waiting_approval("First expired page row").await;
    let second = provider(&key(&holder.run, 2), &requester, "Second expired page row");
    assert!(matches!(
        raise_and_commit(&fixture, &holder, &second).await.0,
        RaiseReply::Receipt { .. }
    ));
    fixture.clock.set_millis(HOUR);
    let attachment = person_attachment(&fixture, &holder).await;
    let command = ApprovalPersonOperation::List(ApprovalListRequest {
        organization: holder.organization_id,
        filter: ApprovalListFilter::All,
        cursor: None,
    });
    let (proof, output) = assert_loan_rollback_each_write(&fixture, 7, async |ordinal| {
        apply_person(&fixture, ordinal, &attachment, &command).await
    })
    .await;
    let PersonApprovalStoreReply::List(reply) = output.reply else {
        panic!("expected a People page");
    };
    let ApprovalListReply::Page { items, next } = reply.take_fixture_reply(&proof).unwrap();
    assert!(next.is_none());
    let ids: Vec<_> = items.iter().map(|item| &item.request_id).collect();
    assert_eq!(ids, [second.request_id(), first.request_id()]);
    assert_eq!(output.effects.release(&proof).unwrap().endings.len(), 2);
}
