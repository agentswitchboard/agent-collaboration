use approval_request_protocol::{PassOnReply, RaiseReply, RegisterRunReply, RunRefusal};
use fleet_approvals::{AdmittedDecider, ApprovalDecisionReply, ApprovalHolderScope, ApprovalRun};

use super::{
    approval_store_cases::{decide_input, first_holder, raise_and_commit, register, snapshot},
    fixtures::{enrollment, key, provider, router_run, session},
    native_approval_store::NativeApprovalStoreFixture,
};

#[tokio::test]
async fn holder_admission_refuses_wrong_or_ended_scope_before_request_and_session_lookup() {
    let fixture = NativeApprovalStoreFixture::open_fresh().await;
    let holder = first_holder();
    let requester = session(&holder.run.enrollment, 0xa171);
    fixture.store_enrollment(&holder, "live").await;
    fixture
        .store_session(&holder.organization_id, &requester, None, true)
        .await;
    let request = provider(&key(&holder.run, 1), &requester, "Authority");
    let (reply, evidence) = raise_and_commit(&fixture, &holder, &request).await;
    assert_eq!(
        reply,
        RaiseReply::AuthorityRefused {
            refusal: RunRefusal::RunNotRegistered {}
        },
    );
    assert!(evidence.session_route_reads.is_empty() && evidence.sql_writes.is_empty());
    assert!(
        evidence
            .sql_reads
            .iter()
            .all(|sql| !sql.contains("approval_requests"))
    );

    register(&fixture, &holder).await;
    let replay = fixture
        .apply(None, async |store| {
            store
                .register_approval_run(
                    &fleet_approvals::ApprovalHolderAdmission::for_fixture_scope(&holder),
                )
                .await
        })
        .await;
    assert_eq!(replay.0.unwrap().reply, RegisterRunReply::Registered {});
    assert!(replay.1.sql_writes.is_empty());

    let wrong_scope = ApprovalHolderScope {
        organization_id: holder.organization_id.clone(),
        run: ApprovalRun {
            enrollment: enrollment(0xa172, 0xa173),
            router_run: holder.run.router_run.clone(),
        },
    };
    fixture.store_enrollment(&wrong_scope, "live").await;
    let other_requester = session(&wrong_scope.run.enrollment, 0xa174);
    fixture
        .store_session(&holder.organization_id, &other_requester, None, true)
        .await;
    let wrong_holder = provider(&key(&wrong_scope.run, 1), &other_requester, "Foreign");
    let (reply, evidence) = raise_and_commit(&fixture, &wrong_scope, &wrong_holder).await;
    assert_eq!(
        reply,
        RaiseReply::AuthorityRefused {
            refusal: RunRefusal::RunNotRegistered {}
        },
    );
    assert!(evidence.session_route_reads.is_empty() && evidence.sql_writes.is_empty());

    let wrong_run = ApprovalRun {
        router_run: router_run(0xa175),
        ..holder.run.clone()
    };
    let wrong_binding = provider(&key(&wrong_run, 1), &requester, "Wrong run");
    let (reply, evidence) = raise_and_commit(&fixture, &holder, &wrong_binding).await;
    assert_eq!(
        reply,
        RaiseReply::AuthorityRefused {
            refusal: RunRefusal::RunRetired {}
        },
    );
    assert!(evidence.session_route_reads.is_empty() && evidence.sql_writes.is_empty());

    fixture.store_enrollment(&holder, "ended").await;
    let before = snapshot(&fixture).await;
    let (reply, evidence) = raise_and_commit(&fixture, &holder, &request).await;
    assert_eq!(
        reply,
        RaiseReply::AuthorityRefused {
            refusal: RunRefusal::EnrollmentEnded {}
        },
    );
    assert!(evidence.sql_reads.is_empty() && evidence.sql_writes.is_empty());
    assert!(evidence.session_route_reads.is_empty());
    assert_eq!(snapshot(&fixture).await, before);
}

#[tokio::test]
async fn an_ended_parent_remains_destination_but_cannot_act() {
    let fixture = NativeApprovalStoreFixture::open_fresh().await;
    let holder = first_holder();
    register(&fixture, &holder).await;
    let requester = session(&holder.run.enrollment, 0xa181);
    let parent = session(&holder.run.enrollment, 0xa182);
    fixture
        .store_session(&holder.organization_id, &requester, Some(&parent), false)
        .await;
    fixture
        .store_session(&holder.organization_id, &parent, None, false)
        .await;
    let request = provider(&key(&holder.run, 1), &requester, "Parent liveness");

    let (reply, evidence) = raise_and_commit(&fixture, &holder, &request).await;
    assert_eq!(reply, RaiseReply::SessionEnded {});
    assert!(evidence.sql_writes.is_empty());

    fixture
        .store_session(&holder.organization_id, &requester, Some(&parent), true)
        .await;
    let (reply, evidence) = raise_and_commit(&fixture, &holder, &request).await;
    assert!(matches!(reply, RaiseReply::Receipt { .. }));
    assert_eq!(evidence.session_liveness_reads, [requester]);
    assert_eq!(
        fixture
            .inspect_rows("SELECT session_id FROM approval_request_destinations")
            .await,
        [parent.session_id.as_str()]
    );
    let before = snapshot(&fixture).await;

    let pass = fixture
        .apply(None, async |store| {
            store
                .fixture_pass_on_approval(
                    &holder.organization_id,
                    parent.clone(),
                    request.request_id(),
                )
                .await
        })
        .await;
    assert_eq!(pass.0.unwrap().reply, PassOnReply::NotVisible {});
    assert!(pass.1.sql_reads.is_empty() && pass.1.sql_writes.is_empty());
    let decision = fixture
        .apply(None, async |store| {
            store
                .fixture_decide_approval(
                    &holder.organization_id,
                    AdmittedDecider::Session(parent.clone()),
                    decide_input(request.request_id(), 101, 1),
                )
                .await
        })
        .await;
    assert_eq!(
        decision.0.unwrap().reply,
        ApprovalDecisionReply::NotVisible {}
    );
    assert!(decision.1.sql_reads.is_empty() && decision.1.sql_writes.is_empty());
    assert_eq!(snapshot(&fixture).await, before);
}
