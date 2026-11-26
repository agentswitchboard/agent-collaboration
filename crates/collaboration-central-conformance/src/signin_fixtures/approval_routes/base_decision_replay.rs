//! Replay the signed writer's captured rows and compare a fresh socket write.

use central_storage::SqlValue;
use serde::Deserialize;
use serde_json::json;

use super::{
    AnswerClass, InitialApproval, NOW_SECONDS, ORGANIZATION, Person, PersonApprovalHarness,
    assert_no_proof_rows, decide, expect, id, publish_person_floor, saved, seed,
};
use crate::ConformanceResult;

#[derive(Deserialize)]
struct BaseDecisionRows {
    approval_requests: Vec<SqlValue>,
    approval_request_destinations: Vec<SqlValue>,
    actor_identities: Vec<SqlValue>,
    actor_persons: Vec<SqlValue>,
    runtime_request_log: Vec<SqlValue>,
}

impl BaseDecisionRows {
    fn capture() -> ConformanceResult<Self> {
        Ok(serde_json::from_str(include_str!(
            "base-signed-decision-rows.json"
        ))?)
    }

    fn raw_decision(&self) -> ConformanceResult<&[SqlValue]> {
        self.runtime_request_log
            .get(2..4)
            .ok_or_else(|| "base operation row lacks its digest and answer".into())
    }

    async fn restore(&self, host: &impl PersonApprovalHarness) -> ConformanceResult<()> {
        host.write(
            "INSERT INTO approval_requests
             (approval_row_id,organization_id,machine_id,enrollment_id,router_run_id,request_id,contents_digest,
              record_state,request_kind,holder_session_id,offer_json,invocation_digest,waiting_status,first_saved_at,
              deadline_ms,stop_family,stop_cause,ended_at,ending_json,decided_operation_id,delivery_state,
              created_at,updated_at,erase_after_ms,compact_ending_json)
             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            self.approval_requests.clone(),
        ).await?;
        host.write(
            "INSERT INTO approval_request_destinations
             (approval_destination_id,approval_row_id,organization_id,destination_kind,session_machine_id,
              session_enrollment_id,session_id,raised_notice_at,created_at,updated_at,request_id,position,
              first_read_at)
             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            self.approval_request_destinations.clone(),
        ).await?;
        host.write(
            "INSERT INTO actor_identities (actor_id,actor_kind,notice_cadence,created_at,updated_at) VALUES (?,?,?,?,?)",
            self.actor_identities.clone(),
        ).await?;
        host.write(
            "INSERT INTO actor_persons (actor_id,person_id,created_at,updated_at) VALUES (?,?,?,?)",
            self.actor_persons.clone(),
        )
        .await?;
        host.write(
            "INSERT INTO runtime_request_log (request_id,caller,content_digest,answer,created_at,updated_at,approval_row_id)
             VALUES (?,?,?,?,?,?,?)",
            self.runtime_request_log.clone(),
        ).await
    }

    async fn assert_preserved(&self, host: &impl PersonApprovalHarness) -> ConformanceResult<()> {
        for (query, expected) in [
            ("SELECT * FROM approval_requests", &self.approval_requests),
            (
                "SELECT * FROM approval_request_destinations",
                &self.approval_request_destinations,
            ),
            ("SELECT * FROM actor_identities", &self.actor_identities),
            ("SELECT * FROM actor_persons", &self.actor_persons),
            (
                "SELECT request_id,caller,content_digest,answer,created_at,updated_at,approval_row_id FROM runtime_request_log",
                &self.runtime_request_log,
            ),
        ] {
            assert_eq!(
                host.read(query).await?.as_slice(),
                [expected.as_slice()],
                "{query}"
            );
        }
        Ok(())
    }
}

pub(super) async fn replay_base_rows(host: &impl PersonApprovalHarness) -> ConformanceResult<()> {
    let captured = BaseDecisionRows::capture()?;
    captured.restore(host).await?;
    captured.assert_preserved(host).await?;
    let mut owner = Person::owner(ORGANIZATION, NOW_SECONDS)?;
    let request = id(10);
    let operation = id(100);
    let answer = AnswerClass::Once.answer();
    let call = decide(&request, &operation, &answer);

    let first = call.send(host, &owner).await?;
    expect(first.clone(), saved(&answer))?;
    let [_, SqlValue::Text(raw_answer)] = captured.raw_decision()? else {
        return Err("base fixture does not contain the raw answer text".into());
    };
    assert_eq!(first.clone().answer()?, raw_answer.as_bytes());
    expect(
        decide(
            &request,
            &operation,
            &json!({"kind":"approval","optionId":2}),
        )
        .send(host, &owner)
        .await?,
        json!({"kind":"operationConflict"}),
    )?;

    publish_person_floor(host, 5, "active").await?;
    owner.revision = 5;
    owner.permissions.retain(|grant| grant != "approval:decide");
    assert_eq!(call.send(host, &owner).await?, first);
    captured.assert_preserved(host).await?;
    assert_no_proof_rows(host).await
}

pub(super) async fn fresh_write_matches_base(
    host: &impl PersonApprovalHarness,
) -> ConformanceResult<()> {
    let captured = BaseDecisionRows::capture()?;
    let owner = Person::owner(ORGANIZATION, NOW_SECONDS)?;
    let request = seed(host, 10, AnswerClass::Once, InitialApproval::Waiting).await?;
    let answer = AnswerClass::Once.answer();
    assert!(
        host.read("SELECT * FROM runtime_request_log")
            .await?
            .is_empty()
    );

    expect(
        decide(&request, &id(100), &answer)
            .send(host, &owner)
            .await?,
        saved(&answer),
    )?;
    let fresh = host
        .read("SELECT content_digest,answer FROM runtime_request_log")
        .await?;
    assert_eq!(fresh.as_slice(), [captured.raw_decision()?]);
    assert_no_proof_rows(host).await
}
