//! Force each competitor through its production entrance while signed Decide is held.

use crate::fixture_link_reader::read_fixture_frame;
use std::future::Future;

use approval_request_protocol::{
    CancelReason, CentralTime, DeliverEndingRequest, Ending, HolderStopCause, Raise,
    RegisterRunReply, SessionCallDecision, StopReceiptEnding, StopReply,
};
use central_link_protocol::{
    MachineLinkCloseCode, TypedCallResult, approval_rpc::ApprovalHolderTarget,
};
use central_storage::{SqlValue, Statement};
use futures_util::{FutureExt, SinkExt, StreamExt};
use reqwest_websocket::{Message, WebSocket};
use serde_json::{Value, json};

use super::{OWNER, OWNER_SUBJECT, POLICY, Person, fresh_request_id};
use crate::{
    ConformanceResult as Result,
    approval_link_scenarios::{
        ApprovalLinkFixture, EnrollmentLoginEnd, heartbeat, held_start, raise, read_request,
        receive_decision, register_run, renew, require_receipt, stop, stop_request,
    },
};

const DECIDE: &str = "/organization-admin/v1/approval-decide";
const REVISIONS: &str = "/internal/organization-authorization-revisions";

#[derive(Clone, Copy)]
pub enum DecisionCompetitor {
    Stop,
    Deadline,
    EnrollmentEnd,
}

#[derive(Clone, Copy)]
pub enum DecisionRaceOrder {
    CompetitorFirst,
    PersonFirst,
}

#[derive(Clone, Copy)]
pub enum PersonApprovalPause {
    BeforeTurn,
    AfterCommit,
}

#[derive(Clone, Copy)]
pub enum RacePauseStep {
    Arm,
    Wait,
    Release,
}

pub trait PersonApprovalRaceFixture {
    type Link: ApprovalLinkFixture;

    fn link(&self) -> &Self::Link;

    fn post_private(
        &self,
        path: &str,
        body: &Value,
        headers: Vec<(&'static str, String)>,
    ) -> impl Future<Output = Result<(u16, Vec<u8>)>>;

    fn control_pause(
        &self,
        point: PersonApprovalPause,
        step: RacePauseStep,
    ) -> impl Future<Output = Result<()>>;

    /// Invoke the normal alarm completion even if the winning decision withdrew
    /// its physical alarm in the meantime.
    fn redeliver_alarm(&self) -> impl Future<Output = Result<()>>;
}

pub async fn check_decision_race(
    fixture: &impl PersonApprovalRaceFixture,
    competitor: DecisionCompetitor,
    order: DecisionRaceOrder,
) -> Result<()> {
    let link = fixture.link();
    if matches!(competitor, DecisionCompetitor::Deadline) {
        let to_boundary = u64::try_from((-link.now_seconds()).rem_euclid(300))?;
        if to_boundary != 0 {
            link.advance(to_boundary).await?;
        }
    }
    let target = link.target()?;
    let organization = &target.organization_id;
    let mut socket = link.connect().await?;
    assert_eq!(
        register_run(&mut socket, organization, &target.router_run).await?,
        RegisterRunReply::Registered {}
    );
    let request = held_start(&target.router_run, 0)?;
    let receipt = require_receipt(raise(&mut socket, organization, request.clone()).await?)?;
    assert_eq!(
        read_request(link, &request).await?,
        [text("waiting"), SqlValue::Null]
    );
    publish_person_floors(fixture, organization.as_str()).await?;
    if matches!(competitor, DecisionCompetitor::Deadline) {
        let deadline = receipt.window.deadline().timestamp();
        assert_eq!(deadline.rem_euclid(300), 0);
        for _ in 0..7 {
            link.advance(500).await?;
            renew(&mut socket, link.refresh().await?).await?;
        }
        link.advance(u64::try_from(deadline - 1 - link.now_seconds())?)
            .await?;
        assert_eq!(link.now_seconds(), deadline - 1);
        assert_eq!(
            read_request(link, &request).await?,
            [text("waiting"), SqlValue::Null]
        );
    }

    let person_starts_at = link.now_seconds();
    let owner = Person::owner(organization.as_str(), person_starts_at)?;
    let operation = fresh_request_id(person_starts_at)?;
    let answer = json!({"kind": "unprovenCall", "decision": "allow"});
    let body = json!({
        "organization": organization, "requestId": request.request_id(),
        "operationId": operation, "answer": answer,
    });
    let intent = json!({
        "v": 1, "operation": "ApprovalDecide", "organization": organization,
        "target": request.request_id(), "operationId": operation, "params": {"answer": answer},
    });
    let headers = owner.headers(
        DECIDE,
        &intent,
        &fresh_request_id(person_starts_at)?,
        person_starts_at,
    )?;
    let person_first = matches!(order, DecisionRaceOrder::PersonFirst);
    let pause = match order {
        DecisionRaceOrder::CompetitorFirst => PersonApprovalPause::BeforeTurn,
        DecisionRaceOrder::PersonFirst => PersonApprovalPause::AfterCommit,
    };
    fixture.control_pause(pause, RacePauseStep::Arm).await?;
    let mut deciding = std::pin::pin!(fixture.post_private(DECIDE, &body, headers));
    let held = tokio::select! {
        reached = fixture.control_pause(pause, RacePauseStep::Wait) => reached,
        early = &mut deciding => Err(format!("person Decide returned before its pause: {early:?}").into()),
    };

    let observed: Result<Winner> = async {
        held?;
        if person_first {
            receive_decision(
                link,
                &mut socket,
                target.clone(),
                &request,
                person_ending()?,
            )
            .await?;
            heartbeat(&mut socket).await?;
            let winner = read_winner(link, &request).await?;
            winner.require(
                &request,
                person_ending()?,
                person_starts_at,
                Some(&operation),
                "received_by_holder",
            )?;
            run_competitor(fixture, competitor, order, &mut socket, &target, &request).await?;
            Ok(winner)
        } else {
            run_competitor(fixture, competitor, order, &mut socket, &target, &request).await?;
            if matches!(competitor, DecisionCompetitor::Deadline) {
                receive_decision(
                    link,
                    &mut socket,
                    target.clone(),
                    &request,
                    Ending::TimedOut {},
                )
                .await?;
            }
            let winner = read_winner(link, &request).await?;
            winner.require(
                &request,
                ending_from_competitor(competitor),
                link.now_seconds(),
                None,
                match competitor {
                    DecisionCompetitor::Deadline => "received_by_holder",
                    DecisionCompetitor::Stop | DecisionCompetitor::EnrollmentEnd => {
                        "closed_without_receipt"
                    }
                },
            )?;
            Ok(winner)
        }
    }
    .await;
    let released = fixture.control_pause(pause, RacePauseStep::Release).await;
    let winner = observed?;
    released?;

    let expected = if person_first {
        json!({"kind": "savedDecision", "decision": {
            "by": {"kind": "person", "personId": OWNER}, "answer": answer,
        }})
    } else {
        json!({"kind": "tooLate"})
    };
    let actual: Value = serde_json::from_slice(&expect_ok(deciding.await?)?)?;
    assert_eq!(actual, expected);
    assert_eq!(read_winner(link, &request).await?, winner);
    match competitor {
        DecisionCompetitor::Stop | DecisionCompetitor::Deadline => heartbeat(&mut socket).await,
        DecisionCompetitor::EnrollmentEnd => require_inactive(&mut socket).await,
    }
}

async fn publish_person_floors(
    fixture: &impl PersonApprovalRaceFixture,
    organization: &str,
) -> Result<()> {
    for floor in [
        json!({
            "kind": "organization", "organizationId": organization, "revision": 3,
            "state": "active", "permissionPolicyDigest": POLICY,
        }),
        json!({
            "kind": "person", "organizationId": organization, "personId": OWNER,
            "subjectId": OWNER_SUBJECT, "revision": 4, "state": "active",
        }),
    ] {
        expect_ok(fixture.post_private(REVISIONS, &floor, Vec::new()).await?)?;
    }
    Ok(())
}

fn expect_ok((status, body): (u16, Vec<u8>)) -> Result<Vec<u8>> {
    if status != 200 {
        return Err(format!(
            "expected HTTP 200, got {status}: {}",
            String::from_utf8_lossy(&body)
        )
        .into());
    }
    Ok(body)
}

async fn run_competitor(
    fixture: &impl PersonApprovalRaceFixture,
    competitor: DecisionCompetitor,
    order: DecisionRaceOrder,
    socket: &mut WebSocket,
    target: &ApprovalHolderTarget,
    request: &Raise,
) -> Result<()> {
    let link = fixture.link();
    match competitor {
        DecisionCompetitor::Stop => {
            let ending = match order {
                DecisionRaceOrder::CompetitorFirst => StopReceiptEnding::Cancelled {
                    reason: CancelReason::Stopped {
                        cause: HolderStopCause::CallWithdrawn,
                    },
                },
                DecisionRaceOrder::PersonFirst => StopReceiptEnding::DecidedEarlier {},
            };
            assert_eq!(
                stop(socket, &target.organization_id, stop_request(request)?).await?,
                TypedCallResult::Answered {
                    reply: StopReply::Stands { ending }
                }
            );
        }
        DecisionCompetitor::Deadline => {
            link.advance(1).await?;
            match order {
                DecisionRaceOrder::CompetitorFirst => link.observe_due_alarm().await?,
                DecisionRaceOrder::PersonFirst => fixture.redeliver_alarm().await?,
            }
        }
        DecisionCompetitor::EnrollmentEnd => {
            link.end_enrollment_through_login(EnrollmentLoginEnd::Supersede)
                .await?;
        }
    }
    Ok(())
}

fn person_ending() -> Result<Ending> {
    Ok(Ending::UnprovenCallDecided {
        decision: SessionCallDecision::Allow,
        person_id: OWNER.parse()?,
    })
}

const fn ending_from_competitor(competitor: DecisionCompetitor) -> Ending {
    match competitor {
        DecisionCompetitor::Stop => Ending::Cancelled {
            reason: CancelReason::Stopped {
                cause: HolderStopCause::CallWithdrawn,
            },
        },
        DecisionCompetitor::Deadline => Ending::TimedOut {},
        DecisionCompetitor::EnrollmentEnd => Ending::Cancelled {
            reason: CancelReason::EnrollmentEnded {},
        },
    }
}

async fn require_inactive(socket: &mut WebSocket) -> Result<()> {
    let frame = match socket.next().now_or_never() {
        Some(frame) => frame,
        None => {
            // A missing invalidation responds to this heartbeat instead of closing.
            let _ = socket.send(Message::Text("ping".into())).await;
            read_fixture_frame(socket, "Inactive authority close").await?
        }
    };
    if !matches!(frame, Some(Ok(Message::Close { code, .. }))
        if MachineLinkCloseCode::from_code(code.into()) == Some(MachineLinkCloseCode::Inactive))
    {
        return Err(
            format!("expected Inactive with no intervening ending offer, got {frame:?}").into(),
        );
    }
    Ok(())
}

#[derive(Debug, Eq, PartialEq)]
struct Winner {
    ending: String,
    ended_at: String,
    deciding_operation: Option<String>,
    delivery: String,
}

impl Winner {
    fn require(
        &self,
        request: &Raise,
        ending: Ending,
        at: i64,
        operation: Option<&str>,
        delivery: &str,
    ) -> Result<()> {
        let ended_at = self.ended_at.parse::<CentralTime>()?;
        if ended_at.timestamp() != at
            || self.deciding_operation.as_deref() != operation
            || self.delivery != delivery
        {
            return Err(format!(
                "unexpected durable winner {self:?}, expected {at} {operation:?} {delivery}"
            )
            .into());
        }

        let stored: DeliverEndingRequest = serde_json::from_str(&self.ending)?;
        if stored.request_id() != request.request_id()
            || stored.contents_digest() != &request.contents_digest()?
            || stored.into_ending() != ending
        {
            return Err("durable ending does not match the winning command".into());
        }
        Ok(())
    }
}

async fn read_winner(fixture: &impl ApprovalLinkFixture, request: &Raise) -> Result<Winner> {
    let result = fixture
        .read(Statement {
            sql: "SELECT record_state, ending_json, ended_at, decided_operation_id, delivery_state FROM approval_requests WHERE request_id=?",
            args: vec![text(request.request_id().as_str())],
        })
        .await?;
    let [row] = result.rows.as_slice() else {
        return Err("expected exactly one durable approval row".into());
    };
    let [
        SqlValue::Text(state),
        SqlValue::Text(ending),
        SqlValue::Text(ended_at),
        operation,
        SqlValue::Text(delivery),
    ] = row.as_slice()
    else {
        return Err(format!("approval winner is not entirely recorded: {row:?}").into());
    };
    assert_eq!(state, "ended");
    let deciding_operation = match operation {
        SqlValue::Null => None,
        SqlValue::Text(operation) => Some(operation.clone()),
        other => return Err(format!("unexpected stored decision operation: {other:?}").into()),
    };
    Ok(Winner {
        ending: ending.clone(),
        ended_at: ended_at.clone(),
        deciding_operation,
        delivery: delivery.clone(),
    })
}

fn text(value: &str) -> SqlValue {
    SqlValue::Text(value.to_owned())
}

#[path = "person_session_approval_race.rs"]
mod person_session;
pub use person_session::check_person_session_race;
