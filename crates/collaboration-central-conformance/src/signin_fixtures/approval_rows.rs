//! Seed the owning approval tables while exercising the real C3 HTTP entry.

use approval_request_protocol::{
    ApprovalWindow, CancelReason, CentralTime, DeliverEndingRequest, Ending, HolderStopCause,
    Raise, RequestKind,
};
use central_storage::SqlValue;
use chrono::SecondsFormat;
use serde_json::{Value, json};

use super::approval_harness::PersonApprovalHarness;
use super::{NOW_SECONDS, ORGANIZATION};
use crate::ConformanceResult;

const MACHINE: &str = "019f0000-0000-7000-8000-00000000c301";
const ENROLLMENT: &str = "019f0000-0000-7000-8000-00000000c302";
const ROUTER_RUN: &str = "019f0000-0000-7000-8000-00000000c303";
const HOLDER: &str = "019f0000-0000-7000-8000-00000000c304";
const PARENT: &str = "019f0000-0000-7000-8000-00000000c305";

pub(super) fn id(serial: u32) -> String {
    format!("019f0000-0000-7000-8000-{serial:012x}")
}

pub(super) fn time(offset_seconds: i64) -> ConformanceResult<CentralTime> {
    CentralTime::from_timestamp(NOW_SECONDS + offset_seconds, 0)
        .ok_or_else(|| "fixture time".into())
}

fn stored_time(at: CentralTime) -> String {
    at.to_rfc3339_opts(SecondsFormat::Millis, true)
}

pub(super) fn text(value: impl ToString) -> SqlValue {
    SqlValue::Text(value.to_string())
}

#[derive(Clone, Copy)]
pub enum AnswerClass {
    Once,
    Turn,
    Session,
    Question,
    KeptUntilChanged,
    UnverifiedLifetime,
    HeldStartAllow,
    HeldStartDecline,
    HeldEndAllow,
    HeldEndDecline,
}

impl AnswerClass {
    pub(super) const fn requires_factor(self) -> bool {
        match self {
            Self::Once | Self::Turn | Self::Session | Self::Question => false,
            Self::KeptUntilChanged
            | Self::UnverifiedLifetime
            | Self::HeldStartAllow
            | Self::HeldStartDecline
            | Self::HeldEndAllow
            | Self::HeldEndDecline => true,
        }
    }

    pub(super) fn answer(self) -> Value {
        match self {
            Self::Once => json!({"kind":"approval","optionId":1}),
            Self::Turn => json!({"kind":"approval","optionId":3}),
            Self::Session => json!({"kind":"approval","optionId":4}),
            Self::KeptUntilChanged => json!({"kind":"approval","optionId":5}),
            Self::UnverifiedLifetime => json!({"kind":"approval","optionId":6}),
            Self::Question => json!({"kind":"question","answer":{"kind":"answered",
                "fields":[{"fieldId":1,"value":{"kind":"boolean","value":true}}]}}),
            Self::HeldStartAllow | Self::HeldEndAllow => {
                json!({"kind":"unprovenCall","decision":"allow"})
            }
            Self::HeldStartDecline | Self::HeldEndDecline => {
                json!({"kind":"unprovenCall","decision":"decline"})
            }
        }
    }

    fn raise(self, request_id: &str) -> ConformanceResult<Raise> {
        let provider = json!({"routerRun":ROUTER_RUN, "sessionId":HOLDER});
        let shape = match self {
            Self::Once
            | Self::Turn
            | Self::Session
            | Self::KeptUntilChanged
            | Self::UnverifiedLifetime => {
                json!({"kind":"providerApproval","requestId":request_id,"holder":provider,"offer":{
                    "title":"Read the plan","description":"Private offer details","subject":null,
                    "options":[
                        {"id":1,"label":"Once","effect":"allow","lasts":{"kind":"thisRequest"}},
                        {"id":2,"label":"Decline","effect":"decline","lasts":{"kind":"thisRequest"}},
                        {"id":3,"label":"This turn","effect":"allow","lasts":{"kind":"thisTurn","kept":"agentMemory"}},
                        {"id":4,"label":"This session","effect":"allow","lasts":{"kind":"thisSession","kept":"agentMemory"}},
                        {"id":5,"label":"Always","effect":"allow","lasts":{"kind":"keptUntilChanged","kept":"agentRulesFile"}},
                        {"id":6,"label":"Provider default","effect":"allow","lasts":{"kind":"notVerified"}}
                    ]
                }})
            }
            Self::Question => {
                json!({"kind":"providerQuestion","requestId":request_id,"holder":provider,
                "offer":{"prompt":"Private question","fields":[{"kind":"boolean","fieldId":1,
                    "label":"Continue","description":null,"required":true}]}})
            }
            Self::HeldStartAllow
            | Self::HeldStartDecline
            | Self::HeldEndAllow
            | Self::HeldEndDecline => {
                let operation = match self {
                    Self::HeldStartAllow | Self::HeldStartDecline => json!({
                        "kind":"startSession","agentProgram":"claude","workingFolder":"/workspace",
                        "mode":"default","workFolders":[]
                    }),
                    Self::HeldEndAllow | Self::HeldEndDecline => {
                        json!({"kind":"endSessionForGood","sessionId":HOLDER})
                    }
                    Self::Once
                    | Self::Turn
                    | Self::Session
                    | Self::Question
                    | Self::KeptUntilChanged
                    | Self::UnverifiedLifetime => {
                        return Err("expected a held call".into());
                    }
                };
                json!({"kind":"unprovenCall","requestId":request_id,
                    "holder":{"routerRun":ROUTER_RUN},
                    "invocation":approval_request_protocol::InvocationDigest::from_bound_bytes(b"fixture held call"),
                    "offer":{"operation":operation}})
            }
        };
        Ok(serde_json::from_value(shape)?)
    }
}

#[derive(Clone, Copy)]
pub(super) enum InitialApproval {
    Waiting,
    Hidden,
    PastDeadline,
    Cancelled,
}

pub(super) async fn seed(
    host: &impl PersonApprovalHarness,
    serial: u32,
    class: AnswerClass,
    initial: InitialApproval,
) -> ConformanceResult<String> {
    let request_id = id(serial);
    let row_id = id(1000 + serial);
    let destination_id = id(2000 + serial);
    let raise = class.raise(&request_id)?;
    let raised = time(if matches!(initial, InitialApproval::PastDeadline) {
        -3600
    } else {
        -10
    })?;
    let window = ApprovalWindow::from_first_save(raised)?;
    let (state, ended_at, ending, delivery) = match initial {
        InitialApproval::Cancelled => (
            "ended",
            text(stored_time(time(-5)?)),
            text(serde_json::to_string(&DeliverEndingRequest::from_raise(
                &raise,
                Ending::Cancelled {
                    reason: CancelReason::Stopped {
                        cause: HolderStopCause::TurnStopped,
                    },
                },
            )?)?),
            text("closed_without_receipt"),
        ),
        InitialApproval::Waiting | InitialApproval::Hidden | InitialApproval::PastDeadline => {
            ("waiting", SqlValue::Null, SqlValue::Null, SqlValue::Null)
        }
    };
    let kind = match raise.kind() {
        RequestKind::ProviderApproval => "provider_approval",
        RequestKind::ProviderQuestion => "provider_question",
        RequestKind::UnprovenCall => "unproven_call",
    };
    let invocation = match &raise {
        Raise::UnprovenCall { invocation, .. } => text(invocation.as_str()),
        Raise::ProviderApproval { .. } | Raise::ProviderQuestion { .. } => SqlValue::Null,
    };
    host.write(
        "INSERT INTO approval_requests
         (approval_row_id,organization_id,machine_id,enrollment_id,router_run_id,request_id,contents_digest,
          record_state,request_kind,holder_session_id,offer_json,invocation_digest,waiting_status,
          first_saved_at,deadline_ms,ended_at,ending_json,delivery_state,created_at,updated_at)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,'pending',?13,?14,?15,?16,?17,?13,COALESCE(?15,?13))",
        vec![text(&row_id), text(ORGANIZATION), text(MACHINE), text(ENROLLMENT), text(ROUTER_RUN),
            text(&request_id), text(raise.contents_digest()?.as_str()), text(state), text(kind),
            raise.requesting_session().map_or(SqlValue::Null, |id|text(id.as_str())),
            text(serde_json::to_string(&raise.offer_view())?), invocation,
            text(stored_time(raised)), SqlValue::Integer(window.deadline().timestamp_millis().try_into()?),
            ended_at, ending, delivery],
    ).await?;
    if matches!(initial, InitialApproval::Hidden) {
        host.write(
            "INSERT INTO approval_request_destinations
             (approval_destination_id,approval_row_id,organization_id,destination_kind,session_machine_id,
              session_enrollment_id,session_id,raised_notice_at,created_at,updated_at)
             VALUES (?1,?2,?3,'parent_session',?4,?5,?6,?7,?7,?7)",
            vec![text(destination_id),text(row_id),text(ORGANIZATION),text(MACHINE),text(ENROLLMENT),
                 text(PARENT),text(stored_time(raised))],
        ).await?;
    } else {
        host.write(
            "INSERT INTO approval_request_destinations
             (approval_destination_id,approval_row_id,organization_id,destination_kind,created_at,updated_at)
             VALUES (?1,?2,?3,'people',?4,?4)",
            vec![text(destination_id),text(row_id),text(ORGANIZATION),text(stored_time(raised))],
        ).await?;
    }
    Ok(request_id)
}
