use approval_request_protocol::{
    Answer, ApprovalOffer, CallOffer, CallOperationName, Description, HolderReport,
    InvocationDigest, KeptWhere, Label, OfferedOption, OfferedOptionId, OfferedOptions,
    OptionDuration, OptionEffect, ParkedCallHolder, ProviderHolder, ProviderStopCause, Raise,
    ReportDeliveryRequest, StopRequest, Title,
};
use central_engine_model::{Context, EngineTime, IdSource, Machine, TransitionResult};
use workspace_ids::{ApprovalRequestId, PersonId, RouterRunId, SessionRef};

use crate::{
    AdmittedDecider, ApprovalDestination, ApprovalEffect, ApprovalEvent, ApprovalLifecycle,
    ApprovalState, FactorStanding, PassOnAuthority,
};

pub(super) const DEADLINE: i64 = 3_600_000;

pub(super) struct UnusedIds;

impl IdSource for UnusedIds {
    fn next_id(&mut self) -> uuid::Uuid {
        panic!("approval identifiers are all inputs")
    }
}

fn fixture_uuid(suffix: u8) -> String {
    format!("019ac19f-aa00-7000-8000-0000000000{suffix:02x}")
}

pub(super) fn request_id() -> ApprovalRequestId {
    fixture_uuid(1).parse().unwrap()
}

pub(super) fn other_request_id() -> ApprovalRequestId {
    fixture_uuid(10).parse().unwrap()
}

pub(super) fn router_run() -> RouterRunId {
    fixture_uuid(2).parse().unwrap()
}

pub(super) fn other_run() -> RouterRunId {
    fixture_uuid(3).parse().unwrap()
}

pub(super) fn person_id() -> PersonId {
    fixture_uuid(4).parse().unwrap()
}

pub(super) fn session(suffix: u8) -> SessionRef {
    SessionRef {
        machine_id: fixture_uuid(5).parse().unwrap(),
        session_id: fixture_uuid(suffix).parse().unwrap(),
    }
}

pub(super) fn requester() -> SessionRef {
    session(6)
}

pub(super) fn parent() -> SessionRef {
    session(7)
}

pub(super) fn observer() -> SessionRef {
    session(8)
}

pub(super) fn destinations() -> Vec<ApprovalDestination> {
    vec![
        ApprovalDestination::Session(parent()),
        ApprovalDestination::Session(observer()),
    ]
}

pub(super) fn time(milliseconds: i64) -> EngineTime {
    EngineTime::from_timestamp_millis(milliseconds).unwrap()
}

pub(super) fn step(
    state: &ApprovalState,
    event: ApprovalEvent,
    at: i64,
) -> TransitionResult<ApprovalLifecycle> {
    ApprovalLifecycle::transition(
        state,
        event,
        &mut Context {
            now: time(at),
            ids: &mut UnusedIds,
        },
    )
}

pub(super) fn apply(
    state: &ApprovalState,
    event: ApprovalEvent,
    at: i64,
) -> (ApprovalState, Vec<ApprovalEffect>) {
    step(state, event, at).unwrap()
}

pub(super) fn approval_raise(title: &str) -> Raise {
    Raise::ProviderApproval {
        request_id: request_id(),
        holder: ProviderHolder {
            session: requester(),
            router_run: router_run(),
        },
        offer: ApprovalOffer {
            title: Title::new(title).unwrap(),
            description: None,
            subject: None,
            options: options(),
        },
    }
}

pub(super) fn call_raise() -> Raise {
    Raise::UnprovenCall {
        request_id: request_id(),
        holder: ParkedCallHolder {
            router_run: router_run(),
        },
        offer: CallOffer {
            operation: CallOperationName::new("read_record").unwrap(),
            summary: Description::new("Read a record").unwrap(),
            invocation: InvocationDigest::try_from("a".repeat(64)).unwrap(),
            options: options(),
        },
    }
}

fn options() -> OfferedOptions {
    OfferedOptions::new(vec![
        offered(
            "once",
            OptionDuration::ThisRequest,
            KeptWhere::Nowhere,
            OptionEffect::Allow,
        ),
        offered(
            "decline",
            OptionDuration::ThisRequest,
            KeptWhere::Nowhere,
            OptionEffect::Decline,
        ),
        offered(
            "kept",
            OptionDuration::KeptUntilChanged,
            KeptWhere::AgentRulesFile,
            OptionEffect::Allow,
        ),
        offered(
            "unchecked",
            OptionDuration::NotVerified,
            KeptWhere::NotVerified,
            OptionEffect::Allow,
        ),
        offered(
            "unknown-allow",
            OptionDuration::ThisRequest,
            KeptWhere::NotVerified,
            OptionEffect::Allow,
        ),
        offered(
            "unknown-deny",
            OptionDuration::ThisRequest,
            KeptWhere::NotVerified,
            OptionEffect::Decline,
        ),
        offered(
            "unknown-duration-allow",
            OptionDuration::NotVerified,
            KeptWhere::Nowhere,
            OptionEffect::Allow,
        ),
        offered(
            "unknown-duration-deny",
            OptionDuration::NotVerified,
            KeptWhere::Nowhere,
            OptionEffect::Decline,
        ),
    ])
    .unwrap()
}

fn offered(
    id: &str,
    lasts: OptionDuration,
    kept: KeptWhere,
    effect: OptionEffect,
) -> OfferedOption {
    OfferedOption {
        id: option_id(id),
        label: Label::new(id).unwrap(),
        effect,
        lasts,
        kept,
    }
}

pub(super) fn option_id(id: &str) -> OfferedOptionId {
    OfferedOptionId::new(id).unwrap()
}

pub(super) fn approval_answer(id: &str) -> Answer {
    Answer::Approval {
        option_id: option_id(id),
    }
}

pub(super) fn session_answer(session: SessionRef, id: &str) -> ApprovalEvent {
    ApprovalEvent::Answer {
        actor: AdmittedDecider::Session(session),
        answer: approval_answer(id),
    }
}

pub(super) fn person_answer(factor: FactorStanding, id: &str) -> ApprovalEvent {
    ApprovalEvent::Answer {
        actor: AdmittedDecider::Person {
            person_id: person_id(),
            factor,
        },
        answer: approval_answer(id),
    }
}

pub(super) fn raise_event() -> ApprovalEvent {
    ApprovalEvent::Raise {
        raise: Box::new(approval_raise("First request")),
        destinations: destinations(),
    }
}

pub(super) fn waiting() -> ApprovalState {
    apply(&ApprovalState::Unraised, raise_event(), 0).0
}

pub(super) fn parent_pass_on() -> ApprovalEvent {
    ApprovalEvent::PassOn {
        authority: PassOnAuthority::LiveParent(parent()),
    }
}

pub(super) fn stop_event() -> ApprovalEvent {
    ApprovalEvent::Stop(StopRequest::Provider {
        request_id: request_id(),
        router_run: router_run(),
        cause: ProviderStopCause::TurnStopped,
    })
}

pub(super) fn holder_report(run: RouterRunId, report: HolderReport) -> ApprovalEvent {
    ApprovalEvent::Report(ReportDeliveryRequest {
        request_id: request_id(),
        router_run: run,
        report,
    })
}
