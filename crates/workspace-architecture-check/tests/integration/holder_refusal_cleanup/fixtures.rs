use approval_request_protocol::{
    ApprovalOffer, CentralTime, Label, OfferedOption, OfferedOptionId, OfferedOptions,
    OptionEffect, OptionLifetime, ProviderHolder, ProviderStopCause, Raise, RaiseReply,
    RegisterRunReply, RetainedOptionStorage, StopReply, StopRequest, Title,
};
use central_engine_model::{Context, IdSource};
use fleet_approvals::{
    ApprovalCollection, ApprovalCollectionEffect, ApprovalCollectionEvent, ApprovalEffect,
    ApprovalEnrollment, ApprovalReceipt, ApprovalRequestKey, ApprovalRouting, ApprovalRun,
    ApprovalSession, ApprovalState, EndedApproval, ProviderSessionAvailability,
};

struct UnusedIds;

impl IdSource for UnusedIds {
    fn next_id(&mut self) -> uuid::Uuid {
        panic!("the lifecycle consumes supplied IDs")
    }
}

fn id(suffix: u16) -> String {
    format!("019ac19f-aa00-7000-8000-00000000{suffix:04x}")
}

fn time(at: i64) -> CentralTime {
    CentralTime::from_timestamp_millis(at).unwrap()
}

fn enrollment(machine: u16, number: u16) -> ApprovalEnrollment {
    ApprovalEnrollment {
        machine_id: id(machine).try_into().unwrap(),
        enrollment_id: id(number).try_into().unwrap(),
    }
}

fn session(enrollment: &ApprovalEnrollment, number: u16) -> ApprovalSession {
    ApprovalSession {
        enrollment: enrollment.clone(),
        session_id: id(number).parse().unwrap(),
    }
}

pub(super) fn key(run: &ApprovalRun, number: u16) -> ApprovalRequestKey {
    ApprovalRequestKey {
        run: run.clone(),
        request_id: id(number).parse().unwrap(),
    }
}

fn option(number: u8) -> OfferedOptionId {
    OfferedOptionId::new(number).unwrap()
}

pub(super) fn provider(
    key: &ApprovalRequestKey,
    requester: &ApprovalSession,
    title: &str,
) -> Raise {
    Raise::ProviderApproval {
        request_id: key.request_id.clone(),
        holder: ProviderHolder {
            session_id: requester.session_id.clone(),
            router_run: key.run.router_run.clone(),
        },
        offer: ApprovalOffer {
            title: Title::new(title).unwrap(),
            description: None,
            subject: None,
            options: options(),
        },
    }
}

fn options() -> OfferedOptions {
    OfferedOptions::new(vec![
        offered(1, OptionEffect::Allow, OptionLifetime::ThisRequest {}),
        offered(2, OptionEffect::Decline, OptionLifetime::ThisRequest {}),
        offered(3, OptionEffect::Allow, OptionLifetime::NotVerified {}),
        offered(4, OptionEffect::Decline, OptionLifetime::NotVerified {}),
        offered(
            5,
            OptionEffect::Allow,
            OptionLifetime::KeptUntilChanged {
                kept: RetainedOptionStorage::AgentRulesFile,
            },
        ),
    ])
    .unwrap()
}

fn offered(number: u8, effect: OptionEffect, lasts: OptionLifetime) -> OfferedOption {
    OfferedOption {
        id: option(number),
        label: Label::new(format!("Option {number}")).unwrap(),
        effect,
        lasts,
    }
}

pub(super) fn routing(parent: Option<&ApprovalSession>) -> ApprovalRouting {
    match parent {
        Some(admitted_parent) => ApprovalRouting::Delegate {
            admitted_parent: admitted_parent.clone(),
        },
        None => ApprovalRouting::Owner,
    }
}

pub(super) fn run_event(
    book: &mut ApprovalCollection,
    event: ApprovalCollectionEvent,
    at: i64,
) -> Vec<ApprovalCollectionEffect> {
    let (next, effects) = book
        .apply(
            event,
            &mut Context {
                now: time(at),
                ids: &mut UnusedIds,
            },
        )
        .unwrap();
    *book = next;
    effects
}

pub(super) fn register(
    book: &mut ApprovalCollection,
    run: &ApprovalRun,
    at: i64,
) -> Vec<ApprovalCollectionEffect> {
    run_event(
        book,
        ApprovalCollectionEvent::RegisterRun { run: run.clone() },
        at,
    )
}

pub(super) fn setup() -> (
    ApprovalCollection,
    ApprovalRun,
    ApprovalSession,
    ApprovalSession,
) {
    let mut book = ApprovalCollection::default();
    let enrollment = enrollment(8000, 8001);
    run_event(
        &mut book,
        ApprovalCollectionEvent::ActivateEnrollment {
            enrollment: enrollment.clone(),
        },
        0,
    );
    let run = ApprovalRun {
        enrollment: enrollment.clone(),
        router_run: id(9000).parse().unwrap(),
    };
    assert_eq!(
        register(&mut book, &run, 0),
        [ApprovalCollectionEffect::Registered(
            RegisterRunReply::Registered {}
        )]
    );
    (
        book,
        run,
        session(&enrollment, 8002),
        session(&enrollment, 8003),
    )
}

pub(super) fn raise_event(
    run: &ApprovalRun,
    raise: Raise,
    routing: ApprovalRouting,
) -> ApprovalCollectionEvent {
    let provider_session = if raise.requesting_session().is_some() {
        ProviderSessionAvailability::Live
    } else {
        ProviderSessionAvailability::SessionFree
    };
    ApprovalCollectionEvent::Raise {
        run: run.clone(),
        raise: Box::new(raise),
        routing,
        provider_session,
    }
}

pub(super) fn closed<'a>(
    book: &'a ApprovalCollection,
    key: &ApprovalRequestKey,
) -> &'a EndedApproval {
    let Some(ApprovalState::Ended { closed, .. }) = book.requests.get(key) else {
        panic!("expected an ending for {key:?}");
    };
    closed
}

pub(super) fn raw_reply(effects: &[ApprovalCollectionEffect]) -> &ApprovalReceipt {
    let Some(ApprovalCollectionEffect::Request(ApprovalEffect::Reply(receipt))) = effects.last()
    else {
        panic!("expected a final operation reply, got {effects:?}");
    };
    receipt
}

#[derive(Debug, Eq, PartialEq)]
pub(super) enum ObservedReply {
    Raise(RaiseReply),
    Stop(StopReply),
}

pub(super) fn reply(effects: &[ApprovalCollectionEffect]) -> ObservedReply {
    match raw_reply(effects) {
        ApprovalReceipt::Raise(payload) => ObservedReply::Raise(payload.clone()),
        ApprovalReceipt::Stop(payload) => ObservedReply::Stop(payload.clone()),
        ApprovalReceipt::Decide(_) | ApprovalReceipt::Read(_) | ApprovalReceipt::PassOn(_) => {
            panic!("expected a Raise or Stop reply, got {effects:?}")
        }
    }
}

pub(super) fn provider_stop(raise: &Raise) -> StopRequest {
    StopRequest::Provider {
        request_id: raise.request_id().clone(),
        router_run: raise.router_run().clone(),
        contents_digest: raise.contents_digest().unwrap(),
        cause: ProviderStopCause::AskReplaced,
    }
}
