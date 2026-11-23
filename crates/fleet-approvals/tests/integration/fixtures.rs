use approval_request_protocol::{
    Answer, ApprovalOffer, CentralTime, DeliverEndingRequest, Label, OfferedOption,
    OfferedOptionId, OfferedOptions, OptionEffect, OptionLifetime, ProviderHolder,
    ProviderStopCause, Raise, RegisterRunReply, RetainedOptionStorage, StopRequest, Title,
};
use central_engine_model::{Context, IdSource};
use fleet_identity_protocol::EnrollmentId;
use workspace_ids::{ApprovalRequestId, MachineId, PersonId, RequestId, RouterRunId};

use fleet_approvals::{
    AdmittedDecider, ApprovalCollection, ApprovalCollectionEffect, ApprovalCollectionEvent,
    ApprovalDecideInput, ApprovalEffect, ApprovalEnrollment, ApprovalReceipt, ApprovalRequestKey,
    ApprovalRouting, ApprovalRun, ApprovalSession, ApprovalState, EndedApproval, FactorStanding,
    ProviderSessionAvailability,
};

pub(super) const HOUR: i64 = 3_600_000;

pub(super) struct UnusedIds;
impl IdSource for UnusedIds {
    fn next_id(&mut self) -> uuid::Uuid {
        panic!("the lifecycle consumes supplied IDs")
    }
}

fn id(suffix: u16) -> String {
    format!("019ac19f-aa00-7000-8000-00000000{suffix:04x}")
}

pub(super) fn time(at: i64) -> CentralTime {
    CentralTime::from_timestamp_millis(at).unwrap()
}

pub(super) fn request_id(number: u16) -> ApprovalRequestId {
    id(number).parse().unwrap()
}
pub(super) fn router_run(number: u16) -> RouterRunId {
    id(number).parse().unwrap()
}
pub(super) fn operation_id(number: u16) -> RequestId {
    id(number).parse().unwrap()
}
pub(super) fn person_id(number: u16) -> PersonId {
    id(number).parse().unwrap()
}

pub(super) fn enrollment(machine: u16, number: u16) -> ApprovalEnrollment {
    ApprovalEnrollment {
        machine_id: MachineId::try_from(id(machine)).unwrap(),
        enrollment_id: EnrollmentId::try_from(id(number)).unwrap(),
    }
}

pub(super) fn session(enrollment: &ApprovalEnrollment, number: u16) -> ApprovalSession {
    ApprovalSession {
        enrollment: enrollment.clone(),
        session_id: id(number).parse().unwrap(),
    }
}

pub(super) fn key(run: &ApprovalRun, number: u16) -> ApprovalRequestKey {
    ApprovalRequestKey {
        run: run.clone(),
        request_id: request_id(number),
    }
}

pub(super) fn option(number: u8) -> OfferedOptionId {
    OfferedOptionId::new(number).unwrap()
}
pub(super) fn answer(number: u8) -> Answer {
    Answer::Approval {
        option_id: option(number),
    }
}
pub(super) fn person(number: u16, factor: FactorStanding) -> AdmittedDecider {
    AdmittedDecider::Person {
        person_id: person_id(number),
        factor,
    }
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
    let mut book = ApprovalCollection::new(time(0));
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
        router_run: router_run(9000),
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

pub(super) fn decide_event(
    key: &ApprovalRequestKey,
    actor: AdmittedDecider,
    operation: u16,
    value: Answer,
) -> ApprovalCollectionEvent {
    ApprovalCollectionEvent::Decide {
        key: key.clone(),
        actor,
        request: ApprovalDecideInput {
            request_id: key.request_id.clone(),
            operation_id: operation_id(operation),
            value,
        },
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum ObservedReply {
    Raise(approval_request_protocol::RaiseReply),
    Stop(approval_request_protocol::StopReply),
    Decide(fleet_approvals::ApprovalDecisionReply),
    Read(Option<approval_request_protocol::RequestView>),
    PassOn(approval_request_protocol::PassOnReply),
}

// Directed cases use this view for payload assertions. The recipient-security and
// ledger tests pass their expected request and recipient to the public gate explicitly.
pub(super) fn reply(effects: &[ApprovalCollectionEffect]) -> ObservedReply {
    match raw_reply(effects) {
        ApprovalReceipt::Raise(payload) => ObservedReply::Raise(payload.clone()),
        ApprovalReceipt::Stop(payload) => ObservedReply::Stop(payload.clone()),
        ApprovalReceipt::Decide(bound) => ObservedReply::Decide(
            bound
                .for_recipient(bound.request(), bound.recipient())
                .unwrap()
                .clone(),
        ),
        ApprovalReceipt::Read(bound) => ObservedReply::Read(
            bound
                .for_recipient(bound.request(), bound.recipient())
                .unwrap()
                .clone(),
        ),
        ApprovalReceipt::PassOn(bound) => ObservedReply::PassOn(
            bound
                .for_recipient(bound.request(), bound.recipient())
                .unwrap()
                .clone(),
        ),
    }
}

pub(super) fn offered_frame(effects: &[ApprovalCollectionEffect]) -> DeliverEndingRequest {
    effects
        .iter()
        .find_map(|effect| match effect {
            ApprovalCollectionEffect::Request(ApprovalEffect::OfferEnding { request, .. }) => {
                Some((**request).clone())
            }
            ApprovalCollectionEffect::Request(_) | ApprovalCollectionEffect::Registered(_) => None,
        })
        .expect("expected a holder offer")
}

pub(super) fn provider_stop(raise: &Raise) -> StopRequest {
    StopRequest::Provider {
        request_id: raise.request_id().clone(),
        router_run: raise.router_run().clone(),
        contents_digest: raise.contents_digest().unwrap(),
        cause: ProviderStopCause::AskReplaced,
    }
}

pub(super) fn view_event(key: &ApprovalRequestKey, person: u16) -> ApprovalCollectionEvent {
    ApprovalCollectionEvent::Read {
        key: key.clone(),
        person_id: person_id(person),
    }
}
