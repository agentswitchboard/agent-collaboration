use approval_request_protocol::{
    Answer, ApprovalOffer, CentralTime, DeliverEndingReply, DeliverEndingRequest, Delivery, Label,
    OfferedOption, OfferedOptionId, OfferedOptions, OptionEffect, OptionLifetime, ProviderHolder,
    ProviderStopCause, Raise, RaiseReply, StopReply, StopRequest, Title,
};
use central_engine_model::{
    Context, EngineTime, EventTag, IdSource, Machine, MachineKind, Transition, TransitionError,
    check_transitions,
};
use fleet_identity_protocol::EnrollmentId;

use crate::{
    AdmittedDecider, ApprovalDecideInput, ApprovalEffect, ApprovalEnrollment, ApprovalEvent,
    ApprovalLifecycle, ApprovalReceipt, ApprovalRefusal, ApprovalRequestKey, ApprovalRouting,
    ApprovalRun, ApprovalSession, ApprovalState, FactorStanding, MAX_OUTSTANDING_PER_RUN,
    ProviderSessionAvailability, RaiseAdmission,
};

const HOUR: i64 = 3_600_000;

struct NoIds;
impl IdSource for NoIds {
    fn next_id(&mut self) -> uuid::Uuid {
        panic!("approval rows allocate no IDs")
    }
}

fn id(number: u16) -> String {
    format!("019ac19f-aa00-7000-8000-00000000{number:04x}")
}
fn time(at: i64) -> CentralTime {
    CentralTime::from_timestamp_millis(at).unwrap()
}
#[derive(Clone)]
struct SampledApprovalEvent {
    event: ApprovalEvent,
    at: i64,
}

impl EventTag for SampledApprovalEvent {
    fn event_tag(&self) -> &'static str {
        self.event.event_tag()
    }
}

struct ApprovalTransitionWitnesses;

impl Machine for ApprovalTransitionWitnesses {
    const KIND: MachineKind = ApprovalLifecycle::KIND;
    type State = ApprovalState;
    type Event = SampledApprovalEvent;
    type Effect = ApprovalEffect;
    type Error = ApprovalRefusal;

    fn transition(
        state: &Self::State,
        witness: Self::Event,
        context: &mut Context<'_>,
    ) -> central_engine_model::TransitionResult<Self> {
        context.now = time(witness.at);
        ApprovalLifecycle::transition(state, witness.event, context)
    }

    fn transitions() -> &'static [Transition] {
        ApprovalLifecycle::transitions()
    }

    fn next_due(state: &Self::State) -> Option<EngineTime> {
        ApprovalLifecycle::next_due(state)
    }
}

fn apply(
    state: &ApprovalState,
    event: ApprovalEvent,
    at: i64,
) -> central_engine_model::TransitionResult<ApprovalLifecycle> {
    ApprovalLifecycle::transition(
        state,
        event,
        &mut Context {
            now: time(at),
            ids: &mut NoIds,
        },
    )
}

fn stale(state: &ApprovalState, event: &ApprovalEvent, at: CentralTime) -> bool {
    match event {
        ApprovalEvent::Alarm { deadline } => !matches!(
            state, ApprovalState::Waiting { saved } if saved.window.deadline() == *deadline && at >= *deadline
        ),
        ApprovalEvent::Reoffer => !matches!(
            state, ApprovalState::Ended { closed, .. } if closed.delivery == (Delivery::Owed {})
        ),
        ApprovalEvent::DeliveryReply { key, request, .. } => !matches!(
            state, ApprovalState::Ended { saved, closed, .. }
            if &saved.key == key && request.request_id() == &key.request_id
                && request.contents_digest() == &saved.contents_digest && request.as_ref() == &closed.ending
        ),
        ApprovalEvent::Raise { .. }
        | ApprovalEvent::Stop { .. }
        | ApprovalEvent::Decide { .. }
        | ApprovalEvent::PassOn { .. }
        | ApprovalEvent::Read { .. }
        | ApprovalEvent::RunRetired
        | ApprovalEvent::SessionEnded
        | ApprovalEvent::EnrollmentEnded => false,
    }
}

#[test]
fn row_matrix_witnesses_every_declared_edge_and_asserts_typed_refusals() {
    let enrollment = ApprovalEnrollment {
        machine_id: id(1).parse().unwrap(),
        enrollment_id: EnrollmentId::try_from(id(2)).unwrap(),
    };
    let run = ApprovalRun {
        enrollment: enrollment.clone(),
        router_run: id(3).parse().unwrap(),
    };
    let key = ApprovalRequestKey {
        run: run.clone(),
        request_id: id(4).parse().unwrap(),
    };
    let foreign_key = ApprovalRequestKey {
        run,
        request_id: id(5).parse().unwrap(),
    };
    let parent = ApprovalSession {
        enrollment,
        session_id: id(6).parse().unwrap(),
    };
    let person_id: workspace_ids::PersonId = id(7).parse().unwrap();
    let answer = Answer::Approval {
        option_id: OfferedOptionId::new(1).unwrap(),
    };
    let raise = Raise::ProviderApproval {
        request_id: key.request_id.clone(),
        holder: ProviderHolder {
            session_id: id(8).parse().unwrap(),
            router_run: key.run.router_run.clone(),
        },
        offer: ApprovalOffer {
            title: Title::new("Matrix").unwrap(),
            description: None,
            subject: None,
            options: OfferedOptions::new(vec![OfferedOption {
                id: OfferedOptionId::new(1).unwrap(),
                label: Label::new("Allow").unwrap(),
                effect: OptionEffect::Allow,
                lasts: OptionLifetime::ThisRequest {},
            }])
            .unwrap(),
        },
    };
    let start = |routing, provider_session, outstanding| ApprovalEvent::Raise {
        key: key.clone(),
        raise: Box::new(raise.clone()),
        routing,
        admission: RaiseAdmission {
            provider_session,
            outstanding,
        },
    };
    let live = ProviderSessionAvailability::Live;
    let delegated = start(
        ApprovalRouting::Delegate {
            admitted_parent: parent.clone(),
        },
        live,
        0,
    );
    let owner = start(ApprovalRouting::Owner, live, 0);
    let stopped = |bound: ApprovalRequestKey, digest| ApprovalEvent::Stop {
        key: bound,
        request: StopRequest::Provider {
            request_id: key.request_id.clone(),
            router_run: key.run.router_run.clone(),
            contents_digest: digest,
            cause: ProviderStopCause::AskReplaced,
        },
    };
    let digest = raise.contents_digest().unwrap();
    let stop = stopped(key.clone(), digest.clone());
    let erroneous_stop = stopped(
        key.clone(),
        approval_request_protocol::RequestContentsDigest::try_from(
            approval_request_protocol::sha256_url(b"wrong immutable content"),
        )
        .unwrap(),
    );
    let unelected_stop = stopped(foreign_key, digest);
    let unraised = ApprovalState::Unraised;
    let marker = apply(&unraised, stop.clone(), 0).unwrap().0;
    let owner_waiting = apply(&unraised, owner.clone(), 0).unwrap().0;
    let delegated_waiting = apply(&unraised, delegated.clone(), 0).unwrap().0;
    let pass = ApprovalEvent::PassOn {
        key: key.clone(),
        live_parent: parent.clone(),
    };
    let passed = apply(&delegated_waiting, pass.clone(), 1).unwrap().0;
    let decide = |actor| ApprovalEvent::Decide {
        key: key.clone(),
        actor,
        request: ApprovalDecideInput {
            request_id: key.request_id.clone(),
            operation_id: id(9).parse().unwrap(),
            value: answer.clone(),
        },
    };
    let parent_decide = decide(AdmittedDecider::Session(parent));
    let person_decide = decide(AdmittedDecider::Person {
        person_id: person_id.clone(),
        factor: FactorStanding::Fresh,
    });
    let mismatched_decide = ApprovalEvent::Decide {
        key: key.clone(),
        actor: AdmittedDecider::Person {
            person_id: person_id.clone(),
            factor: FactorStanding::Fresh,
        },
        request: ApprovalDecideInput {
            request_id: id(88).parse().unwrap(),
            operation_id: id(9).parse().unwrap(),
            value: answer,
        },
    };
    let (owed, effects) = apply(&owner_waiting, person_decide.clone(), 1).unwrap();
    let frame = effects
        .into_iter()
        .find_map(|effect| match effect {
            ApprovalEffect::OfferEnding { request, .. } => Some(request),
            _ => None,
        })
        .unwrap();
    let delivery = |request: Box<DeliverEndingRequest>, reply| ApprovalEvent::DeliveryReply {
        key: key.clone(),
        request,
        reply,
    };
    let received = delivery(frame.clone(), DeliverEndingReply::Received {});
    let not_held = delivery(frame.clone(), DeliverEndingReply::NotHeld {});
    let received_row = apply(&owed, received.clone(), 1).unwrap().0;
    let closed_row = apply(&owed, not_held.clone(), 1).unwrap().0;
    let bad_frame =
        DeliverEndingRequest::from_raise(&raise, approval_request_protocol::Ending::TimedOut {})
            .unwrap();
    let unavailable = start(
        ApprovalRouting::Owner,
        ProviderSessionAvailability::NotAvailable,
        0,
    );
    let at_capacity = start(ApprovalRouting::Owner, live, MAX_OUTSTANDING_PER_RUN);
    for (event, expected) in [
        (unavailable.clone(), RaiseReply::SessionEnded {}),
        (at_capacity.clone(), RaiseReply::TooManyOutstanding {}),
    ] {
        assert_eq!(
            apply(&unraised, event, 1),
            Ok((
                unraised.clone(),
                vec![ApprovalEffect::Reply(ApprovalReceipt::Raise(expected))],
            ))
        );
    }
    assert_eq!(
        apply(&delegated_waiting, erroneous_stop.clone(), 1),
        Ok((
            delegated_waiting.clone(),
            vec![ApprovalEffect::Reply(ApprovalReceipt::Stop(
                StopReply::NotVisible {}
            ))],
        ))
    );
    let self_route = start(
        ApprovalRouting::Delegate {
            admitted_parent: ApprovalSession {
                enrollment: key.run.enrollment.clone(),
                session_id: id(8).parse().unwrap(),
            },
        },
        live,
        0,
    );
    assert_eq!(
        apply(&unraised, self_route, 1),
        Err(TransitionError::Domain(
            ApprovalRefusal::InvalidInitialRoute
        ))
    );

    let states = [
        unraised,
        marker,
        owner_waiting,
        delegated_waiting,
        passed,
        owed,
        received_row,
        closed_row,
    ];
    let events = [
        owner,
        delegated,
        unavailable,
        at_capacity,
        stop,
        erroneous_stop,
        unelected_stop,
        pass,
        parent_decide,
        person_decide,
        mismatched_decide,
        ApprovalEvent::Read {
            key: key.clone(),
            person_id,
        },
        ApprovalEvent::RunRetired,
        ApprovalEvent::EnrollmentEnded,
        ApprovalEvent::SessionEnded,
        ApprovalEvent::Alarm {
            deadline: time(HOUR),
        },
        ApprovalEvent::Alarm {
            deadline: time(HOUR + 1),
        },
        ApprovalEvent::Reoffer,
        received,
        not_held,
        delivery(frame, DeliverEndingReply::HolderNotReady {}),
        delivery(Box::new(bad_frame), DeliverEndingReply::Received {}),
    ];
    let mut witnesses = Vec::new();
    for state in states {
        for event in &events {
            for at in [2, HOUR] {
                let result = apply(&state, event.clone(), at);
                if stale(&state, event, time(at)) {
                    assert_eq!(
                        result,
                        Err(TransitionError::Stale),
                        "{state:?} + {event:?} at {at}"
                    );
                } else {
                    assert!(
                        result.is_ok(),
                        "required successor for {state:?} + {event:?} at {at}: {result:?}"
                    );
                }
                witnesses.push((
                    state.clone(),
                    SampledApprovalEvent {
                        event: event.clone(),
                        at,
                    },
                ));
            }
        }
    }
    assert_eq!(
        check_transitions::<ApprovalTransitionWitnesses>(
            witnesses,
            &mut Context {
                now: time(0),
                ids: &mut NoIds
            }
        ),
        Ok(())
    );
}
