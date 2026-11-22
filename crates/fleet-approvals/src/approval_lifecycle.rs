use approval_request_protocol::{
    CancelReason, CentralTime, Delivery, Ending, HolderReport, HolderStopCause, Raise,
    RaiseReceipt, RaiseReply, Refusal, ReportDeliveryReply, ReportDeliveryRequest,
    StopReceiptEnding, StopReply, StopRequest, WaitingStatus,
};
use central_engine_model::{
    Context, EngineTime, EventTag, Machine, MachineKind, StateTag, Transition, TransitionError,
    TransitionResult, supported_time,
};
use std::collections::BTreeSet;
use workspace_ids::{ApprovalRequestId, RouterRunId};

use crate::{
    ApprovalDestination, ApprovalEffect, ApprovalEvent, ApprovalReceipt, ApprovalRefusal,
    ApprovalState, EndedApproval, RetainedApproval, SessionNoticePhase,
};

use crate::approval_lifecycle_admission::{
    check_destination, check_parent, destination_refusal, validate_answer,
};

/// Reduces admitted events for one scoped request into its next state and owed writes.
pub struct ApprovalLifecycle;

type LifecycleResult = TransitionResult<ApprovalLifecycle>;
type LifecycleError = TransitionError<ApprovalRefusal>;

const APPROVAL_LIFETIME_SECONDS: i64 = 60 * 60;

impl Machine for ApprovalLifecycle {
    const KIND: MachineKind = MachineKind("approval_request");
    type State = ApprovalState;
    type Event = ApprovalEvent;
    type Effect = ApprovalEffect;
    type Error = ApprovalRefusal;

    fn transition(
        state: &ApprovalState,
        event: ApprovalEvent,
        context: &mut Context<'_>,
    ) -> LifecycleResult {
        let now = context.now;
        match state {
            ApprovalState::Unraised => apply_unraised(event, now),
            ApprovalState::StoppedBeforeRaise {
                request_id,
                router_run,
                cause,
                ..
            } => apply_stopped_marker(state, request_id, router_run, *cause, event),
            ApprovalState::Waiting { saved, status } => {
                apply_waiting(state, saved, *status, event, now)
            }
            ApprovalState::Ended { saved, closed } => apply_ended(state, saved, closed, event),
        }
    }

    fn transitions() -> &'static [Transition] {
        crate::approval_lifecycle_edges::TRANSITIONS
    }

    fn next_due(state: &ApprovalState) -> Option<EngineTime> {
        match state {
            ApprovalState::Waiting { saved, .. } => Some(saved.receipt.deadline),
            ApprovalState::Unraised
            | ApprovalState::StoppedBeforeRaise { .. }
            | ApprovalState::Ended { .. } => None,
        }
    }
}

fn apply_unraised(event: ApprovalEvent, now: EngineTime) -> LifecycleResult {
    match event {
        ApprovalEvent::Raise {
            raise,
            mut destinations,
        } => {
            let deadline = now
                .timestamp()
                .checked_add(APPROVAL_LIFETIME_SECONDS)
                .and_then(|seconds| {
                    EngineTime::from_timestamp(seconds, now.timestamp_subsec_nanos())
                })
                .filter(|deadline| supported_time(*deadline))
                .ok_or(TransitionError::Domain(ApprovalRefusal::DeadlineOutOfRange))?;
            let receipt = RaiseReceipt {
                request_id: raise.request_id().clone(),
                raised_at: now,
                deadline,
            };
            let mut seen = BTreeSet::new();
            destinations.retain(|destination| seen.insert(destination.clone()));
            let mut effects = session_notices(
                &receipt.request_id,
                &destinations,
                SessionNoticePhase::Raised,
            );
            effects.push(ApprovalEffect::RegisterDue {
                request_id: receipt.request_id.clone(),
                deadline,
            });
            effects.push(ApprovalEffect::Reply(ApprovalReceipt::Raise(
                RaiseReply::RaiseReceipt {
                    receipt: receipt.clone(),
                },
            )));
            let saved = RetainedApproval {
                raise: *raise,
                receipt,
                destinations,
            };
            Ok((
                ApprovalState::Waiting {
                    saved: Box::new(saved),
                    status: WaitingStatus::Pending,
                },
                effects,
            ))
        }
        ApprovalEvent::Stop(stop) => {
            let cause = stop.cause();
            let (request_id, router_run) = stop_scope(&stop);
            let state = ApprovalState::StoppedBeforeRaise {
                request_id: request_id.clone(),
                router_run: router_run.clone(),
                cause,
                stopped_at: now,
            };
            Ok((
                state,
                vec![ApprovalEffect::Reply(stop_receipt(
                    StopReceiptEnding::Cancelled {
                        reason: CancelReason::Stopped { cause },
                    },
                ))],
            ))
        }
        event @ (ApprovalEvent::Answer { .. }
        | ApprovalEvent::PassOn { .. }
        | ApprovalEvent::ClockReached
        | ApprovalEvent::RunRetired
        | ApprovalEvent::SessionEnded
        | ApprovalEvent::EnrollmentEnded
        | ApprovalEvent::OfferAgain { .. }
        | ApprovalEvent::Report(_)) => Err(not_allowed(&ApprovalState::Unraised, &event)),
    }
}

fn apply_stopped_marker(
    state: &ApprovalState,
    request_id: &ApprovalRequestId,
    router_run: &RouterRunId,
    cause: HolderStopCause,
    event: ApprovalEvent,
) -> LifecycleResult {
    match event {
        ApprovalEvent::Raise { raise, .. } => {
            check_scope(
                request_id,
                router_run,
                raise.request_id(),
                raise_run(&raise),
            )?;
            reply_unchanged(state, ApprovalReceipt::Raise(RaiseReply::AlreadyStopped {}))
        }
        ApprovalEvent::Stop(stop) => {
            let (stop_id, stop_run) = stop_scope(&stop);
            check_scope(request_id, router_run, stop_id, stop_run)?;
            reply_unchanged(
                state,
                stop_receipt(StopReceiptEnding::Cancelled {
                    reason: CancelReason::Stopped { cause },
                }),
            )
        }
        event @ (ApprovalEvent::Answer { .. }
        | ApprovalEvent::PassOn { .. }
        | ApprovalEvent::ClockReached
        | ApprovalEvent::RunRetired
        | ApprovalEvent::SessionEnded
        | ApprovalEvent::EnrollmentEnded
        | ApprovalEvent::OfferAgain { .. }
        | ApprovalEvent::Report(_)) => Err(not_allowed(state, &event)),
    }
}

fn apply_waiting(
    state: &ApprovalState,
    saved: &RetainedApproval,
    status: WaitingStatus,
    event: ApprovalEvent,
    now: EngineTime,
) -> LifecycleResult {
    match event {
        ApprovalEvent::Raise { raise, .. } => replay_raise(state, saved, &raise),
        ApprovalEvent::Answer { actor, answer } => {
            check_destination(saved, &actor)?;
            validate_answer(saved, &actor, &answer)?;
            if now >= saved.receipt.deadline {
                return Ok(end_wait(
                    saved,
                    Ending::TimedOut {},
                    Delivery::AwaitingReport {},
                    now,
                    Some(ApprovalReceipt::AnswerRefused(Refusal::TooLate {})),
                ));
            }
            let by = actor.identity();
            let ending = Ending::Decided {
                answer,
                by: by.clone(),
            };
            let receipt = ApprovalReceipt::Answer(ending.for_destination(&by));
            Ok(end_wait(
                saved,
                ending,
                Delivery::AwaitingReport {},
                now,
                Some(receipt),
            ))
        }
        ApprovalEvent::PassOn { authority } => {
            check_parent(saved, authority)?;
            if now >= saved.receipt.deadline {
                return Ok(end_wait(
                    saved,
                    Ending::TimedOut {},
                    Delivery::AwaitingReport {},
                    now,
                    Some(ApprovalReceipt::PassOnRefused(Refusal::TooLate {})),
                ));
            }
            if status == WaitingStatus::PassedOn {
                return reply_unchanged(state, ApprovalReceipt::PassedOn);
            }
            let mut saved = Box::new(saved.clone());
            if !saved.destinations.contains(&ApprovalDestination::People) {
                saved.destinations.push(ApprovalDestination::People);
            }
            Ok((
                ApprovalState::Waiting {
                    saved,
                    status: WaitingStatus::PassedOn,
                },
                vec![ApprovalEffect::Reply(ApprovalReceipt::PassedOn)],
            ))
        }
        ApprovalEvent::Stop(stop) => {
            check_stop(saved, &stop)?;
            let ending = if now >= saved.receipt.deadline {
                Ending::TimedOut {}
            } else {
                Ending::Cancelled {
                    reason: CancelReason::Stopped {
                        cause: stop.cause(),
                    },
                }
            };
            let receipt = stop_receipt(which_ending_stands(&ending));
            Ok(end_wait(
                saved,
                ending,
                gone_before_handover(),
                now,
                Some(receipt),
            ))
        }
        ApprovalEvent::ClockReached => {
            if now < saved.receipt.deadline {
                return Err(TransitionError::Stale);
            }
            Ok(end_wait(
                saved,
                Ending::TimedOut {},
                Delivery::AwaitingReport {},
                now,
                None,
            ))
        }
        ApprovalEvent::RunRetired => Ok(end_for_authority_loss(
            saved,
            CancelReason::RequesterGone {},
            now,
        )),
        ApprovalEvent::EnrollmentEnded => Ok(end_for_authority_loss(
            saved,
            CancelReason::EnrollmentEnded {},
            now,
        )),
        ApprovalEvent::SessionEnded => {
            let ending = if now >= saved.receipt.deadline {
                Ending::TimedOut {}
            } else {
                Ending::Cancelled {
                    reason: CancelReason::SessionEnded {},
                }
            };
            Ok(end_wait(
                saved,
                ending,
                Delivery::AwaitingReport {},
                now,
                None,
            ))
        }
        event @ (ApprovalEvent::OfferAgain { .. } | ApprovalEvent::Report(_)) => {
            Err(not_allowed(state, &event))
        }
    }
}

fn apply_ended(
    state: &ApprovalState,
    saved: &RetainedApproval,
    closed: &EndedApproval,
    event: ApprovalEvent,
) -> LifecycleResult {
    match event {
        ApprovalEvent::Raise { raise, .. } => replay_raise(state, saved, &raise),
        ApprovalEvent::Answer { actor, .. } => {
            check_destination(saved, &actor)?;
            Err(destination_refusal(Refusal::Ended {
                ending: closed.ending.for_destination(&actor.identity()),
            }))
        }
        ApprovalEvent::PassOn { authority } => {
            let parent = check_parent(saved, authority)?;
            Err(destination_refusal(Refusal::Ended {
                ending: closed.ending.for_destination(
                    &approval_request_protocol::DecidedBy::Session { session: parent },
                ),
            }))
        }
        ApprovalEvent::Stop(stop) => {
            check_stop(saved, &stop)?;
            if matches!(
                closed.delivery,
                Delivery::ReceiptUnknown {} | Delivery::NoHandoverOwed {}
            ) {
                return Err(TransitionError::Stale);
            }
            let receipt = stop_receipt(which_ending_stands(&closed.ending));
            let delivery = if matches!(closed.delivery, Delivery::Reported { .. }) {
                closed.delivery
            } else {
                gone_before_handover()
            };
            Ok((
                with_delivery(saved, closed, delivery),
                vec![ApprovalEffect::Reply(receipt)],
            ))
        }
        ApprovalEvent::OfferAgain { router_run } => {
            if &router_run != saved.holder_run() {
                return Err(TransitionError::Stale);
            }
            if closed.delivery != (Delivery::AwaitingReport {}) {
                return Err(TransitionError::Domain(ApprovalRefusal::NoHandoverOwed));
            }
            Ok((
                state.clone(),
                vec![offer_effect(saved, closed.ending.clone())],
            ))
        }
        ApprovalEvent::Report(report) => apply_report(saved, closed, &report),
        ApprovalEvent::RunRetired | ApprovalEvent::EnrollmentEnded => {
            if closed.delivery == (Delivery::AwaitingReport {}) {
                return Ok((
                    with_delivery(saved, closed, Delivery::ReceiptUnknown {}),
                    vec![],
                ));
            }
            Ok((state.clone(), vec![]))
        }
        ApprovalEvent::SessionEnded | ApprovalEvent::ClockReached => Ok((state.clone(), vec![])),
    }
}

fn end_for_authority_loss(
    saved: &RetainedApproval,
    reason: CancelReason,
    now: EngineTime,
) -> (ApprovalState, Vec<ApprovalEffect>) {
    if now >= saved.receipt.deadline {
        end_wait(
            saved,
            Ending::TimedOut {},
            Delivery::ReceiptUnknown {},
            now,
            None,
        )
    } else {
        end_wait(
            saved,
            Ending::Cancelled { reason },
            Delivery::NoHandoverOwed {},
            now,
            None,
        )
    }
}

fn end_wait(
    saved: &RetainedApproval,
    ending: Ending,
    delivery: Delivery,
    at: CentralTime,
    receipt: Option<ApprovalReceipt>,
) -> (ApprovalState, Vec<ApprovalEffect>) {
    let request_id = &saved.receipt.request_id;
    let mut effects = session_notices(request_id, &saved.destinations, SessionNoticePhase::Ended);
    effects.push(ApprovalEffect::ClearDue {
        request_id: request_id.clone(),
    });
    if delivery == (Delivery::AwaitingReport {}) {
        effects.push(offer_effect(saved, ending.clone()));
    }
    if let Some(receipt) = receipt {
        effects.push(ApprovalEffect::Reply(receipt));
    }
    let state = ApprovalState::Ended {
        saved: Box::new(saved.clone()),
        closed: EndedApproval {
            ending,
            ended_at: at,
            delivery,
        },
    };
    (state, effects)
}

fn apply_report(
    saved: &RetainedApproval,
    closed: &EndedApproval,
    report: &ReportDeliveryRequest,
) -> LifecycleResult {
    check_scope(
        &saved.receipt.request_id,
        saved.holder_run(),
        &report.request_id,
        &report.router_run,
    )?;
    let stored = match closed.delivery {
        Delivery::AwaitingReport {} => report.report,
        Delivery::Reported { report: prior } if prior == report.report => prior,
        Delivery::Reported { .. } => {
            return Err(TransitionError::Domain(ApprovalRefusal::ReportDiffers));
        }
        Delivery::NoHandoverOwed {} | Delivery::ReceiptUnknown {} => {
            return Err(TransitionError::Domain(ApprovalRefusal::NoHandoverOwed));
        }
    };
    Ok((
        with_delivery(saved, closed, Delivery::Reported { report: stored }),
        vec![ApprovalEffect::Reply(ApprovalReceipt::Report(
            ReportDeliveryReply::DeliveryRecorded {},
        ))],
    ))
}

fn check_scope(
    id: &ApprovalRequestId,
    run: &RouterRunId,
    event_id: &ApprovalRequestId,
    event_run: &RouterRunId,
) -> Result<(), LifecycleError> {
    if id == event_id && run == event_run {
        Ok(())
    } else {
        Err(TransitionError::Stale)
    }
}

fn check_stop(saved: &RetainedApproval, stop: &StopRequest) -> Result<(), LifecycleError> {
    let (id, run) = stop_scope(stop);
    check_scope(&saved.receipt.request_id, saved.holder_run(), id, run)?;
    match (&saved.raise, stop) {
        (
            Raise::ProviderApproval { .. } | Raise::ProviderQuestion { .. },
            StopRequest::Provider { .. },
        )
        | (Raise::UnprovenCall { .. }, StopRequest::UnprovenCall { .. }) => Ok(()),
        (
            Raise::ProviderApproval { .. } | Raise::ProviderQuestion { .. },
            StopRequest::UnprovenCall { .. },
        )
        | (Raise::UnprovenCall { .. }, StopRequest::Provider { .. }) => Err(TransitionError::Stale),
    }
}

fn stop_scope(stop: &StopRequest) -> (&ApprovalRequestId, &RouterRunId) {
    match stop {
        StopRequest::Provider {
            request_id,
            router_run,
            ..
        }
        | StopRequest::UnprovenCall {
            request_id,
            router_run,
            ..
        } => (request_id, router_run),
    }
}

fn raise_run(raise: &Raise) -> &RouterRunId {
    match raise {
        Raise::ProviderApproval { holder, .. } | Raise::ProviderQuestion { holder, .. } => {
            &holder.router_run
        }
        Raise::UnprovenCall { holder, .. } => &holder.router_run,
    }
}

fn replay_raise(state: &ApprovalState, saved: &RetainedApproval, raise: &Raise) -> LifecycleResult {
    check_scope(
        &saved.receipt.request_id,
        saved.holder_run(),
        raise.request_id(),
        raise_run(raise),
    )?;
    if &saved.raise != raise {
        return Err(TransitionError::Domain(ApprovalRefusal::ContentsDiffer));
    }
    reply_unchanged(
        state,
        ApprovalReceipt::Raise(RaiseReply::RaiseReceipt {
            receipt: saved.receipt.clone(),
        }),
    )
}

fn session_notices(
    request_id: &ApprovalRequestId,
    destinations: &[ApprovalDestination],
    phase: SessionNoticePhase,
) -> Vec<ApprovalEffect> {
    destinations
        .iter()
        .filter_map(|destination| match destination {
            ApprovalDestination::People => None,
            ApprovalDestination::Session(session) => Some(ApprovalEffect::StoreSessionNotice {
                request_id: request_id.clone(),
                session: session.clone(),
                phase,
            }),
        })
        .collect()
}

fn offer_effect(saved: &RetainedApproval, ending: Ending) -> ApprovalEffect {
    ApprovalEffect::OfferEnding(Box::new(saved.offer_ending(ending)))
}

fn with_delivery(
    saved: &RetainedApproval,
    closed: &EndedApproval,
    delivery: Delivery,
) -> ApprovalState {
    ApprovalState::Ended {
        saved: Box::new(saved.clone()),
        closed: EndedApproval {
            delivery,
            ..closed.clone()
        },
    }
}

fn gone_before_handover() -> Delivery {
    Delivery::Reported {
        report: HolderReport::GoneBeforeHandover,
    }
}

fn which_ending_stands(ending: &Ending) -> StopReceiptEnding {
    match ending {
        Ending::Decided { .. } => StopReceiptEnding::DecidedEarlier {},
        Ending::Cancelled { reason } => StopReceiptEnding::Cancelled { reason: *reason },
        Ending::TimedOut {} => StopReceiptEnding::TimedOut {},
    }
}

fn stop_receipt(ending: StopReceiptEnding) -> ApprovalReceipt {
    ApprovalReceipt::Stop(StopReply::StopReceipt { ending })
}

fn reply_unchanged(state: &ApprovalState, receipt: ApprovalReceipt) -> LifecycleResult {
    Ok((state.clone(), vec![ApprovalEffect::Reply(receipt)]))
}

fn not_allowed(state: &ApprovalState, event: &ApprovalEvent) -> LifecycleError {
    TransitionError::NotAllowed {
        state: state.state_tag(),
        event: event.event_tag(),
    }
}

#[cfg(test)]
#[path = "approval_lifecycle_test.rs"]
mod tests;
