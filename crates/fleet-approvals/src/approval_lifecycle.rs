use approval_request_protocol::RequestContentsDigest;
use approval_request_protocol::{
    ApprovalWindow, CancelReason, CentralTime, Delivery, Ending, Raise, RaiseReceipt, RaiseReply,
    StopReceiptEnding, StopReply, StopRequest, WaitingStatus,
};
use central_engine_model::{
    Context, EngineTime, Machine, MachineKind, Transition, TransitionError, supported_time,
};

use crate::approval_lifecycle_admission::{
    applying_time, matches_key, stop_binding, stop_cause_matches_raise, stop_marker_matches_cause,
};
use crate::approval_lifecycle_delivery::{receive, reoffer};
use crate::approval_lifecycle_destinations::{decide, pass_on, read};
use crate::approval_lifecycle_effects::{
    ApprovalChange, finish, parent_notice, reply, standing, with_delivery,
};
use crate::{
    ApprovalActor, ApprovalApply, ApprovalEffect, ApprovalEvent, ApprovalReceipt, ApprovalRefusal,
    ApprovalRequestKey, ApprovalRouting, ApprovalState, ProviderSessionAvailability,
    RaiseAdmission, RetainedApproval, StopMarkerCause,
};

pub const MAX_OUTSTANDING_PER_RUN: usize = 256;

/// Reduces one request within the applying authority's guarded scope.
pub struct ApprovalLifecycle;

impl Machine for ApprovalLifecycle {
    const KIND: MachineKind = MachineKind("approval_request");
    type State = ApprovalState;
    type Event = ApprovalApply;
    type Effect = ApprovalEffect;
    type Error = ApprovalRefusal;

    fn transition(
        state: &ApprovalState,
        apply: ApprovalApply,
        context: &mut Context<'_>,
    ) -> ApprovalChange {
        let now = applying_time(context.now, apply.high_water)?;
        match apply.event {
            ApprovalEvent::Raise {
                key,
                raise,
                routing,
                admission,
            } => apply_raise(state, key, *raise, routing, admission, now),
            ApprovalEvent::Decide {
                key,
                actor,
                request,
                prior,
            } => decide(state, &key, &actor, request, prior.as_deref(), now),
            ApprovalEvent::Read { key, person_id } => {
                read(state, &key, &ApprovalActor::Person(person_id), now)
            }
            ApprovalEvent::PassOn { key, live_parent } => pass_on(state, &key, &live_parent, now),
            ApprovalEvent::Stop { key, request } => apply_stop(state, key, &request, now),
            ApprovalEvent::DeliveryReply {
                key,
                request,
                reply,
            } => receive(state, &key, &request, reply),
            ApprovalEvent::Alarm { deadline } => apply_alarm(state, deadline, now),
            ApprovalEvent::RunRetired => end_authority(state, AuthorityEnd::Run, now),
            ApprovalEvent::EnrollmentEnded => end_authority(state, AuthorityEnd::Enrollment, now),
            ApprovalEvent::SessionEnded => end_authority(state, AuthorityEnd::ProviderSession, now),
            ApprovalEvent::Reoffer => reoffer(state),
        }
    }

    fn transitions() -> &'static [Transition] {
        crate::approval_lifecycle_edges::TRANSITIONS
    }

    fn next_due(state: &ApprovalState) -> Option<EngineTime> {
        match state {
            ApprovalState::Waiting { saved } => Some(saved.window.deadline()),
            ApprovalState::Unraised
            | ApprovalState::StoppedBeforeRaise { .. }
            | ApprovalState::Ended { .. } => None,
        }
    }
}

fn apply_raise(
    state: &ApprovalState,
    key: ApprovalRequestKey,
    raise: Raise,
    routing: ApprovalRouting,
    admission: RaiseAdmission,
    now: CentralTime,
) -> ApprovalChange {
    if !matches_key(&key, raise.request_id(), raise.router_run())
        || state.key().is_some_and(|saved| saved != &key)
    {
        return reply(state, ApprovalReceipt::Raise(RaiseReply::ContentsDiffer {}));
    }
    match state {
        ApprovalState::Waiting { saved } | ApprovalState::Ended { saved, .. } => {
            let result = if saved.contents_digest == raise_digest(&raise)? {
                RaiseReply::Receipt {
                    receipt: RaiseReceipt {
                        request_id: saved.key.request_id.clone(),
                        window: saved.window,
                    },
                }
            } else {
                RaiseReply::ContentsDiffer {}
            };
            reply(state, ApprovalReceipt::Raise(result))
        }
        ApprovalState::StoppedBeforeRaise {
            contents_digest,
            cause,
            ..
        } => {
            let digest = raise_digest(&raise)?;
            let result = if contents_digest == &digest && stop_cause_matches_raise(*cause, &raise) {
                RaiseReply::AlreadyStopped {}
            } else {
                RaiseReply::ContentsDiffer {}
            };
            reply(state, ApprovalReceipt::Raise(result))
        }
        ApprovalState::Unraised => {
            match (&raise, admission.provider_session) {
                (Raise::UnprovenCall { .. }, ProviderSessionAvailability::SessionFree)
                    if routing == ApprovalRouting::Owner => {}
                (
                    Raise::ProviderApproval { .. } | Raise::ProviderQuestion { .. },
                    ProviderSessionAvailability::Live,
                ) => {}
                (
                    Raise::ProviderApproval { .. } | Raise::ProviderQuestion { .. },
                    ProviderSessionAvailability::NotAvailable,
                ) => {
                    return reply(state, ApprovalReceipt::Raise(RaiseReply::SessionEnded {}));
                }
                (
                    Raise::UnprovenCall { .. },
                    ProviderSessionAvailability::SessionFree
                    | ProviderSessionAvailability::Live
                    | ProviderSessionAvailability::NotAvailable,
                )
                | (
                    Raise::ProviderApproval { .. } | Raise::ProviderQuestion { .. },
                    ProviderSessionAvailability::SessionFree,
                ) => {
                    return Err(TransitionError::Domain(
                        ApprovalRefusal::InvalidInitialRoute,
                    ));
                }
            }
            if admission.outstanding >= MAX_OUTSTANDING_PER_RUN {
                return reply(
                    state,
                    ApprovalReceipt::Raise(RaiseReply::TooManyOutstanding {}),
                );
            }
            let window = ApprovalWindow::from_first_save(now)
                .ok()
                .filter(|window| supported_time(window.deadline()))
                .ok_or(TransitionError::Domain(ApprovalRefusal::DeadlineOutOfRange))?;
            let contents_digest = raise_digest(&raise)?;
            let saved = RetainedApproval {
                key,
                raise,
                contents_digest,
                window,
                routing,
                waiting_status: WaitingStatus::Pending,
            };
            if let ApprovalRouting::Delegate { admitted_parent } = &saved.routing
                && saved.raise.requesting_session() == Some(&admitted_parent.session_id)
            {
                return Err(TransitionError::Domain(
                    ApprovalRefusal::InvalidInitialRoute,
                ));
            }
            let mut effects: Vec<_> = parent_notice(&saved).into_iter().collect();
            effects.push(ApprovalEffect::RegisterDue {
                key: saved.key.clone(),
                deadline: window.deadline(),
            });
            effects.push(ApprovalEffect::Reply(ApprovalReceipt::Raise(
                RaiseReply::Receipt {
                    receipt: RaiseReceipt {
                        request_id: saved.key.request_id.clone(),
                        window,
                    },
                },
            )));
            Ok((
                ApprovalState::Waiting {
                    saved: Box::new(saved),
                },
                effects,
            ))
        }
    }
}

fn raise_digest(raise: &Raise) -> Result<RequestContentsDigest, TransitionError<ApprovalRefusal>> {
    raise
        .contents_digest()
        .map_err(|_| TransitionError::Domain(ApprovalRefusal::InvalidDigest))
}

fn apply_stop(
    state: &ApprovalState,
    key: ApprovalRequestKey,
    stop: &StopRequest,
    now: CentralTime,
) -> ApprovalChange {
    let (request_id, run, digest) = stop_binding(stop);
    if !matches_key(&key, request_id, run) || state.key().is_some_and(|saved| saved != &key) {
        return reply(state, ApprovalReceipt::Stop(StopReply::NotVisible {}));
    }
    let response = |ending| ApprovalReceipt::Stop(StopReply::Stands { ending });
    let cause = StopMarkerCause::from(stop);
    match state {
        ApprovalState::Unraised => Ok((
            ApprovalState::StoppedBeforeRaise {
                key,
                contents_digest: digest.clone(),
                cause,
            },
            vec![ApprovalEffect::Reply(response(
                StopReceiptEnding::Cancelled {
                    reason: CancelReason::Stopped {
                        cause: cause.holder_cause(),
                    },
                },
            ))],
        )),
        ApprovalState::StoppedBeforeRaise {
            contents_digest,
            cause: stored_cause,
            ..
        } if contents_digest == digest && stop_marker_matches_cause(*stored_cause, cause) => reply(
            state,
            response(StopReceiptEnding::Cancelled {
                reason: CancelReason::Stopped {
                    cause: stored_cause.holder_cause(),
                },
            }),
        ),
        ApprovalState::Waiting { saved }
            if &saved.contents_digest == digest
                && stop_cause_matches_raise(cause, &saved.raise) =>
        {
            let ending = deadline_ending(
                saved,
                Ending::Cancelled {
                    reason: CancelReason::Stopped {
                        cause: cause.holder_cause(),
                    },
                },
                now,
            );
            let receipt = response(standing(&ending));
            let (next, mut effects) =
                finish(saved, ending, Delivery::ClosedWithoutReceipt {}, now)?;
            effects.push(ApprovalEffect::Reply(receipt));
            Ok((next, effects))
        }
        ApprovalState::Ended { saved, closed }
            if &saved.contents_digest == digest
                && stop_cause_matches_raise(cause, &saved.raise) =>
        {
            let receipt = response(standing(&closed.ending.clone().into_ending()));
            if closed.delivery == (Delivery::Owed {}) {
                Ok((
                    with_delivery(saved, closed, Delivery::ClosedWithoutReceipt {}),
                    vec![ApprovalEffect::Reply(receipt)],
                ))
            } else {
                reply(state, receipt)
            }
        }
        ApprovalState::StoppedBeforeRaise { .. }
        | ApprovalState::Waiting { .. }
        | ApprovalState::Ended { .. } => {
            reply(state, ApprovalReceipt::Stop(StopReply::NotVisible {}))
        }
    }
}

fn apply_alarm(state: &ApprovalState, deadline: CentralTime, now: CentralTime) -> ApprovalChange {
    match state {
        ApprovalState::Waiting { saved }
            if saved.window.deadline() == deadline && now >= deadline =>
        {
            finish(saved, Ending::TimedOut {}, Delivery::Owed {}, now)
        }
        ApprovalState::Unraised
        | ApprovalState::StoppedBeforeRaise { .. }
        | ApprovalState::Waiting { .. }
        | ApprovalState::Ended { .. } => Err(TransitionError::Stale),
    }
}

#[derive(Clone, Copy)]
enum AuthorityEnd {
    Run,
    Enrollment,
    ProviderSession,
}

fn end_authority(
    state: &ApprovalState,
    authority: AuthorityEnd,
    now: CentralTime,
) -> ApprovalChange {
    let reason = match authority {
        AuthorityEnd::Run => CancelReason::RequesterGone {},
        AuthorityEnd::Enrollment => CancelReason::EnrollmentEnded {},
        AuthorityEnd::ProviderSession => CancelReason::SessionEnded {},
    };
    match (state, authority) {
        (ApprovalState::Waiting { saved }, AuthorityEnd::ProviderSession)
            if saved.raise.requesting_session().is_some() =>
        {
            let ending = deadline_ending(saved, Ending::Cancelled { reason }, now);
            finish(saved, ending, Delivery::Owed {}, now)
        }
        (ApprovalState::Waiting { .. }, AuthorityEnd::ProviderSession) => {
            Ok((state.clone(), Vec::new()))
        }
        (ApprovalState::Waiting { saved }, AuthorityEnd::Run | AuthorityEnd::Enrollment) => {
            let ending = deadline_ending(saved, Ending::Cancelled { reason }, now);
            finish(saved, ending, Delivery::ClosedWithoutReceipt {}, now)
        }
        (ApprovalState::Ended { saved, closed }, AuthorityEnd::Run | AuthorityEnd::Enrollment)
            if closed.delivery == (Delivery::Owed {}) =>
        {
            Ok((
                with_delivery(saved, closed, Delivery::ClosedWithoutReceipt {}),
                Vec::new(),
            ))
        }
        (
            ApprovalState::Unraised
            | ApprovalState::StoppedBeforeRaise { .. }
            | ApprovalState::Ended { .. },
            AuthorityEnd::Run | AuthorityEnd::Enrollment | AuthorityEnd::ProviderSession,
        ) => Ok((state.clone(), Vec::new())),
    }
}

fn deadline_ending(saved: &RetainedApproval, before_deadline: Ending, now: CentralTime) -> Ending {
    if now >= saved.window.deadline() {
        Ending::TimedOut {}
    } else {
        before_deadline
    }
}

#[cfg(test)]
#[path = "approval_lifecycle_edges_test.rs"]
mod tests;
