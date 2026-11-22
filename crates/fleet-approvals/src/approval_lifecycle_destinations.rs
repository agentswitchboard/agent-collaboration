use approval_request_protocol::{
    AdmittedDestination, CentralTime, DecideRequest, DecidedBy, DecisionFactor, Delivery, Ending,
    EndingView, PassOnReply, RequestView, WaitingStatus,
};
use central_engine_model::TransitionError;

use crate::approval_lifecycle_admission::{is_assigned_parent, is_destination};
use crate::approval_lifecycle_effects::{ApprovalChange, finish, reply};
use crate::{
    AdmittedDecider, ApprovalActor, ApprovalDecisionReply, ApprovalDestination, ApprovalEffect,
    ApprovalReceipt, ApprovalRefusal, ApprovalRequestKey, ApprovalSession, ApprovalState,
    FactorStanding, RetainedApproval, SavedDecision,
};

fn visible<'a>(
    state: &'a ApprovalState,
    key: &ApprovalRequestKey,
    actor: &ApprovalActor,
) -> Option<&'a RetainedApproval> {
    let retained = state.retained()?;
    if &retained.key != key || !is_destination(retained, actor) {
        return None;
    }
    Some(retained)
}

pub(super) fn decide(
    state: &ApprovalState,
    key: &ApprovalRequestKey,
    decider: &AdmittedDecider,
    request: DecideRequest,
    prior: Option<&SavedDecision>,
    now: CentralTime,
) -> ApprovalChange {
    let actor = decider.actor();
    let Some(saved) = visible(state, key, &actor).filter(|_| request.request_id == key.request_id)
    else {
        return reply(
            state,
            ApprovalReceipt::Decide(ApprovalDecisionReply::NotVisible {}),
        );
    };
    if let Some(prior) = prior {
        let result = if &prior.key == key
            && prior.actor == actor
            && prior.value == request.value
            && prior.operation_id == request.operation_id
        {
            prior.reply.clone()
        } else {
            ApprovalDecisionReply::OperationConflict {}
        };
        return reply(state, ApprovalReceipt::Decide(result));
    }
    let identity = actor.identity();
    let admitted = checked_destination(saved, &identity)?;
    let (next, mut effects, result) = match state {
        ApprovalState::Ended { closed, .. } => (
            state.clone(),
            vec![],
            ApprovalDecisionReply::Standing {
                ending: EndingView::for_destination(
                    &admitted,
                    &crate::approval_lifecycle_effects::ending_frame(saved, closed),
                )
                .map_err(|_| TransitionError::Domain(ApprovalRefusal::InvalidRetainedEnding))?
                .view()
                .clone(),
            },
        ),
        ApprovalState::Waiting { .. } if now >= saved.window.deadline() => {
            let (next, effects) = finish(saved, Ending::TimedOut {}, Delivery::Owed {}, now)?;
            (next, effects, ApprovalDecisionReply::TooLate {})
        }
        ApprovalState::Waiting { .. } => {
            let factor = if matches!(
                decider,
                AdmittedDecider::Person {
                    factor: FactorStanding::Fresh,
                    ..
                }
            ) {
                DecisionFactor::FreshPersonFactor
            } else {
                DecisionFactor::NoFreshPersonFactor
            };
            if let Ok(ending) = admitted.decide(request.value.clone(), factor) {
                let (next, effects) = finish(saved, ending, Delivery::Owed {}, now)?;
                (
                    next,
                    effects,
                    ApprovalDecisionReply::SavedDecision {
                        answer: request.value.clone(),
                        by: identity,
                    },
                )
            } else {
                (state.clone(), vec![], ApprovalDecisionReply::Invalid {})
            }
        }
        ApprovalState::Unraised | ApprovalState::StoppedBeforeRaise { .. } => {
            return reply(
                state,
                ApprovalReceipt::Decide(ApprovalDecisionReply::NotVisible {}),
            );
        }
    };
    effects.push(ApprovalEffect::SaveDecision(Box::new(SavedDecision {
        key: key.clone(),
        actor,
        operation_id: request.operation_id,
        value: request.value,
        reply: result.clone(),
    })));
    effects.push(ApprovalEffect::Reply(ApprovalReceipt::Decide(result)));
    Ok((next, effects))
}

pub(super) fn read(
    state: &ApprovalState,
    key: &ApprovalRequestKey,
    actor: &ApprovalActor,
    now: CentralTime,
) -> ApprovalChange {
    let Some(saved) = visible(state, key, actor) else {
        return reply(state, ApprovalReceipt::Read(None));
    };
    if matches!(state, ApprovalState::Waiting { .. }) && now >= saved.window.deadline() {
        let (next, mut effects) = finish(saved, Ending::TimedOut {}, Delivery::Owed {}, now)?;
        let view = project_view(&next, saved, actor)?;
        effects.push(ApprovalEffect::Reply(ApprovalReceipt::Read(Some(
            Box::new(view),
        ))));
        return Ok((next, effects));
    }
    let view = project_view(state, saved, actor)?;
    reply(state, ApprovalReceipt::Read(Some(Box::new(view))))
}

fn project_view(
    state: &ApprovalState,
    saved: &RetainedApproval,
    actor: &ApprovalActor,
) -> Result<RequestView, TransitionError<ApprovalRefusal>> {
    let identity = actor.identity();
    let admitted = checked_destination(saved, &identity)?;
    match state {
        ApprovalState::Waiting { .. } => Ok(RequestView::waiting(
            &admitted,
            saved.waiting_status,
            saved.window,
        )),
        ApprovalState::Ended { closed, .. } => RequestView::ended(
            &admitted,
            saved.window,
            &crate::approval_lifecycle_effects::ending_frame(saved, closed),
            closed.ended_at,
            closed.delivery,
        ),
        ApprovalState::Unraised | ApprovalState::StoppedBeforeRaise { .. } => {
            return Err(TransitionError::Stale);
        }
    }
    .map(|view| view.view().clone())
    .map_err(|_| TransitionError::Domain(ApprovalRefusal::InvalidRetainedEnding))
}

fn checked_destination<'a>(
    saved: &'a RetainedApproval,
    actor: &'a DecidedBy,
) -> Result<AdmittedDestination<'a>, TransitionError<ApprovalRefusal>> {
    // visible() checks the actor's current membership in the same retained row before this binding.
    AdmittedDestination::for_request(&saved.raise, actor, std::slice::from_ref(actor))
        .map_err(|_| TransitionError::Domain(ApprovalRefusal::InvalidRetainedEnding))
}

pub(super) fn pass_on(
    state: &ApprovalState,
    key: &ApprovalRequestKey,
    parent: &ApprovalSession,
    now: CentralTime,
) -> ApprovalChange {
    let actor = ApprovalActor::Session(parent.clone());
    let Some(saved) = visible(state, key, &actor).filter(|saved| is_assigned_parent(saved, parent))
    else {
        return reply(state, ApprovalReceipt::PassOn(PassOnReply::NotVisible {}));
    };
    let forwarded = saved.waiting_status == WaitingStatus::PassedOn;
    match state {
        ApprovalState::Waiting { .. } if now >= saved.window.deadline() => {
            let (next, mut effects) = finish(saved, Ending::TimedOut {}, Delivery::Owed {}, now)?;
            let result = if forwarded {
                PassOnReply::Forwarded {}
            } else {
                PassOnReply::TooLate {}
            };
            effects.push(ApprovalEffect::Reply(ApprovalReceipt::PassOn(result)));
            Ok((next, effects))
        }
        ApprovalState::Waiting { .. } if !forwarded => {
            let mut saved = Box::new(saved.clone());
            saved.waiting_status = WaitingStatus::PassedOn;
            saved
                .routing
                .destinations
                .insert(ApprovalDestination::People);
            Ok((
                ApprovalState::Waiting { saved },
                vec![ApprovalEffect::Reply(ApprovalReceipt::PassOn(
                    PassOnReply::Forwarded {},
                ))],
            ))
        }
        ApprovalState::Waiting { .. } | ApprovalState::Ended { .. } => reply(
            state,
            ApprovalReceipt::PassOn(if forwarded {
                PassOnReply::Forwarded {}
            } else {
                PassOnReply::Ended {}
            }),
        ),
        ApprovalState::Unraised | ApprovalState::StoppedBeforeRaise { .. } => {
            reply(state, ApprovalReceipt::PassOn(PassOnReply::NotVisible {}))
        }
    }
}
