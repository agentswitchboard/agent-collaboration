use approval_request_protocol::{
    AdmittedDestination, CentralTime, DecidedBy, DecisionFactor, Delivery, Ending, EndingView,
    PassOnReply, RequestView, WaitingStatus,
};
use central_engine_model::TransitionError;

use crate::approval_lifecycle_admission::{is_assigned_parent, is_destination};
use crate::approval_lifecycle_effects::{ApprovalChange, finish, finish_decided, reply};
use crate::{
    AdmittedDecider, ApprovalActor, ApprovalDecideInput, ApprovalDecisionReply, ApprovalEffect,
    ApprovalReceipt, ApprovalRefusal, ApprovalRequestKey, ApprovalSession, ApprovalState,
    DestinationReply, FactorStanding, RetainedApproval, SavedDecision,
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
    request: ApprovalDecideInput,
    prior: Option<&SavedDecision>,
    now: CentralTime,
) -> ApprovalChange {
    let actor = decider.actor();
    let identity = actor.identity();
    let Some(saved) = visible(state, key, &actor).filter(|_| request.request_id == key.request_id)
    else {
        return reply(
            state,
            ApprovalReceipt::decide_for(key, identity, ApprovalDecisionReply::NotVisible {}),
        );
    };
    if let Some(prior) = prior {
        if &prior.key != key
            || prior.actor != actor
            || prior.value != request.value
            || prior.operation_id != request.operation_id
        {
            return reply(
                state,
                ApprovalReceipt::decide_for(
                    key,
                    identity,
                    ApprovalDecisionReply::OperationConflict {},
                ),
            );
        }
        prior
            .reply
            .for_recipient(key, &identity)
            .map_err(|_| TransitionError::Domain(ApprovalRefusal::InvalidRetainedEnding))?;
        return reply(
            state,
            ApprovalReceipt::Decide(Box::new(prior.reply.clone())),
        );
    }
    let admitted = checked_destination(saved, &identity)?;
    let (next, mut effects, result) = match state {
        ApprovalState::Ended { closed, .. } => (
            state.clone(),
            vec![],
            ApprovalDecisionReply::Standing {
                ending: EndingView::for_destination(
                    &admitted,
                    &crate::approval_lifecycle_effects::ending_frame(closed),
                )
                .map_err(|_| TransitionError::Domain(ApprovalRefusal::InvalidRetainedEnding))?
                .for_recipient(&identity)
                .map_err(|_| TransitionError::Domain(ApprovalRefusal::InvalidRetainedEnding))?,
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
                let (next, effects) = finish_decided(saved, ending, now)?;
                (
                    next,
                    effects,
                    ApprovalDecisionReply::SavedDecision {
                        answer: request.value.clone(),
                        by: identity.clone(),
                    },
                )
            } else {
                (state.clone(), vec![], ApprovalDecisionReply::Invalid {})
            }
        }
        ApprovalState::Unraised | ApprovalState::StoppedBeforeRaise { .. } => {
            return reply(
                state,
                ApprovalReceipt::decide_for(key, identity, ApprovalDecisionReply::NotVisible {}),
            );
        }
    };
    let bound = DestinationReply::new(key, identity, result);
    effects.push(ApprovalEffect::SaveDecision(Box::new(SavedDecision {
        key: key.clone(),
        actor,
        operation_id: request.operation_id,
        value: request.value,
        reply: bound.clone(),
    })));
    effects.push(ApprovalEffect::Reply(ApprovalReceipt::Decide(Box::new(
        bound,
    ))));
    Ok((next, effects))
}

pub(super) fn read(
    state: &ApprovalState,
    key: &ApprovalRequestKey,
    actor: &ApprovalActor,
    now: CentralTime,
) -> ApprovalChange {
    let Some(saved) = visible(state, key, actor) else {
        return reply(
            state,
            ApprovalReceipt::read_for(key, actor.identity(), None),
        );
    };
    if matches!(state, ApprovalState::Waiting { .. }) && now >= saved.window.deadline() {
        let (next, mut effects) = finish(saved, Ending::TimedOut {}, Delivery::Owed {}, now)?;
        let view = project_view(&next, saved, actor)?;
        effects.push(ApprovalEffect::Reply(ApprovalReceipt::read_for(
            key,
            actor.identity(),
            Some(view),
        )));
        return Ok((next, effects));
    }
    let view = project_view(state, saved, actor)?;
    reply(
        state,
        ApprovalReceipt::read_for(key, actor.identity(), Some(view)),
    )
}

fn project_view(
    state: &ApprovalState,
    saved: &RetainedApproval,
    actor: &ApprovalActor,
) -> Result<RequestView, TransitionError<ApprovalRefusal>> {
    let identity = actor.identity();
    let admitted = checked_destination(saved, &identity)?;
    match state {
        ApprovalState::Waiting { .. } => {
            RequestView::waiting(&admitted, saved.waiting_status, saved.window)
        }
        ApprovalState::Ended { closed, .. } => RequestView::ended(
            &admitted,
            saved.window,
            &crate::approval_lifecycle_effects::ending_frame(closed),
            closed.ended_at,
            closed.delivery,
        ),
        ApprovalState::Unraised | ApprovalState::StoppedBeforeRaise { .. } => {
            return Err(TransitionError::Stale);
        }
    }
    .and_then(|view| view.for_recipient(&identity))
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
    let for_parent = |payload| ApprovalReceipt::pass_on_for(key, actor.identity(), payload);
    let Some(saved) = visible(state, key, &actor).filter(|saved| is_assigned_parent(saved, parent))
    else {
        return reply(state, for_parent(PassOnReply::NotVisible {}));
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
            effects.push(ApprovalEffect::Reply(for_parent(result)));
            Ok((next, effects))
        }
        ApprovalState::Waiting { .. } if !forwarded => {
            let mut saved = Box::new(saved.clone());
            saved.waiting_status = WaitingStatus::PassedOn;
            Ok((
                ApprovalState::Waiting { saved },
                vec![ApprovalEffect::Reply(for_parent(PassOnReply::Forwarded {}))],
            ))
        }
        ApprovalState::Waiting { .. } | ApprovalState::Ended { .. } => reply(
            state,
            for_parent(if forwarded {
                PassOnReply::Forwarded {}
            } else {
                PassOnReply::Ended {}
            }),
        ),
        ApprovalState::Unraised | ApprovalState::StoppedBeforeRaise { .. } => {
            reply(state, for_parent(PassOnReply::NotVisible {}))
        }
    }
}
