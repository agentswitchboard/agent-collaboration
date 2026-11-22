use approval_request_protocol::{
    Delivery, Ending, RaiseReply, Refusal, ReportDeliveryReply, StopReceiptEnding, StopReply,
};
use central_engine_model::{EngineTime, EventTag, StateTag, TransitionError};
use workspace_ids::{ApprovalRequestId, RouterRunId};

use crate::approval_lifecycle_admission::{
    check_scope, destination_refusal, raise_run, stop_scope,
};
use crate::{
    ApprovalEffect, ApprovalEvent, ApprovalReceipt, ApprovalRefusal, ApprovalState, EndedApproval,
    RetainedApproval,
};

const ENDED_RETENTION_DAYS: i64 = 60;

type ErasureResult = Result<(ApprovalState, Vec<ApprovalEffect>), TransitionError<ApprovalRefusal>>;

pub(super) fn erase_ended(
    saved: &RetainedApproval,
    closed: &EndedApproval,
    now: EngineTime,
) -> ErasureResult {
    if now.signed_duration_since(closed.ended_at).num_days() < ENDED_RETENTION_DAYS {
        return Err(TransitionError::Stale);
    }

    let request_id = saved.receipt.request_id.clone();
    let effects = if closed.delivery == (Delivery::AwaitingReport {}) {
        vec![ApprovalEffect::SettleErasedDeliveryAsUnknown {
            request_id: request_id.clone(),
        }]
    } else {
        vec![]
    };
    Ok((
        ApprovalState::Erased {
            request_id,
            router_run: saved.holder_run().clone(),
            ending: which_ending_stands(&closed.ending),
        },
        effects,
    ))
}

pub(super) fn apply_erased_marker(
    state: &ApprovalState,
    request_id: &ApprovalRequestId,
    router_run: &RouterRunId,
    ending: StopReceiptEnding,
    event: ApprovalEvent,
) -> ErasureResult {
    let receipt = match event {
        ApprovalEvent::Raise { raise, .. } => {
            check_scope(
                request_id,
                router_run,
                raise.request_id(),
                raise_run(&raise),
            )?;
            ApprovalReceipt::Raise(RaiseReply::AlreadyEnded {})
        }
        ApprovalEvent::Stop(stop) => {
            let (stop_id, stop_run) = stop_scope(&stop);
            check_scope(request_id, router_run, stop_id, stop_run)?;
            ApprovalReceipt::Stop(StopReply::StopReceipt { ending })
        }
        ApprovalEvent::Report(report) => {
            check_scope(
                request_id,
                router_run,
                &report.request_id,
                &report.router_run,
            )?;
            ApprovalReceipt::Report(ReportDeliveryReply::NoHandoverOwed {})
        }
        ApprovalEvent::OfferAgain {
            router_run: requested_run,
        } => {
            if &requested_run != router_run {
                return Err(TransitionError::Stale);
            }
            return Err(TransitionError::Domain(ApprovalRefusal::NoHandoverOwed));
        }
        ApprovalEvent::Answer { .. } | ApprovalEvent::PassOn { .. } => {
            return Err(destination_refusal(Refusal::NotVisible {}));
        }
        ApprovalEvent::Erase => return Err(TransitionError::Stale),
        event @ (ApprovalEvent::ClockReached
        | ApprovalEvent::RunRetired
        | ApprovalEvent::SessionEnded
        | ApprovalEvent::EnrollmentEnded) => {
            // The run writer deletes the marker when its enclosing run is retired.
            return Err(TransitionError::NotAllowed {
                state: state.state_tag(),
                event: event.event_tag(),
            });
        }
    };
    Ok((state.clone(), vec![ApprovalEffect::Reply(receipt)]))
}

pub(super) fn which_ending_stands(ending: &Ending) -> StopReceiptEnding {
    match ending {
        Ending::Decided { .. } => StopReceiptEnding::DecidedEarlier {},
        Ending::Cancelled { reason } => StopReceiptEnding::Cancelled { reason: *reason },
        Ending::TimedOut {} => StopReceiptEnding::TimedOut {},
    }
}
