use approval_request_protocol::{
    CancelReason, Ending, HolderReport, RaiseReply, Refusal, ReportDeliveryReply, StopReceiptEnding,
};
use central_engine_model::{Machine, StateTag, TransitionError};

use crate::{
    ApprovalDestination, ApprovalEffect, ApprovalEvent, ApprovalLifecycle, ApprovalReceipt,
    ApprovalRefusal, ApprovalState,
};

use super::fixtures::{
    DEADLINE, apply, approval_raise, holder_report, other_run, raise_event, request_id, router_run,
    step, stop_event, waiting,
};
use super::{allow_from_parent, closed, refused, stop_reply};

pub(super) const SIXTY_DAYS: i64 = 60 * 24 * 3_600_000;

#[test]
fn erasure_at_sixty_days_discards_contents_and_settles_only_unreported_delivery() {
    let (awaiting, _) = apply(&waiting(), allow_from_parent(), 1);
    let (reported, _) = apply(
        &awaiting,
        holder_report(router_run(), HolderReport::Applied),
        2,
    );
    let marker = ApprovalState::Erased {
        request_id: request_id(),
        router_run: router_run(),
        ending: StopReceiptEnding::DecidedEarlier {},
    };
    let cutoff = 1 + SIXTY_DAYS;

    assert_eq!(
        step(&awaiting, ApprovalEvent::Erase, cutoff - 1),
        Err(TransitionError::Stale)
    );
    assert_eq!(
        step(&awaiting, ApprovalEvent::Erase, cutoff),
        Ok((
            marker.clone(),
            vec![ApprovalEffect::SettleErasedDeliveryAsUnknown {
                request_id: request_id()
            }],
        ))
    );
    assert_eq!(
        step(&reported, ApprovalEvent::Erase, cutoff + 1),
        Ok((marker.clone(), vec![]))
    );
    assert!(marker.is_terminal());
    assert_eq!(ApprovalLifecycle::next_due(&marker), None);
    assert_eq!(
        step(&marker, ApprovalEvent::Erase, cutoff + 2),
        Err(TransitionError::Stale)
    );
}

#[test]
fn erased_raise_returns_already_ended_even_when_the_resubmitted_offer_differs() {
    let (awaiting, _) = apply(&waiting(), allow_from_parent(), 1);
    let cutoff = 1 + SIXTY_DAYS;
    let (marker, _) = apply(&awaiting, ApprovalEvent::Erase, cutoff);
    let different = ApprovalEvent::Raise {
        raise: Box::new(approval_raise("Changed after erasure")),
        destinations: vec![ApprovalDestination::People],
    };

    for raise in [raise_event(), different] {
        assert_eq!(
            step(&marker, raise, cutoff + 1),
            Ok((
                marker.clone(),
                vec![ApprovalEffect::Reply(ApprovalReceipt::Raise(
                    RaiseReply::AlreadyEnded {}
                ))],
            ))
        );
    }
    assert_eq!(
        step(&marker, allow_from_parent(), cutoff + 1),
        Err(refused(Refusal::NotVisible {}))
    );
}

#[test]
fn erased_stop_returns_the_original_standing_for_each_ending() {
    let initial = waiting();
    for (event, at, expected) in [
        (allow_from_parent(), 1, StopReceiptEnding::DecidedEarlier {}),
        (
            ApprovalEvent::SessionEnded,
            1,
            StopReceiptEnding::Cancelled {
                reason: CancelReason::SessionEnded {},
            },
        ),
        (
            ApprovalEvent::ClockReached,
            DEADLINE,
            StopReceiptEnding::TimedOut {},
        ),
    ] {
        let (ended, _) = apply(&initial, event, at);
        let (marker, _) = apply(&ended, ApprovalEvent::Erase, at + SIXTY_DAYS);
        assert_eq!(
            step(&marker, stop_event(), at + SIXTY_DAYS + 1),
            Ok((
                ApprovalState::Erased {
                    request_id: request_id(),
                    router_run: router_run(),
                    ending: expected
                },
                vec![stop_reply(expected)],
            ))
        );
    }
}

#[test]
fn erased_report_is_acknowledged_as_no_handover_owed_and_no_ending_is_offered() {
    let (awaiting, _) = apply(&waiting(), ApprovalEvent::SessionEnded, 1);
    assert_eq!(
        closed(&awaiting).ending,
        Ending::Cancelled {
            reason: CancelReason::SessionEnded {}
        }
    );
    let cutoff = 1 + SIXTY_DAYS;
    let (marker, _) = apply(&awaiting, ApprovalEvent::Erase, cutoff);
    let report = holder_report(router_run(), HolderReport::Applied);

    assert_eq!(
        step(&marker, report, cutoff + 1),
        Ok((
            marker.clone(),
            vec![ApprovalEffect::Reply(ApprovalReceipt::Report(
                ReportDeliveryReply::NoHandoverOwed {}
            ))],
        ))
    );
    assert_eq!(
        step(
            &marker,
            holder_report(other_run(), HolderReport::Applied),
            cutoff + 1
        ),
        Err(TransitionError::Stale)
    );
    assert_eq!(
        step(
            &marker,
            ApprovalEvent::OfferAgain {
                router_run: router_run()
            },
            cutoff + 1
        ),
        Err(TransitionError::Domain(ApprovalRefusal::NoHandoverOwed))
    );
}
