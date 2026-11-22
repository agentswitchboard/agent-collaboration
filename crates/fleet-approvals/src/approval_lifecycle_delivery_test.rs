use approval_request_protocol::{
    DeliverEndingReply, DeliverEndingRequest, Delivery, Ending, StopReceiptEnding, StopReply,
};

use crate::{ApprovalCollectionEvent, ApprovalReceipt, ApprovalRun, FactorStanding};

use super::fixtures::{
    answer, closed, decide_event, key, offered_frame, person, provider, provider_stop, raise_event,
    reply, routing, run_event, setup,
};

fn observed(
    run: &ApprovalRun,
    request: &DeliverEndingRequest,
    reply: DeliverEndingReply,
) -> ApprovalCollectionEvent {
    ApprovalCollectionEvent::DeliveryReply {
        run: run.clone(),
        request: Box::new(request.clone()),
        reply,
    }
}

#[test]
fn holder_delivery_settles_only_on_exact_received_or_authoritative_not_held() {
    let (mut book, run, requester, _) = setup();
    let item = key(&run, 1);
    let different = key(&run, 2);
    for key in [&item, &different] {
        run_event(
            &mut book,
            raise_event(
                &run,
                provider(key, &requester, "Bound"),
                routing(None, true),
            ),
            0,
        );
    }
    let commit = run_event(
        &mut book,
        decide_event(&item, person(12, FactorStanding::Fresh), 101, answer(1)),
        1,
    );
    let offer = offered_frame(&commit);
    assert_eq!(offer.request_id, item.request_id);
    assert_eq!(offer.ending, closed(&book, &item).ending);
    assert_eq!(
        offer.contents_digest,
        provider(&item, &requester, "Bound")
            .contents_digest()
            .unwrap()
    );
    assert_eq!(closed(&book, &item).delivery, Delivery::Owed {});
    assert_eq!(
        offered_frame(&run_event(
            &mut book,
            ApprovalCollectionEvent::Reoffer { key: item.clone() },
            1
        )),
        offer
    );

    let wrong_ending = DeliverEndingRequest::from_raise(
        &provider(&item, &requester, "Bound"),
        Ending::TimedOut {},
    )
    .unwrap();
    let wrong_contents = DeliverEndingRequest::from_raise(
        &provider(&item, &requester, "Different"),
        offer.ending.clone().into_ending(),
    )
    .unwrap();
    let mut wrong_target = offer.clone();
    wrong_target.request_id = different.request_id;
    for mismatched in [&wrong_ending, &wrong_contents, &wrong_target] {
        run_event(
            &mut book,
            observed(&run, mismatched, DeliverEndingReply::Received {}),
            2,
        );
        assert_eq!(closed(&book, &item).delivery, Delivery::Owed {});
    }
    for reply in [
        DeliverEndingReply::StopOwed {},
        DeliverEndingReply::HolderNotReady {},
        DeliverEndingReply::EndingDiffers {},
    ] {
        run_event(&mut book, observed(&run, &offer, reply), 2);
        assert_eq!(closed(&book, &item).delivery, Delivery::Owed {});
    }

    run_event(
        &mut book,
        observed(&run, &offer, DeliverEndingReply::NotHeld {}),
        3,
    );
    assert_eq!(
        closed(&book, &item).delivery,
        Delivery::ClosedWithoutReceipt {}
    );
    assert!(
        run_event(
            &mut book,
            ApprovalCollectionEvent::Reoffer { key: item.clone() },
            3
        )
        .is_empty()
    );

    run_event(
        &mut book,
        observed(&run, &offer, DeliverEndingReply::Received {}),
        4,
    );
    assert_eq!(closed(&book, &item).delivery, Delivery::ReceivedByHolder {});
    run_event(
        &mut book,
        observed(&run, &offer, DeliverEndingReply::NotHeld {}),
        5,
    );
    run_event(
        &mut book,
        observed(&run, &offer, DeliverEndingReply::Received {}),
        5,
    );
    assert_eq!(closed(&book, &item).delivery, Delivery::ReceivedByHolder {});
}

#[test]
fn a_standing_stop_closes_only_unpaid_delivery_and_keeps_the_original_ending() {
    let (mut book, run, requester, _) = setup();
    let unpaid = key(&run, 1);
    let paid = key(&run, 2);
    for item in [&unpaid, &paid] {
        let raise = provider(item, &requester, "Original");
        run_event(&mut book, raise_event(&run, raise, routing(None, true)), 0);
    }
    run_event(
        &mut book,
        decide_event(&unpaid, person(20, FactorStanding::Fresh), 101, answer(1)),
        1,
    );
    let offer = offered_frame(&run_event(
        &mut book,
        decide_event(&paid, person(20, FactorStanding::Fresh), 102, answer(1)),
        1,
    ));
    run_event(
        &mut book,
        observed(&run, &offer, DeliverEndingReply::Received {}),
        2,
    );
    for (key, expected) in [
        (&unpaid, Delivery::ClosedWithoutReceipt {}),
        (&paid, Delivery::ReceivedByHolder {}),
    ] {
        let raise = provider(key, &requester, "Original");
        let ended_at = closed(&book, key).ended_at;
        let effects = run_event(
            &mut book,
            ApprovalCollectionEvent::Stop {
                run: run.clone(),
                request: provider_stop(&raise),
            },
            3,
        );
        assert_eq!(
            reply(&effects),
            &ApprovalReceipt::Stop(StopReply::Stands {
                ending: StopReceiptEnding::DecidedEarlier {}
            })
        );
        assert!(matches!(
            closed(&book, key).ending.clone().into_ending(),
            Ending::Decided { .. }
        ));
        assert_eq!(closed(&book, key).ended_at, ended_at);
        assert_eq!(closed(&book, key).delivery, expected);
    }
}
