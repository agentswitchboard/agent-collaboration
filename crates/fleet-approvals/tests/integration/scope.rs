use approval_request_protocol::{
    CancelReason, DeliverEndingReply, Delivery, Ending, RaiseReply, RegisterRunReply, RunRefusal,
    StopReply,
};

use fleet_approvals::{
    ApprovalCollection, ApprovalCollectionEffect, ApprovalCollectionEvent, ApprovalRun,
    ApprovalState, FactorStanding, ProviderSessionAvailability,
};

use super::fixtures::{
    HOUR, ObservedReply as ApprovalReceipt, answer, closed, decide_event, enrollment, key,
    offered_frame, person, provider, provider_stop, raise_event, register, reply, router_run,
    routing, run_event, session, setup,
};

#[test]
fn ending_one_session_settles_its_whole_batch_and_rejects_an_unavailable_first_raise() {
    let (mut book, run, requester, _) = setup();
    let old = key(&run, 1);
    let later = key(&run, 2);
    let other = key(&run, 3);
    let another_session = session(&run.enrollment, 8010);
    let route = routing(None);
    run_event(
        &mut book,
        raise_event(&run, provider(&old, &requester, "Older"), route.clone()),
        0,
    );
    let replay = provider(&later, &requester, "Later");
    let receipt = run_event(
        &mut book,
        raise_event(&run, replay.clone(), route.clone()),
        HOUR - 1,
    );
    run_event(
        &mut book,
        raise_event(
            &run,
            provider(&other, &another_session, "Other"),
            route.clone(),
        ),
        HOUR - 1,
    );

    run_event(
        &mut book,
        ApprovalCollectionEvent::EndSession {
            session: requester.clone(),
        },
        HOUR,
    );
    assert!(matches!(book.requests[&old], ApprovalState::Ended { .. }));
    assert!(matches!(book.requests[&later], ApprovalState::Ended { .. }));
    assert_eq!(
        closed(&book, &old).ending.clone().into_ending(),
        Ending::TimedOut {}
    );
    assert_eq!(
        closed(&book, &later).ending.clone().into_ending(),
        Ending::Cancelled {
            reason: CancelReason::SessionEnded {}
        }
    );
    assert_eq!(closed(&book, &later).delivery, Delivery::Owed {});
    assert!(matches!(
        book.requests[&other],
        ApprovalState::Waiting { .. }
    ));
    assert_eq!(
        reply(&run_event(
            &mut book,
            raise_event(&run, replay, route.clone()),
            HOUR
        )),
        reply(&receipt)
    );

    let never_opened = key(&run, 4);
    let unavailable = key(&run, 5);
    let unavailable_raise = ApprovalCollectionEvent::Raise {
        run: run.clone(),
        raise: Box::new(provider(&unavailable, &another_session, "Unavailable")),
        routing: route.clone(),
        provider_session: ProviderSessionAvailability::NotAvailable,
    };
    for event in [
        raise_event(&run, provider(&never_opened, &requester, "Closed"), route),
        unavailable_raise,
    ] {
        assert_eq!(
            reply(&run_event(&mut book, event, HOUR)),
            ApprovalReceipt::Raise(RaiseReply::SessionEnded {})
        );
    }
    assert!(!book.requests.contains_key(&never_opened));
    assert!(!book.requests.contains_key(&unavailable));
}

#[test]
fn capacity_counts_waiting_plus_unpaid_and_never_rejects_an_exact_replay() {
    let (mut book, run, requester, _) = setup();
    let first = key(&run, 1);
    let first_raise = provider(&first, &requester, "First");
    let route = routing(None);
    let initial_receipt = run_event(
        &mut book,
        raise_event(&run, first_raise.clone(), route.clone()),
        0,
    );
    let offer = offered_frame(&run_event(
        &mut book,
        decide_event(&first, person(12, FactorStanding::Fresh), 2000, answer(1)),
        1,
    ));
    for ordinal in 2..=256 {
        let row = key(&run, ordinal);
        let effects = run_event(
            &mut book,
            raise_event(&run, provider(&row, &requester, "Waiting"), route.clone()),
            1,
        );
        assert!(matches!(
            reply(&effects),
            ApprovalReceipt::Raise(RaiseReply::Receipt { .. })
        ));
    }
    let excess = key(&run, 257);
    let attempt = raise_event(
        &run,
        provider(&excess, &requester, "Overflow"),
        route.clone(),
    );
    assert_eq!(
        reply(&run_event(&mut book, attempt.clone(), 1)),
        ApprovalReceipt::Raise(RaiseReply::TooManyOutstanding {})
    );
    assert!(!book.requests.contains_key(&excess));
    assert_eq!(
        reply(&run_event(
            &mut book,
            raise_event(&run, first_raise, route),
            1
        )),
        reply(&initial_receipt)
    );
    run_event(
        &mut book,
        ApprovalCollectionEvent::DeliveryReply {
            run,
            request: Box::new(offer),
            reply: DeliverEndingReply::NotHeld {},
        },
        2,
    );
    assert!(matches!(
        reply(&run_event(&mut book, attempt, 2)),
        ApprovalReceipt::Raise(RaiseReply::Receipt { .. })
    ));
}

#[test]
fn restart_fences_old_authority_and_same_organization_id_reuse_reports_only_collision() {
    let (mut book, old, requester, _) = setup();
    let waiting = key(&old, 1);
    let owed = key(&old, 2);
    let route = routing(None);
    for row in [&waiting, &owed] {
        run_event(
            &mut book,
            raise_event(&old, provider(row, &requester, "Old run"), route.clone()),
            0,
        );
    }
    let old_offer = offered_frame(&run_event(
        &mut book,
        decide_event(&owed, person(12, FactorStanding::Fresh), 101, answer(1)),
        1,
    ));
    let incoming = ApprovalRun {
        enrollment: old.enrollment.clone(),
        router_run: router_run(20),
    };
    assert_eq!(
        register(&mut book, &incoming, 2).last(),
        Some(&ApprovalCollectionEffect::Registered(
            RegisterRunReply::Registered {}
        ))
    );
    assert_eq!(
        closed(&book, &waiting).ending.clone().into_ending(),
        Ending::Cancelled {
            reason: CancelReason::RequesterGone {}
        }
    );
    assert_eq!(
        closed(&book, &owed).delivery,
        Delivery::ClosedWithoutReceipt {}
    );
    run_event(
        &mut book,
        ApprovalCollectionEvent::DeliveryReply {
            run: old.clone(),
            request: Box::new(old_offer),
            reply: DeliverEndingReply::Received {},
        },
        2,
    );
    assert_eq!(
        closed(&book, &owed).delivery,
        Delivery::ClosedWithoutReceipt {}
    );
    let conflicting_id = key(&incoming, 1);
    let conflict = raise_event(
        &incoming,
        provider(
            &conflicting_id,
            &requester,
            "A different run cannot reuse this id",
        ),
        route.clone(),
    );
    assert_eq!(
        reply(&run_event(&mut book, conflict, 2)),
        ApprovalReceipt::Raise(RaiseReply::ContentsDiffer {})
    );
    let compared_stop = ApprovalCollectionEvent::Stop {
        run: incoming.clone(),
        request: provider_stop(&provider(&conflicting_id, &requester, "Different new body")),
    };
    assert_eq!(
        reply(&run_event(&mut book, compared_stop, 2)),
        ApprovalReceipt::Stop(StopReply::NotVisible {})
    );
    assert!(!book.requests.contains_key(&conflicting_id));
    let lost_raise = key(&old, 3);
    let old_raise = provider(&lost_raise, &requester, "Old unseen");
    let old_event = raise_event(&old, old_raise.clone(), route.clone());
    assert_eq!(
        reply(&run_event(&mut book, old_event.clone(), 2)),
        ApprovalReceipt::Raise(RaiseReply::AuthorityRefused {
            refusal: RunRefusal::RunRetired {}
        })
    );
    assert!(!book.requests.contains_key(&lost_raise));
    assert_eq!(
        register(&mut book, &old, 3),
        [ApprovalCollectionEffect::Registered(
            RegisterRunReply::RunRetired {}
        )]
    );
    assert_eq!(
        register(&mut book, &incoming, 3),
        [ApprovalCollectionEffect::Registered(
            RegisterRunReply::Registered {}
        )]
    );
    let active_wait = key(&incoming, 4);
    run_event(
        &mut book,
        raise_event(
            &incoming,
            provider(&active_wait, &requester, "Active"),
            route,
        ),
        3,
    );
    let installation = enrollment(8000, 8888);
    run_event(
        &mut book,
        ApprovalCollectionEvent::ActivateEnrollment {
            enrollment: installation.clone(),
        },
        HOUR + 3,
    );
    assert_eq!(
        closed(&book, &active_wait).ending.clone().into_ending(),
        Ending::TimedOut {}
    );
    assert_eq!(
        closed(&book, &active_wait).delivery,
        Delivery::ClosedWithoutReceipt {}
    );
    assert_eq!(
        reply(&run_event(&mut book, old_event, HOUR + 3)),
        ApprovalReceipt::Raise(RaiseReply::AuthorityRefused {
            refusal: RunRefusal::EnrollmentEnded {}
        })
    );
    assert_eq!(
        reply(&run_event(
            &mut book,
            ApprovalCollectionEvent::Stop {
                run: old,
                request: provider_stop(&old_raise)
            },
            HOUR + 3
        )),
        ApprovalReceipt::Stop(StopReply::AuthorityRefused {
            refusal: RunRefusal::EnrollmentEnded {}
        })
    );
    let replacement = ApprovalRun {
        enrollment: installation,
        router_run: router_run(9000),
    };
    assert_eq!(
        register(&mut book, &replacement, HOUR + 3),
        [ApprovalCollectionEffect::Registered(
            RegisterRunReply::Registered {}
        )]
    );
}

#[test]
fn a_holder_frame_before_registration_cannot_create_a_request() {
    let authority = enrollment(8800, 8801);
    let run = ApprovalRun {
        enrollment: authority.clone(),
        router_run: router_run(8880),
    };
    let mut book = ApprovalCollection::default();
    run_event(
        &mut book,
        ApprovalCollectionEvent::ActivateEnrollment {
            enrollment: authority.clone(),
        },
        0,
    );
    let request = key(&run, 1);
    let event = raise_event(
        &run,
        provider(&request, &session(&authority, 8882), "Request"),
        routing(None),
    );
    assert_eq!(
        reply(&run_event(&mut book, event.clone(), 0)),
        ApprovalReceipt::Raise(RaiseReply::AuthorityRefused {
            refusal: RunRefusal::RunNotRegistered {},
        })
    );
    assert!(book.requests.is_empty());
    register(&mut book, &run, 1);
    assert!(matches!(
        reply(&run_event(&mut book, event, 1)),
        ApprovalReceipt::Raise(RaiseReply::Receipt { .. })
    ));
}
