use super::*;
use std::sync::Arc;
use switchboard_harness_shared_mechanics::opening::OpeningOffer;
use tokio::sync::mpsc::error::TryRecvError;

fn run() -> TransportRun {
    TransportRun {
        incarnation: OwnerIncarnation("019ac19f-aa00-7000-8000-000000000001".parse().unwrap()),
        generation: SessionGeneration::from_number(1),
    }
}

fn close_attempt() -> CloseAttempt {
    CloseAttempt("019ac19f-aa00-7000-8000-000000000002".parse().unwrap())
}

fn authorization(run: TransportRun, input: &str) -> DispatchAuthorization {
    DispatchAuthorization {
        incarnation: run.incarnation,
        run: run.generation,
        input_id: InputId::new(input).unwrap(),
        authorization_seq: 1,
        target: AuthorizationTarget::Session,
    }
}

fn transport() -> (
    AcpTransport<&'static str>,
    mpsc::Receiver<OrderedOutput<&'static str>>,
) {
    AcpTransport::new(NonZeroUsize::MIN)
}

#[test]
fn a_refused_frame_can_read_the_transport_in_its_destructor() {
    use std::sync::{OnceLock, Weak};

    #[derive(Debug, Eq, PartialEq)]
    enum OnDrop {
        ReadSession(Option<String>),
        GuardHeld,
        TransportGone,
    }

    struct Frame {
        transport: Weak<AcpTransport<Frame>>,
        live_run: TransportRun,
        observed: Arc<OnceLock<OnDrop>>,
    }

    impl Drop for Frame {
        fn drop(&mut self) {
            let result = match self.transport.upgrade() {
                None => OnDrop::TransportGone,
                Some(transport) => {
                    // Probe without waiting, so the wrong build records GuardHeld
                    // rather than attempting a same-thread lock and hanging.
                    let unlocked = transport.state.try_lock().is_ok();
                    if unlocked {
                        OnDrop::ReadSession(transport.session_for_route(self.live_run))
                    } else {
                        OnDrop::GuardHeld
                    }
                }
            };
            let _ = self.observed.set(result);
        }
    }

    let live_run = run();
    let missing_run = TransportRun {
        generation: SessionGeneration::from_number(2),
        ..live_run
    };
    let (transport, _output) = AcpTransport::new(NonZeroUsize::MIN);
    let transport = Arc::new(transport);
    transport.open_route(live_run, "still live".into());
    let observed = Arc::new(OnceLock::new());
    let frame = Frame {
        transport: Arc::downgrade(&transport),
        live_run,
        observed: Arc::clone(&observed),
    };

    let refusal = transport.offer_work(authorization(missing_run, "refused"), None, frame);

    assert_eq!(refusal, Err(TransportOfferRefusal::RouteUnavailable));
    assert_eq!(
        observed.get(),
        Some(&OnDrop::ReadSession(Some("still live".into())))
    );
}

#[test]
fn a_removed_but_keyed_route_still_takes_work_and_is_absent_from_name_lookup() {
    let (transport, mut output) = transport();
    let run = run();
    let route = LifecycleRoute::new(run);
    assert_eq!(
        transport.install_lifecycle_route(&route, "session".into()),
        Ok(false)
    );
    assert!(transport.activate_lifecycle_route(&route));
    let turn = TurnId("019ac19f-aa00-7000-8000-000000000003".parse().unwrap());
    transport.apply_membership(
        run.incarnation,
        TurnMembership::ObservedEligible {
            run: run.generation,
            turn,
        },
    );

    route.mark_removed();
    let key = authorization(run, "after removal");
    assert_eq!(
        transport.offer_work(key.clone(), None, "keyed work"),
        Ok(())
    );
    assert_eq!(transport.take(&key), Ok(DispatchResult::Taken(key)));
    let sent = output.try_recv().unwrap();
    assert_eq!((sent.run, sent.frame), (run, "keyed work"));
    assert_eq!(transport.route_for_session("session"), None);
    assert_eq!(transport.current_scope("session"), None);
    assert!(!transport.has_lifecycle_route(&route));
    assert_eq!(
        transport.resolve_callback("unseen", CallbackKind::Request),
        CallbackResolution::NeverObserved {
            sole_eligible: Some((run, turn))
        },
    );
    assert!(transport.retire_lifecycle_route(&route));
    assert_eq!(transport.session_for_route(run), None);
}

#[test]
fn a_replaced_instance_retires_its_claims_and_keeps_all_offer_evidence() {
    let (transport, mut output) = transport();
    let run = run();
    let old = LifecycleRoute::new(run);
    assert_eq!(
        transport.install_lifecycle_route(&old, "session".into()),
        Ok(false)
    );
    assert!(transport.activate_lifecycle_route(&old));
    let turn = TurnId("019ac19f-aa00-7000-8000-000000000003".parse().unwrap());
    transport.apply_membership(
        run.incarnation,
        TurnMembership::ObservedEligible {
            run: run.generation,
            turn,
        },
    );
    assert!(matches!(
        transport.observe_tool(run, "old-claim", ToolObservation::Announcement),
        ToolOwnershipEffect::ForwardFrame { .. }
    ));
    assert_eq!(
        transport.resolve_callback("old-claim", CallbackKind::Request),
        CallbackResolution::Exact { run, turn }
    );

    let taken = authorization(run, "first-taken");
    let offered = authorization(run, "second-offered");
    let revoked = authorization(run, "third-revoked");
    assert_eq!(transport.offer_work(taken.clone(), None, "taken"), Ok(()));
    assert_eq!(
        transport.take(&taken),
        Ok(DispatchResult::Taken(taken.clone()))
    );
    assert_eq!(output.try_recv().unwrap().frame, "taken");
    assert_eq!(
        transport.offer_work(offered.clone(), None, "offered"),
        Ok(())
    );
    assert_eq!(
        transport.offer_work(revoked.clone(), None, "revoked"),
        Ok(())
    );
    assert_eq!(
        transport.revoke(&revoked),
        Ok(DispatchResult::Revoked(revoked.clone()))
    );

    old.mark_removed();
    let next = LifecycleRoute::new(run);
    assert_eq!(
        transport.install_lifecycle_route(&next, "successor".into()),
        Ok(true)
    );

    assert!(!transport.has_lifecycle_route(&old));
    assert!(transport.has_lifecycle_route(&next));
    assert!(!transport.retire_lifecycle_route(&old));
    assert_eq!(transport.route_for_session("session"), None);
    assert_eq!(transport.route_for_session("successor"), Some(run));
    assert!(transport.activate_lifecycle_route(&next));
    // An owned claim for the old turn cannot resolve into an idle continuation.
    // The retired claim can, even though the successor has a different session name.
    assert_eq!(
        transport.resolve_callback("old-claim", CallbackKind::Continuation),
        CallbackResolution::ExactIdle {
            run,
            item_id: "old-claim".into(),
        }
    );
    assert_eq!(transport.current_turn(run), None);

    let successor_turn = TurnId("019ac19f-aa00-7000-8000-000000000004".parse().unwrap());
    transport.apply_membership(
        run.incarnation,
        TurnMembership::ObservedEligible {
            run: run.generation,
            turn: successor_turn,
        },
    );
    assert_eq!(transport.current_turn(run), Some(successor_turn));
    assert_eq!(
        transport.resolve_callback("old-claim", CallbackKind::Request),
        CallbackResolution::Retired
    );
    for key in [&taken, &offered, &revoked] {
        assert_eq!(
            transport.offer_work(key.clone(), None, "duplicate"),
            Err(TransportOfferRefusal::AuthorizationSequenceOnRecord),
        );
    }
    assert_eq!(
        transport.revoke_incarnation(run.incarnation),
        [
            (taken, SlotEvidence::Taken),
            (offered, SlotEvidence::Revoked),
            (revoked, SlotEvidence::Revoked),
        ],
    );

    let attempt = close_attempt();
    let stale = OpeningOffer::default();
    assert_eq!(
        transport.offer_lifecycle_close(&old, attempt, &stale, "stale close"),
        Err(TransportRefusal::RouteUnavailable)
    );
    assert_eq!(
        transport.enqueue_lifecycle_request(&old, &stale, "stale request"),
        Err(TransportRefusal::RouteUnavailable)
    );
    assert!(!stale.was_taken_or_withdraw());
    assert_eq!(transport.closing_marker_for_test(run), None);
    assert_eq!(
        transport.offer_lifecycle_close(
            &next,
            attempt,
            &OpeningOffer::default(),
            "successor close"
        ),
        Ok(())
    );
    assert_eq!(
        transport.reopen_lifecycle_route(&old, attempt),
        ReadmitOutcome::RouteLost
    );
    assert_eq!(transport.closing_marker_for_test(run), Some(attempt));
    assert_eq!(output.try_recv().unwrap().frame, "successor close");
    assert!(matches!(output.try_recv(), Err(TryRecvError::Empty)));
}

#[test]
fn an_unsent_close_leaves_the_route_open_and_does_not_take_the_lifecycle_claim() {
    let (transport, mut output) = transport();
    let run = run();
    let route = LifecycleRoute::new(run);
    assert_eq!(
        transport.install_lifecycle_route(&route, "session".into()),
        Ok(false)
    );
    let attempt = close_attempt();
    let claim = OpeningOffer::default();
    assert_eq!(transport.enqueue_control(run, "occupied"), Ok(()));

    assert_eq!(
        transport.enqueue_close(run, attempt, "keyed close"),
        Err(TransportRefusal::WriterFull)
    );
    assert_eq!(
        transport.offer_lifecycle_close(&route, attempt, &claim, "exact close"),
        Err(TransportRefusal::WriterFull)
    );
    assert_eq!(transport.closing_marker_for_test(run), None);
    assert_eq!(output.try_recv().unwrap().frame, "occupied");
    assert_eq!(
        transport.offer_lifecycle_close(&route, attempt, &claim, "exact close"),
        Ok(())
    );
    assert!(claim.was_taken_or_withdraw());
    assert_eq!(transport.closing_marker_for_test(run), Some(attempt));
    assert_eq!(output.try_recv().unwrap().frame, "exact close");

    assert_eq!(
        transport.reopen_lifecycle_route(&route, attempt),
        ReadmitOutcome::Reopened
    );
    drop(output);
    let stopped_claim = OpeningOffer::default();
    assert_eq!(
        transport.enqueue_close(run, attempt, "keyed close"),
        Err(TransportRefusal::WriterStopped)
    );
    assert_eq!(
        transport.offer_lifecycle_close(&route, attempt, &stopped_claim, "exact close"),
        Err(TransportRefusal::WriterStopped)
    );
    assert!(!stopped_claim.was_taken_or_withdraw());
    assert_eq!(transport.closing_marker_for_test(run), None);
}

#[test]
fn a_withdrawn_lifecycle_claim_leaves_no_close_mark_and_releases_its_permit() {
    let (transport, mut output) = transport();
    let run = run();
    let route = LifecycleRoute::new(run);
    let attempt = close_attempt();
    assert_eq!(
        transport.install_lifecycle_route(&route, "session".into()),
        Ok(false)
    );
    let withdrawn = OpeningOffer::default();
    assert!(!withdrawn.was_taken_or_withdraw());

    assert_eq!(
        transport.offer_lifecycle_close(&route, attempt, &withdrawn, "withdrawn close"),
        Err(TransportRefusal::AuthorizationMismatch)
    );
    assert_eq!(transport.closing_marker_for_test(run), None);
    assert!(matches!(output.try_recv(), Err(TryRecvError::Empty)));
    assert_eq!(transport.enqueue_request(run, "next request"), Ok(()));
    assert_eq!(output.try_recv().unwrap().frame, "next request");
}

#[cfg(all(debug_assertions, feature = "test-observation"))]
#[test]
fn a_failed_writer_reservation_skips_both_pauses_and_the_question() {
    use crate::handover_test_control::{HandoverPause, HandoverPausePoint};
    use tokio::sync::oneshot::error::TryRecvError as PauseReadError;

    let (transport, mut output) = transport();
    let run = run();
    transport.open_route(run, "session".into());
    let key = authorization(run, "one offer");
    assert_eq!(transport.offer_work(key.clone(), None, "offered"), Ok(()));
    assert_eq!(transport.enqueue_control(run, "occupied"), Ok(()));
    for point in [
        HandoverPausePoint::BeforeQuestion,
        HandoverPausePoint::BeforePublication,
    ] {
        let HandoverPause {
            mut reached,
            release,
        } = transport.handover_test.arm(point, key.input_id.clone());
        drop(release);
        assert_eq!(
            transport.hand_over_work(&key, || panic!("writer full before question")),
            Err(HandOverRefusal::WriterFull)
        );
        assert_eq!(reached.try_recv(), Err(PauseReadError::Empty), "{point:?}");
    }
    assert_eq!(output.try_recv().unwrap().frame, "occupied");
    drop(output);
    for point in [
        HandoverPausePoint::BeforeQuestion,
        HandoverPausePoint::BeforePublication,
    ] {
        let HandoverPause {
            mut reached,
            release,
        } = transport.handover_test.arm(point, key.input_id.clone());
        drop(release);
        assert_eq!(
            transport.hand_over_work(&key, || panic!("writer stopped before question")),
            Err(HandOverRefusal::WriterStopped)
        );
        assert_eq!(reached.try_recv(), Err(PauseReadError::Empty), "{point:?}");
    }
    assert_eq!(transport.revoke(&key), Ok(DispatchResult::Revoked(key)));
}

#[cfg(all(debug_assertions, feature = "test-observation"))]
#[test]
fn the_question_follows_its_pause_and_only_yes_reaches_publication() {
    use crate::handover_test_control::{HandoverPause, HandoverPausePoint};
    use tokio::sync::oneshot::error::TryRecvError as PauseReadError;

    let (transport, mut output) = transport();
    let run = run();
    transport.open_route(run, "session".into());
    let before_question = authorization(run, "before question");
    assert_eq!(
        transport.offer_work(before_question.clone(), None, "denied"),
        Ok(())
    );
    let HandoverPause {
        mut reached,
        release,
    } = transport.handover_test.arm(
        HandoverPausePoint::BeforeQuestion,
        before_question.input_id.clone(),
    );
    drop(release);
    assert_eq!(
        transport.hand_over_work(&before_question, || {
            assert_eq!(reached.try_recv(), Ok(before_question.clone()));
            false
        }),
        Ok(DispatchResult::DefinitelyUnsent(before_question)),
    );

    let denied = authorization(run, "no publication");
    assert_eq!(transport.offer_work(denied.clone(), None, "denied"), Ok(()));
    let HandoverPause {
        mut reached,
        release,
    } = transport.handover_test.arm(
        HandoverPausePoint::BeforePublication,
        denied.input_id.clone(),
    );
    drop(release);
    assert_eq!(
        transport.hand_over_work(&denied, || false),
        Ok(DispatchResult::DefinitelyUnsent(denied))
    );
    assert_eq!(reached.try_recv(), Err(PauseReadError::Empty));
    assert!(matches!(output.try_recv(), Err(TryRecvError::Empty)));

    let approved = authorization(run, "publication");
    assert_eq!(
        transport.offer_work(approved.clone(), None, "approved"),
        Ok(())
    );
    let HandoverPause {
        mut reached,
        release,
    } = transport.handover_test.arm(
        HandoverPausePoint::BeforePublication,
        approved.input_id.clone(),
    );
    drop(release);
    assert_eq!(
        transport.hand_over_work(&approved, || {
            assert_eq!(reached.try_recv(), Err(PauseReadError::Empty));
            assert!(matches!(output.try_recv(), Err(TryRecvError::Empty)));
            true
        }),
        Ok(DispatchResult::Taken(approved.clone())),
    );
    assert_eq!(reached.try_recv(), Ok(approved));
    assert_eq!(output.try_recv().unwrap().frame, "approved");
}
