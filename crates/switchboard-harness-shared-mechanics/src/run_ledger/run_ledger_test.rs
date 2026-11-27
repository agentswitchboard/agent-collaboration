use super::test_fixtures::{authorization, close_attempt, test_input, test_run, test_turn};
use super::*;

fn ledger_with_run(run: TransportRun) -> RunLedger<u16> {
    let mut ledger = RunLedger::default();
    assert_eq!(
        ledger.open_run(run, "session".to_owned()),
        RunOpening::Opened
    );
    assert!(ledger.activate_run(run));
    ledger
}

fn slot(ledger: &RunLedger<u16>, key: &DispatchAuthorization) -> Option<SlotEvidence> {
    ledger.offers.get(key).map(OfferRecord::evidence)
}

fn ready<'ledger>(
    ledger: &'ledger mut RunLedger<u16>,
    key: &DispatchAuthorization,
) -> ReadyHandOver<'ledger, u16> {
    match ledger.check_hand_over(key) {
        Ok(HandOverCheck::Ready(ready)) => ready,
        Ok(HandOverCheck::Settled(result)) => panic!("expected Ready, settled as {result:?}"),
        Err(NoSuchOffer) => panic!("expected Ready, no offer present"),
    }
}

fn assert_settled(
    ledger: &mut RunLedger<u16>,
    key: &DispatchAuthorization,
    expected: DispatchResult,
) {
    match ledger.check_hand_over(key) {
        Ok(HandOverCheck::Settled(result)) => assert_eq!(result, expected),
        Ok(HandOverCheck::Ready(_)) => panic!("expected {expected:?}, still Ready"),
        Err(NoSuchOffer) => panic!("expected {expected:?}, no offer present"),
    }
}

fn begin(ledger: &mut RunLedger<u16>, run: TransportRun, turn: TurnId) {
    assert_eq!(
        ledger.apply_membership(
            run.incarnation,
            TurnMembership::Provisional {
                run: run.generation,
                turn
            }
        ),
        MembershipOutcome::Applied
    );
}

fn remove_provisional(ledger: &mut RunLedger<u16>, run: TransportRun, turn: TurnId) {
    assert_eq!(
        ledger.apply_membership(
            run.incarnation,
            TurnMembership::RemoveOnRevoke {
                run: run.generation,
                turn
            }
        ),
        MembershipOutcome::Applied
    );
}

fn offered_session(
    ledger: &mut RunLedger<u16>,
    run: TransportRun,
    input: u8,
    frame: u16,
) -> DispatchAuthorization {
    let key = authorization(run, input, 1, AuthorizationTarget::Session);
    assert_eq!(ledger.offer(key.clone(), None, frame), Ok(()));
    key
}

#[test]
fn commit_sends_one_original_frame_until_the_slot_is_forgotten() {
    let run = test_run(1, 1);
    let mut ledger = ledger_with_run(run);
    let key = offered_session(&mut ledger, run, 1, 19);

    assert_eq!(
        ready(&mut ledger, &key).commit(true),
        HandOverCommit::Send { run, frame: 19 }
    );
    assert_settled(&mut ledger, &key, DispatchResult::AlreadyTaken(key.clone()));
    assert_eq!(slot(&ledger, &key), Some(SlotEvidence::Taken));

    ledger.forget(&key);
    assert_eq!(ledger.offer(key.clone(), None, 29), Ok(()));
    assert_eq!(
        ready(&mut ledger, &key).commit(true),
        HandOverCommit::Send { run, frame: 29 }
    );
}

#[test]
fn dropping_ready_keeps_the_exact_offer_and_provisional_turn() {
    let run = test_run(1, 1);
    let mut ledger = ledger_with_run(run);
    let turn = test_turn(1);
    let key = offered_new_turn_for(&mut ledger, run, 1, turn);

    assert!(matches!(
        ledger.check_hand_over(&key),
        Ok(HandOverCheck::Ready(_))
    ));

    assert_eq!(slot(&ledger, &key), Some(SlotEvidence::Offered));
    assert_eq!(
        ledger.membership(run),
        Some(TurnMembershipState::Provisional { turn })
    );
    assert_eq!(
        ready(&mut ledger, &key).commit(true),
        HandOverCommit::Send { run, frame: 10 }
    );
    assert_eq!(
        ledger.membership(run),
        Some(TurnMembershipState::Eligible { turn })
    );
}

#[test]
fn a_negative_last_answer_revokes_the_offer_and_clears_its_provisional_turn() {
    let run = test_run(1, 1);
    let mut ledger = ledger_with_run(run);
    let key = offered_new_turn_for(&mut ledger, run, 1, test_turn(1));

    assert_eq!(
        ready(&mut ledger, &key).commit(false),
        HandOverCommit::Unsent(DispatchResult::DefinitelyUnsent(key.clone())),
    );
    assert_eq!(slot(&ledger, &key), Some(SlotEvidence::Revoked));
    assert_eq!(ledger.membership(run), Some(TurnMembershipState::Idle));
    assert_settled(
        &mut ledger,
        &key,
        DispatchResult::DefinitelyUnsent(key.clone()),
    );
}

#[test]
fn a_close_mark_revokes_an_offer_before_hand_over() {
    let run = test_run(1, 1);
    let mut ledger = ledger_with_run(run);
    let key = offered_session(&mut ledger, run, 1, 10);
    let new_turn = offered_new_turn_for(&mut ledger, run, 2, test_turn(1));
    let attempt = close_attempt(1);
    ledger.begin_close(run).unwrap().mark(attempt);

    assert_settled(
        &mut ledger,
        &key,
        DispatchResult::DefinitelyUnsent(key.clone()),
    );
    assert_eq!(slot(&ledger, &key), Some(SlotEvidence::Revoked));
    assert_settled(
        &mut ledger,
        &new_turn,
        DispatchResult::DefinitelyUnsent(new_turn.clone()),
    );
    assert_eq!(slot(&ledger, &new_turn), Some(SlotEvidence::Revoked));
    assert_eq!(ledger.membership(run), Some(TurnMembershipState::Idle));
    assert_eq!(ledger.closing_marker(run), Some(attempt));
}

fn offered_new_turn_for(
    ledger: &mut RunLedger<u16>,
    run: TransportRun,
    input: u8,
    turn: TurnId,
) -> DispatchAuthorization {
    let key = authorization(run, input, 1, AuthorizationTarget::NewTurn);
    begin(ledger, run, turn);
    assert_eq!(ledger.offer(key.clone(), Some(turn), 10), Ok(()));
    key
}

#[test]
fn a_gone_run_revokes_an_offer_but_a_missing_offer_stays_missing() {
    let run = test_run(1, 1);
    let mut ledger = ledger_with_run(run);
    let key = offered_session(&mut ledger, run, 1, 10);
    let missing = authorization(run, 2, 1, AuthorizationTarget::Session);
    assert_eq!(
        ledger.retire_run(run),
        Some(RetiredRun {
            run,
            session: "session".into()
        })
    );

    assert_settled(
        &mut ledger,
        &key,
        DispatchResult::DefinitelyUnsent(key.clone()),
    );
    assert_eq!(slot(&ledger, &key), Some(SlotEvidence::Revoked));
    assert!(matches!(ledger.check_hand_over(&missing), Err(NoSuchOffer)));
    assert_eq!(ledger.membership(run), None);
}

#[test]
fn an_old_existing_turn_target_is_revoked_without_clearing_its_successor() {
    let run = test_run(1, 1);
    let mut ledger = ledger_with_run(run);
    let (first, next) = (test_turn(1), test_turn(2));
    assert_eq!(
        ledger.apply_membership(
            run.incarnation,
            TurnMembership::ObservedEligible {
                run: run.generation,
                turn: first
            }
        ),
        MembershipOutcome::Applied
    );
    let old = authorization(run, 1, 1, AuthorizationTarget::ExistingTurn { turn: first });
    assert_eq!(ledger.offer(old.clone(), None, 10), Ok(()));
    assert_eq!(
        ledger.apply_membership(
            run.incarnation,
            TurnMembership::End {
                run: run.generation,
                turn: first
            }
        ),
        MembershipOutcome::Ended {
            run,
            turn: first,
            session: "session".into()
        }
    );
    assert_eq!(
        ledger.apply_membership(
            run.incarnation,
            TurnMembership::ObservedEligible {
                run: run.generation,
                turn: next
            }
        ),
        MembershipOutcome::Applied
    );

    assert_settled(&mut ledger, &old, DispatchResult::StaleTarget(old.clone()));
    assert_eq!(slot(&ledger, &old), Some(SlotEvidence::Revoked));
    assert_eq!(
        ledger.membership(run),
        Some(TurnMembershipState::ProviderStarted { turn: next })
    );
}

#[test]
fn a_changed_provisional_turn_revokes_the_old_offer_and_leaves_the_new_one() {
    let run = test_run(1, 1);
    let mut ledger = ledger_with_run(run);
    let (first, next) = (test_turn(1), test_turn(2));
    let old = offered_new_turn_for(&mut ledger, run, 1, first);
    remove_provisional(&mut ledger, run, first);
    begin(&mut ledger, run, next);

    assert_settled(&mut ledger, &old, DispatchResult::StaleTarget(old.clone()));
    assert_eq!(slot(&ledger, &old), Some(SlotEvidence::Revoked));
    assert_eq!(
        ledger.membership(run),
        Some(TurnMembershipState::Provisional { turn: next })
    );
}

#[test]
fn revoking_taken_work_preserves_its_evidence_after_retire_and_replacement() {
    let run = test_run(1, 1);
    let mut ledger = ledger_with_run(run);
    let key = offered_session(&mut ledger, run, 1, 10);
    assert_eq!(
        ready(&mut ledger, &key).commit(true),
        HandOverCommit::Send { run, frame: 10 }
    );

    assert_eq!(
        ledger.revoke(&key),
        Some(DispatchResult::AlreadyTaken(key.clone()))
    );
    assert_eq!(slot(&ledger, &key), Some(SlotEvidence::Taken));
    ledger.retire_run(run);
    assert_eq!(
        ledger.revoke(&key),
        Some(DispatchResult::AlreadyTaken(key.clone()))
    );
    assert_settled(&mut ledger, &key, DispatchResult::AlreadyTaken(key.clone()));
    assert_eq!(ledger.replace_run(run, "new".into()), None);
    assert_eq!(
        ledger.revoke(&key),
        Some(DispatchResult::AlreadyTaken(key.clone()))
    );
    assert_eq!(
        ledger.revoke_incarnation(run.incarnation),
        [(key, SlotEvidence::Taken)]
    );
}

#[test]
fn give_up_answers_taken_for_a_taken_input_after_the_run_is_gone() {
    let run = test_run(1, 1);
    let mut ledger = ledger_with_run(run);
    let key = offered_session(&mut ledger, run, 1, 10);
    assert_eq!(
        ready(&mut ledger, &key).commit(true),
        HandOverCommit::Send { run, frame: 10 }
    );
    ledger.retire_run(run);

    assert_eq!(
        ledger.give_up_owner(run.incarnation, &BTreeSet::from([test_input(1)])),
        BTreeMap::from([(
            test_input(1),
            OwnerInputEvidence::Taken {
                slots: vec![(key.clone(), SlotEvidence::Taken)]
            }
        )]),
    );
    assert_settled(&mut ledger, &key, DispatchResult::AlreadyTaken(key.clone()));
}

#[test]
fn give_up_answers_only_asked_inputs_and_keeps_full_ordered_custody() {
    let first = test_run(1, 1);
    let second = test_run(1, 2);
    let other = test_run(2, 1);
    let mut ledger = ledger_with_run(first);
    assert_eq!(ledger.open_run(second, "second".into()), RunOpening::Opened);
    assert_eq!(ledger.open_run(other, "other".into()), RunOpening::Opened);
    let taken = offered_session(&mut ledger, first, 1, 10);
    assert_eq!(
        ready(&mut ledger, &taken).commit(true),
        HandOverCommit::Send {
            run: first,
            frame: 10
        }
    );
    let offered = offered_session(&mut ledger, second, 1, 20);
    assert!(ledger.activate_run(second));
    let provisional = test_turn(2);
    let unasked = offered_new_turn_for(&mut ledger, second, 2, provisional);
    let disjoint = offered_session(&mut ledger, other, 1, 40);

    assert_eq!(
        ledger.give_up_owner(
            first.incarnation,
            &BTreeSet::from([test_input(1), test_input(3)])
        ),
        BTreeMap::from([
            (
                test_input(1),
                OwnerInputEvidence::Taken {
                    slots: vec![
                        (taken, SlotEvidence::Taken),
                        (offered, SlotEvidence::Revoked)
                    ],
                }
            ),
            (test_input(3), OwnerInputEvidence::NeverOffered),
        ]),
    );
    assert_eq!(slot(&ledger, &unasked), Some(SlotEvidence::Revoked));
    assert_eq!(ledger.membership(second), Some(TurnMembershipState::Idle));
    assert_eq!(slot(&ledger, &disjoint), Some(SlotEvidence::Offered));
    assert_eq!(ledger.session_of(first), Some("session"));
}

#[test]
fn retirement_leaves_an_offered_slot_available_to_revoke() {
    let run = test_run(1, 1);
    let mut ledger = ledger_with_run(run);
    let offered = offered_session(&mut ledger, run, 1, 10);

    assert_eq!(
        ledger.apply_membership(run.incarnation, TurnMembership::Retire(run.generation)),
        MembershipOutcome::Retired(RetiredRun {
            run,
            session: "session".into()
        }),
    );
    assert_eq!(slot(&ledger, &offered), Some(SlotEvidence::Offered));
    assert_eq!(
        ledger.revoke(&offered),
        Some(DispatchResult::Revoked(offered.clone()))
    );
    assert_eq!(ledger.session_of(run), None);
}

#[test]
fn replacing_a_run_resets_its_record_but_keeps_all_three_slot_states() {
    let run = test_run(1, 1);
    let mut ledger = ledger_with_run(run);
    let offered = offered_session(&mut ledger, run, 1, 10);
    let taken = offered_session(&mut ledger, run, 2, 20);
    let revoked = offered_session(&mut ledger, run, 3, 30);
    assert_eq!(
        ready(&mut ledger, &taken).commit(true),
        HandOverCommit::Send { run, frame: 20 }
    );
    assert_eq!(
        ledger.revoke(&revoked),
        Some(DispatchResult::Revoked(revoked.clone()))
    );
    ledger.begin_close(run).unwrap().mark(close_attempt(1));

    assert_eq!(
        ledger.replace_run(run, "replacement".into()),
        Some(RetiredRun {
            run,
            session: "session".into()
        })
    );
    assert_eq!(ledger.session_of(run), Some("replacement"));
    assert_eq!(ledger.membership(run), Some(TurnMembershipState::Loading));
    assert_eq!(ledger.closing_marker(run), None);
    for (key, evidence) in [
        (offered, SlotEvidence::Offered),
        (taken, SlotEvidence::Taken),
        (revoked, SlotEvidence::Revoked),
    ] {
        assert_eq!(slot(&ledger, &key), Some(evidence));
        assert_eq!(
            ledger.offer(key, None, 40),
            Err(RefusedOffer {
                refusal: OfferRefusal::AuthorizationSequenceOnRecord,
                frame: 40
            })
        );
    }
}

#[test]
fn a_marked_run_refuses_an_offer_before_creating_a_slot() {
    let run = test_run(1, 1);
    let mut ledger = ledger_with_run(run);
    let key = authorization(run, 1, 1, AuthorizationTarget::Session);
    ledger.begin_close(run).unwrap().mark(close_attempt(1));

    assert_eq!(
        ledger.offer(key.clone(), None, 10),
        Err(RefusedOffer {
            refusal: OfferRefusal::Closing,
            frame: 10
        })
    );
    assert_eq!(slot(&ledger, &key), None);
}

#[test]
fn a_recorded_sequence_wins_over_a_standing_offer_and_is_free_after_forget() {
    let run = test_run(1, 1);
    let mut ledger = ledger_with_run(run);
    let first = offered_session(&mut ledger, run, 1, 10);
    assert_eq!(
        ready(&mut ledger, &first).commit(true),
        HandOverCommit::Send { run, frame: 10 }
    );
    let collision = authorization(
        run,
        1,
        1,
        AuthorizationTarget::ExistingTurn { turn: test_turn(2) },
    );
    assert_eq!(
        ledger.offer(collision.clone(), None, 33),
        Err(RefusedOffer {
            refusal: OfferRefusal::AuthorizationSequenceOnRecord,
            frame: 33
        })
    );
    let second = authorization(run, 1, 2, AuthorizationTarget::Session);
    assert_eq!(ledger.offer(second.clone(), None, 20), Ok(()));

    assert_eq!(
        ledger.offer(collision.clone(), None, 33),
        Err(RefusedOffer {
            refusal: OfferRefusal::AuthorizationSequenceOnRecord,
            frame: 33
        })
    );
    assert_eq!(slot(&ledger, &second), Some(SlotEvidence::Offered));
    ledger.forget(&second);
    ledger.forget(&first);
    assert_eq!(ledger.offer(collision.clone(), None, 33), Ok(()));
    assert_eq!(slot(&ledger, &collision), Some(SlotEvidence::Offered));
}

#[test]
fn another_sequence_cannot_stand_for_the_same_run_and_input() {
    let run = test_run(1, 1);
    let other = test_run(1, 2);
    let mut ledger = ledger_with_run(run);
    ledger.open_run(other, "other".into());
    let first = offered_session(&mut ledger, run, 1, 10);
    let second = authorization(run, 1, 2, AuthorizationTarget::Session);

    assert_eq!(
        ledger.offer(second.clone(), None, 20),
        Err(RefusedOffer {
            refusal: OfferRefusal::OtherAuthorizationOffered,
            frame: 20
        })
    );
    assert_eq!(slot(&ledger, &second), None);
    assert_eq!(slot(&ledger, &first), Some(SlotEvidence::Offered));
    offered_session(&mut ledger, run, 2, 30);
    offered_session(&mut ledger, other, 1, 40);
}

#[test]
fn only_the_exact_close_attempt_reopens_the_run() {
    let run = test_run(1, 1);
    let mut ledger = ledger_with_run(run);
    let (first, other) = (close_attempt(1), close_attempt(2));
    assert!(ledger.begin_close(run).is_ok());
    assert_eq!(ledger.closing_marker(run), None);
    ledger.begin_close(run).unwrap().mark(first);

    assert_eq!(ledger.readmit(run, other), ReadmitOutcome::DifferentMarker);
    assert_eq!(ledger.closing_marker(run), Some(first));
    assert_eq!(ledger.readmit(run, first), ReadmitOutcome::Reopened);
    assert_eq!(ledger.closing_marker(run), None);
    assert_eq!(ledger.readmit(run, first), ReadmitOutcome::MarkerAbsent);
    assert_eq!(
        ledger.readmit(test_run(2, 1), first),
        ReadmitOutcome::RouteLost
    );
}

#[test]
fn membership_preserves_eligible_turn_identity_and_origin() {
    let run = test_run(1, 1);
    let mut ledger = ledger_with_run(run);
    let (first, next) = (test_turn(1), test_turn(2));
    let change = TurnMembership::ObservedEligible {
        run: run.generation,
        turn: first,
    };
    assert_eq!(
        ledger.apply_membership(run.incarnation, change),
        MembershipOutcome::Applied
    );
    assert_eq!(ledger.provider_started_turn(run), Some(first));
    assert_eq!(ledger.current_turn(run), Some(first));

    assert_eq!(
        ledger.apply_membership(
            run.incarnation,
            TurnMembership::RemoveOnRevoke {
                run: run.generation,
                turn: first
            }
        ),
        MembershipOutcome::Ignored(TurnMembershipRefusal::NotThatTurn)
    );
    assert_eq!(
        ledger.membership(run),
        Some(TurnMembershipState::ProviderStarted { turn: first })
    );
    assert_eq!(
        ledger.apply_membership(
            run.incarnation,
            TurnMembership::End {
                run: run.generation,
                turn: first
            }
        ),
        MembershipOutcome::Ended {
            run,
            turn: first,
            session: "session".into()
        }
    );
    let key = offered_new_turn_for(&mut ledger, run, 1, next);
    assert_eq!(
        ready(&mut ledger, &key).commit(true),
        HandOverCommit::Send { run, frame: 10 }
    );
    for rejected in [
        TurnMembership::RemoveOnRevoke {
            run: run.generation,
            turn: first,
        },
        TurnMembership::RemoveOnRevoke {
            run: run.generation,
            turn: next,
        },
        TurnMembership::End {
            run: run.generation,
            turn: first,
        },
    ] {
        assert_eq!(
            ledger.apply_membership(run.incarnation, rejected),
            MembershipOutcome::Ignored(TurnMembershipRefusal::NotThatTurn)
        );
        assert_eq!(
            ledger.membership(run),
            Some(TurnMembershipState::Eligible { turn: next })
        );
        assert_eq!(ledger.provider_started_turn(run), None);
    }
}
