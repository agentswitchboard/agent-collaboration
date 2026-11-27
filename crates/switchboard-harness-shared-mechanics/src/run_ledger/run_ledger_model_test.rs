use hegel::generators as gs;

use super::test_fixtures::{authorization, close_attempt, test_input, test_run, test_turn};
use super::*;

#[derive(Clone, Copy, Eq, PartialEq)]
enum TurnOrigin {
    Proposed,
    Taken,
    Provider,
}

struct ModelRun {
    key: TransportRun,
    session: String,
    close: Option<CloseAttempt>,
    live: bool,
    turn: Option<(TurnId, TurnOrigin)>,
}

impl ModelRun {
    fn loading(key: TransportRun, session: String) -> Self {
        Self {
            key,
            session,
            close: None,
            live: false,
            turn: None,
        }
    }

    fn eligible(&self) -> Option<TurnId> {
        self.turn.and_then(|(turn, origin)| match origin {
            TurnOrigin::Proposed => None,
            TurnOrigin::Taken | TurnOrigin::Provider => Some(turn),
        })
    }

    fn membership(&self) -> TurnMembershipState {
        match (self.live, self.turn) {
            (false, _) => TurnMembershipState::Loading,
            (true, None) => TurnMembershipState::Idle,
            (true, Some((turn, TurnOrigin::Proposed))) => TurnMembershipState::Provisional { turn },
            (true, Some((turn, TurnOrigin::Taken))) => TurnMembershipState::Eligible { turn },
            (true, Some((turn, TurnOrigin::Provider))) => {
                TurnMembershipState::ProviderStarted { turn }
            }
        }
    }
}

struct ModelOffer {
    key: DispatchAuthorization,
    turn: Option<TurnId>,
    frame: u16,
    evidence: SlotEvidence,
}

#[derive(Clone, Copy, Debug)]
enum HandoverChoice {
    Drop,
    Commit(bool),
}

#[derive(Debug, Eq, PartialEq)]
enum HandoverAnswer {
    Missing,
    Ready,
    Settled(DispatchResult),
    Committed(HandOverCommit<u16>),
}

#[derive(Default)]
struct Model {
    runs: Vec<ModelRun>,
    offers: Vec<ModelOffer>,
}

impl Model {
    fn run(&self, key: TransportRun) -> Option<&ModelRun> {
        self.runs.iter().find(|row| row.key == key)
    }

    fn run_mut(&mut self, key: TransportRun) -> Option<&mut ModelRun> {
        self.runs.iter_mut().find(|row| row.key == key)
    }

    fn retire(&mut self, key: TransportRun) -> Option<RetiredRun> {
        let position = self.runs.iter().position(|row| row.key == key)?;
        let removed = self.runs.remove(position);
        Some(RetiredRun {
            run: key,
            session: removed.session,
        })
    }

    fn membership(&mut self, run: TransportRun, change: TurnMembership) -> MembershipOutcome {
        let Some(record) = self.run_mut(run) else {
            return MembershipOutcome::NoSuchRun;
        };
        match change {
            TurnMembership::Provisional { turn, .. }
            | TurnMembership::ObservedEligible { turn, .. } => {
                if !record.live {
                    return MembershipOutcome::Ignored(TurnMembershipRefusal::NotLive);
                }
                if record.turn.is_some() {
                    return MembershipOutcome::Ignored(TurnMembershipRefusal::TurnAlreadySet);
                }
                let origin = if matches!(change, TurnMembership::Provisional { .. }) {
                    TurnOrigin::Proposed
                } else {
                    TurnOrigin::Provider
                };
                record.turn = Some((turn, origin));
                MembershipOutcome::Applied
            }
            TurnMembership::RemoveOnRevoke { turn, .. }
                if record.turn == Some((turn, TurnOrigin::Proposed)) =>
            {
                record.turn = None;
                MembershipOutcome::Applied
            }
            TurnMembership::End { turn, .. } if record.eligible() == Some(turn) => {
                record.turn = None;
                MembershipOutcome::Ended {
                    run,
                    turn,
                    session: record.session.clone(),
                }
            }
            TurnMembership::End { .. } | TurnMembership::RemoveOnRevoke { .. } => {
                MembershipOutcome::Ignored(TurnMembershipRefusal::NotThatTurn)
            }
            TurnMembership::Retire(_) => self
                .retire(run)
                .map_or(MembershipOutcome::NoSuchRun, MembershipOutcome::Retired),
        }
    }

    fn can_write(&self, run: TransportRun) -> Result<(), RouteRefusal> {
        match self.run(run) {
            None => Err(RouteRefusal::RouteUnavailable),
            Some(record) if record.close.is_some() => Err(RouteRefusal::Closing),
            Some(_) => Ok(()),
        }
    }

    fn offer(
        &mut self,
        run: TransportRun,
        key: &DispatchAuthorization,
        turn: Option<TurnId>,
        frame: u16,
    ) -> Result<(), OfferRefusal> {
        self.can_write(run).map_err(|reason| match reason {
            RouteRefusal::RouteUnavailable => OfferRefusal::RouteUnavailable,
            RouteRefusal::Closing => OfferRefusal::Closing,
        })?;
        if matches!(
            (key.target, turn),
            (AuthorizationTarget::NewTurn, None)
                | (
                    AuthorizationTarget::Session | AuthorizationTarget::ExistingTurn { .. },
                    Some(_)
                )
        ) {
            return Err(OfferRefusal::WorkDoesNotFitAuthorization);
        }
        let related = |candidate: &&ModelOffer| {
            candidate.key.incarnation == key.incarnation
                && candidate.key.run == key.run
                && candidate.key.input_id == key.input_id
        };
        if self
            .offers
            .iter()
            .filter(related)
            .any(|candidate| candidate.key.authorization_seq == key.authorization_seq)
        {
            return Err(OfferRefusal::AuthorizationSequenceOnRecord);
        }
        if self
            .offers
            .iter()
            .filter(related)
            .any(|candidate| candidate.evidence == SlotEvidence::Offered)
        {
            return Err(OfferRefusal::OtherAuthorizationOffered);
        }
        self.offers.push(ModelOffer {
            key: key.clone(),
            turn,
            frame,
            evidence: SlotEvidence::Offered,
        });
        Ok(())
    }

    fn revoke(&mut self, key: &DispatchAuthorization) -> Option<DispatchResult> {
        let offer = self.offers.iter_mut().find(|row| &row.key == key)?;
        match offer.evidence {
            SlotEvidence::Taken => return Some(DispatchResult::AlreadyTaken(key.clone())),
            SlotEvidence::Revoked => return Some(DispatchResult::Revoked(key.clone())),
            SlotEvidence::Offered => offer.evidence = SlotEvidence::Revoked,
        }
        let proposed = offer.turn.map(|turn| (turn, TurnOrigin::Proposed));
        let run = TransportRun {
            incarnation: key.incarnation,
            generation: key.run,
        };
        if proposed.is_some()
            && let Some(record) = self.run_mut(run)
            && record.turn == proposed
        {
            record.turn = None;
        }
        Some(DispatchResult::Revoked(key.clone()))
    }

    fn owner_slots(
        &mut self,
        owner: OwnerIncarnation,
    ) -> Vec<(DispatchAuthorization, SlotEvidence)> {
        let mut keys: Vec<_> = self
            .offers
            .iter()
            .filter(|row| row.key.incarnation == owner)
            .map(|row| row.key.clone())
            .collect();
        keys.sort();
        keys.into_iter()
            .map(|key| {
                let evidence = match self.revoke(&key) {
                    Some(DispatchResult::AlreadyTaken(_)) => SlotEvidence::Taken,
                    Some(DispatchResult::Revoked(_)) => SlotEvidence::Revoked,
                    other => panic!("known slot has unexpected revoke answer: {other:?}"),
                };
                (key, evidence)
            })
            .collect()
    }

    fn give_up(&mut self, owner: OwnerIncarnation, asked: &BTreeSet<InputId>) -> OwnerEvidence {
        let slots = self.owner_slots(owner);
        asked
            .iter()
            .map(|input| {
                let evidence: Vec<_> = slots
                    .iter()
                    .filter(|(key, _)| &key.input_id == input)
                    .cloned()
                    .collect();
                let answer = if evidence.is_empty() {
                    OwnerInputEvidence::NeverOffered
                } else if evidence
                    .iter()
                    .any(|(_, slot)| *slot == SlotEvidence::Taken)
                {
                    OwnerInputEvidence::Taken { slots: evidence }
                } else {
                    OwnerInputEvidence::Withdrawn { slots: evidence }
                };
                (input.clone(), answer)
            })
            .collect()
    }

    fn hand_over(
        &mut self,
        run: TransportRun,
        key: &DispatchAuthorization,
        choice: HandoverChoice,
    ) -> HandoverAnswer {
        let Some(offer) = self.offers.iter().find(|row| &row.key == key) else {
            return HandoverAnswer::Missing;
        };
        match offer.evidence {
            SlotEvidence::Taken => {
                return HandoverAnswer::Settled(DispatchResult::AlreadyTaken(key.clone()));
            }
            SlotEvidence::Revoked => {
                return HandoverAnswer::Settled(DispatchResult::DefinitelyUnsent(key.clone()));
            }
            SlotEvidence::Offered => {}
        }
        let pending_turn = offer.turn;
        let frame = offer.frame;
        let Some(record) = self.run(run).filter(|row| row.close.is_none()) else {
            self.revoke(key);
            return HandoverAnswer::Settled(DispatchResult::DefinitelyUnsent(key.clone()));
        };
        let correct_target = match key.target {
            AuthorizationTarget::Session => true,
            AuthorizationTarget::NewTurn => {
                record.turn == pending_turn.map(|turn| (turn, TurnOrigin::Proposed))
            }
            AuthorizationTarget::ExistingTurn { turn } => record.eligible() == Some(turn),
        };
        if !correct_target {
            self.revoke(key);
            return HandoverAnswer::Settled(DispatchResult::StaleTarget(key.clone()));
        }
        match choice {
            HandoverChoice::Drop => HandoverAnswer::Ready,
            HandoverChoice::Commit(false) => {
                self.revoke(key);
                HandoverAnswer::Committed(HandOverCommit::Unsent(DispatchResult::DefinitelyUnsent(
                    key.clone(),
                )))
            }
            HandoverChoice::Commit(true) => {
                self.offers
                    .iter_mut()
                    .find(|row| &row.key == key)
                    .unwrap()
                    .evidence = SlotEvidence::Taken;
                if let Some(turn) = pending_turn {
                    self.run_mut(run).unwrap().turn = Some((turn, TurnOrigin::Taken));
                }
                HandoverAnswer::Committed(HandOverCommit::Send { run, frame })
            }
        }
    }
}

fn actual_hand_over(
    ledger: &mut RunLedger<u16>,
    key: &DispatchAuthorization,
    choice: HandoverChoice,
) -> HandoverAnswer {
    match ledger.check_hand_over(key) {
        Err(NoSuchOffer) => HandoverAnswer::Missing,
        Ok(HandOverCheck::Settled(answer)) => HandoverAnswer::Settled(answer),
        Ok(HandOverCheck::Ready(ready)) => match choice {
            HandoverChoice::Drop => HandoverAnswer::Ready,
            HandoverChoice::Commit(answer) => HandoverAnswer::Committed(ready.commit(answer)),
        },
    }
}

fn compare_reads(ledger: &RunLedger<u16>, model: &Model, scope: &[TransportRun; 4]) {
    for &run in scope {
        let expected = model.run(run);
        assert_eq!(
            ledger.session_of(run),
            expected.map(|row| row.session.as_str())
        );
        assert_eq!(ledger.membership(run), expected.map(ModelRun::membership));
        assert_eq!(
            ledger.current_turn(run),
            expected.and_then(ModelRun::eligible)
        );
        assert_eq!(
            ledger.provider_started_turn(run),
            expected.and_then(|row| match row.turn {
                Some((turn, TurnOrigin::Provider)) => Some(turn),
                Some((_, TurnOrigin::Proposed | TurnOrigin::Taken)) | None => None,
            })
        );
        assert_eq!(
            ledger.closing_marker(run),
            expected.and_then(|row| row.close)
        );
        assert_eq!(ledger.check_takes_requests(run), model.can_write(run));
        let generations: Vec<_> = scope
            .iter()
            .filter(|key| key.incarnation == run.incarnation && model.run(**key).is_some())
            .map(|key| key.generation)
            .collect();
        assert_eq!(
            ledger.runs_of(run.incarnation).collect::<Vec<_>>(),
            generations
        );
    }
    let ordered: Vec<_> = scope.iter().filter_map(|run| model.run(*run)).collect();
    assert!(ledger.run_keys().eq(ordered.iter().map(|row| row.key)));
    assert!(
        ledger.owner_sessions().eq(ordered
            .iter()
            .map(|row| (row.key.incarnation, row.session.as_str())))
    );
    for name in ["alpha", "beta"] {
        assert!(
            ledger.runs_named(name).eq(ordered
                .iter()
                .filter(|row| row.session == name)
                .map(|row| row.key))
        );
    }
    let eligible: Vec<_> = ordered
        .iter()
        .filter_map(|row| row.eligible().map(|turn| (row.key, turn)))
        .collect();
    let sole = match eligible.as_slice() {
        [only] => Some(*only),
        _ => None,
    };
    assert_eq!(ledger.sole_eligible_turn(), sole);
    let slots: BTreeMap<_, _> = model
        .offers
        .iter()
        .map(|row| {
            (
                &row.key,
                (
                    row.evidence,
                    (row.evidence == SlotEvidence::Offered).then_some((row.frame, row.turn)),
                ),
            )
        })
        .collect();
    let actual: BTreeMap<_, _> = ledger
        .offers
        .iter()
        .map(|(key, row)| {
            (
                key,
                (
                    row.evidence(),
                    match row {
                        OfferRecord::Offered { frame, new_turn } => Some((*frame, *new_turn)),
                        OfferRecord::Taken | OfferRecord::Revoked => None,
                    },
                ),
            )
        })
        .collect();
    assert_eq!(actual, slots);
}

#[hegel::test(test_cases = 200)]
fn generated_runs_and_writer_offers_match_a_separate_custody_model(case: hegel::TestCase) {
    let scope = [
        test_run(1, 1),
        test_run(1, 2),
        test_run(2, 1),
        test_run(2, 2),
    ];
    let mut ledger = RunLedger::default();
    let mut model = Model::default();
    for frame in 0..case.draw(gs::integers::<u16>().min_value(1).max_value(100)) {
        let run = scope[case.draw(gs::integers::<usize>().max_value(3))];
        let input = case.draw(gs::integers::<u8>().max_value(2));
        let sequence = u64::from(case.draw(gs::integers::<u8>().min_value(1).max_value(3)));
        let turn = test_turn(case.draw(gs::integers::<u8>().min_value(1).max_value(2)));
        let attempt = close_attempt(case.draw(gs::integers::<u8>().min_value(1).max_value(2)));
        let target = match case.draw(gs::integers::<u8>().max_value(2)) {
            0 => AuthorizationTarget::Session,
            1 => AuthorizationTarget::NewTurn,
            _ => AuthorizationTarget::ExistingTurn { turn },
        };
        let key = authorization(run, input, sequence, target);
        match case.draw(gs::integers::<u8>().max_value(18)) {
            0 | 1 => {
                let name = if frame % 2 == 0 { "alpha" } else { "beta" };
                let expected = if model.run(run).is_some() {
                    RunOpening::AlreadyOpen
                } else {
                    model.runs.push(ModelRun::loading(run, name.into()));
                    RunOpening::Opened
                };
                assert_eq!(ledger.open_run(run, name.into()), expected);
            }
            2 => {
                let previous = model.retire(run);
                model.runs.push(ModelRun::loading(run, "beta".into()));
                assert_eq!(ledger.replace_run(run, "beta".into()), previous);
            }
            3 => {
                let expected = model
                    .run_mut(run)
                    .filter(|row| row.close.is_none())
                    .map(|row| row.live = true);
                assert_eq!(ledger.activate_run(run), expected.is_some());
            }
            step @ 4..=8 => {
                let generation = run.generation;
                let event = match step {
                    4 => TurnMembership::Provisional {
                        run: generation,
                        turn,
                    },
                    5 => TurnMembership::ObservedEligible {
                        run: generation,
                        turn,
                    },
                    6 => TurnMembership::RemoveOnRevoke {
                        run: generation,
                        turn,
                    },
                    7 => TurnMembership::End {
                        run: generation,
                        turn,
                    },
                    _ => TurnMembership::Retire(generation),
                };
                assert_eq!(
                    ledger.apply_membership(run.incarnation, event),
                    model.membership(run, event)
                );
            }
            9 | 10 => {
                let include_turn = case.draw(gs::booleans());
                let new_turn = include_turn.then_some(turn);
                let expected = model.offer(run, &key, new_turn, frame);
                assert_eq!(ledger.offer(key, new_turn, frame), expected);
            }
            step @ 11..=13 => {
                let choice = if step == 11 {
                    HandoverChoice::Drop
                } else {
                    HandoverChoice::Commit(step == 12)
                };
                assert_eq!(
                    actual_hand_over(&mut ledger, &key, choice),
                    model.hand_over(run, &key, choice)
                );
            }
            14 => assert_eq!(ledger.revoke(&key), model.revoke(&key)),
            15 => {
                if case.draw(gs::booleans()) {
                    assert_eq!(
                        ledger.revoke_incarnation(run.incarnation),
                        model.owner_slots(run.incarnation)
                    );
                } else {
                    let asked: BTreeSet<_> = (0..3)
                        .filter(|_| case.draw(gs::booleans()))
                        .map(test_input)
                        .collect();
                    assert_eq!(
                        ledger.give_up_owner(run.incarnation, &asked),
                        model.give_up(run.incarnation, &asked)
                    );
                }
            }
            16 => {
                let mark = case.draw(gs::booleans());
                let expected = model.can_write(run);
                let observed = ledger.begin_close(run).map(|ready| {
                    if mark {
                        ready.mark(attempt);
                    }
                });
                assert_eq!(observed, expected);
                if expected.is_ok() && mark {
                    model.run_mut(run).unwrap().close = Some(attempt);
                }
            }
            17 => {
                let expected = match model.run_mut(run) {
                    None => ReadmitOutcome::RouteLost,
                    Some(record) => match record.close {
                        None => ReadmitOutcome::MarkerAbsent,
                        Some(mark) if mark == attempt => {
                            record.close = None;
                            ReadmitOutcome::Reopened
                        }
                        Some(_) => ReadmitOutcome::DifferentMarker,
                    },
                };
                assert_eq!(ledger.readmit(run, attempt), expected);
            }
            _ => {
                if case.draw(gs::booleans()) {
                    assert_eq!(ledger.retire_run(run), model.retire(run));
                } else {
                    ledger.forget(&key);
                    model.offers.retain(|offer| offer.key != key);
                }
            }
        }
        compare_reads(&ledger, &model, &scope);
    }
}
