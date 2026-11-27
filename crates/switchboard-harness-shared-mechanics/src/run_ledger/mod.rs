//! One harness connection's runs and its ordered writer's work offers.

mod ledger_outcomes;
mod offer_slot_machine;
mod route_close_machine;
mod turn_membership_machine;

use std::collections::{BTreeMap, BTreeSet, btree_map::Entry};

use switchboard_harness_contracts::{
    AuthorizationTarget, CloseAttempt, DispatchAuthorization, DispatchResult, InputId,
    OwnerEvidence, OwnerIncarnation, OwnerInputEvidence, SessionGeneration, SlotEvidence,
    TransportRun, TurnId, TurnMembership,
};

pub use ledger_outcomes::{
    HandOverCommit, MembershipOutcome, NoSuchOffer, OfferRefusal, ReadmitOutcome, RefusedOffer,
    RetiredRun, RouteRefusal, RunOpening,
};
use offer_slot_machine::{OfferSlotEvent, OfferSlotMachine, OfferSlotRefusal, OfferSlotState};
use route_close_machine::{RouteCloseEvent, RouteCloseMachine, RouteCloseRefusal, RouteCloseState};
use turn_membership_machine::{TurnMembershipEvent, TurnMembershipMachine};
pub use turn_membership_machine::{TurnMembershipRefusal, TurnMembershipState};

/// The harness holds this record under its own lock, beside its own writer.
/// Retirement and replacement leave offer history intact until `forget`.
pub struct RunLedger<Frame> {
    runs: BTreeMap<TransportRun, RunRecord>,
    offers: BTreeMap<DispatchAuthorization, OfferRecord<Frame>>,
}

impl<Frame> Default for RunLedger<Frame> {
    fn default() -> Self {
        Self {
            runs: BTreeMap::new(),
            offers: BTreeMap::new(),
        }
    }
}

struct RunRecord {
    session: String,
    close: RouteCloseState,
    membership: TurnMembershipState,
}

impl RunRecord {
    fn loading(session: String) -> Self {
        Self {
            session,
            close: RouteCloseState::Open,
            membership: TurnMembershipState::Loading,
        }
    }
}

enum OfferRecord<Frame> {
    Offered {
        frame: Frame,
        new_turn: Option<TurnId>,
    },
    Taken,
    Revoked,
}

impl<Frame> OfferRecord<Frame> {
    const fn state(&self) -> OfferSlotState {
        match self {
            Self::Offered { .. } => OfferSlotState::Offered,
            Self::Taken => OfferSlotState::Taken,
            Self::Revoked => OfferSlotState::Revoked,
        }
    }

    const fn evidence(&self) -> SlotEvidence {
        match self {
            Self::Offered { .. } => SlotEvidence::Offered,
            Self::Taken => SlotEvidence::Taken,
            Self::Revoked => SlotEvidence::Revoked,
        }
    }

    fn revoke(&mut self) -> Option<TurnId> {
        if OfferSlotMachine::reduce(self.state(), OfferSlotEvent::Revoke)
            != Ok(OfferSlotState::Revoked)
        {
            return None;
        }
        match std::mem::replace(self, Self::Revoked) {
            Self::Offered { frame, new_turn } => {
                drop(frame);
                new_turn
            }
            Self::Taken | Self::Revoked => None,
        }
    }
}

pub enum HandOverCheck<'ledger, Frame> {
    Settled(DispatchResult),
    Ready(ReadyHandOver<'ledger, Frame>),
}

/// A checked offer. The exclusive borrow keeps its slot and turn unchanged
/// while the harness reserves writer room and asks the caller's final question.
pub struct ReadyHandOver<'ledger, Frame> {
    ledger: &'ledger mut RunLedger<Frame>,
    authorization: DispatchAuthorization,
    taken_membership: Option<TurnMembershipState>,
}

impl<Frame> ReadyHandOver<'_, Frame> {
    pub fn commit(self, still_requested: bool) -> HandOverCommit<Frame> {
        let Self {
            ledger,
            authorization,
            taken_membership,
        } = self;
        if !still_requested {
            ledger.revoke(&authorization);
            return HandOverCommit::Unsent(DispatchResult::DefinitelyUnsent(authorization));
        }
        let run = authorization_run(&authorization);
        // The checked slot cannot change while Ready borrows the ledger.
        let Some(slot @ OfferRecord::Offered { .. }) = ledger.offers.get_mut(&authorization) else {
            return HandOverCommit::Unsent(DispatchResult::DefinitelyUnsent(authorization));
        };
        let OfferRecord::Offered { frame, .. } = std::mem::replace(slot, OfferRecord::Taken) else {
            return HandOverCommit::Unsent(DispatchResult::DefinitelyUnsent(authorization));
        };
        if let Some(membership) = taken_membership
            && let Some(record) = ledger.runs.get_mut(&run)
        {
            record.membership = membership;
        }
        HandOverCommit::Send { run, frame }
    }
}

/// An unmarked close. Mark only after the harness sends the close frame.
pub struct ReadyClose<'ledger>(&'ledger mut RouteCloseState);

impl ReadyClose<'_> {
    pub fn mark(self, attempt: CloseAttempt) {
        if let Ok(next) = RouteCloseMachine::reduce(*self.0, RouteCloseEvent::CloseSent { attempt })
        {
            *self.0 = next;
        }
    }
}

impl<Frame> RunLedger<Frame> {
    pub fn open_run(&mut self, run: TransportRun, session: String) -> RunOpening {
        match self.runs.entry(run) {
            Entry::Vacant(entry) => {
                entry.insert(RunRecord::loading(session));
                RunOpening::Opened
            }
            Entry::Occupied(_) => RunOpening::AlreadyOpen,
        }
    }

    pub fn replace_run(&mut self, run: TransportRun, session: String) -> Option<RetiredRun> {
        self.runs
            .insert(run, RunRecord::loading(session))
            .map(|record| RetiredRun {
                run,
                session: record.session,
            })
    }

    pub fn retire_run(&mut self, run: TransportRun) -> Option<RetiredRun> {
        self.runs.remove(&run).map(|record| RetiredRun {
            run,
            session: record.session,
        })
    }

    pub fn activate_run(&mut self, run: TransportRun) -> bool {
        let Some(record) = self
            .runs
            .get_mut(&run)
            .filter(|record| record.close == RouteCloseState::Open)
        else {
            return false;
        };
        match TurnMembershipMachine::reduce(record.membership, TurnMembershipEvent::Activate) {
            Ok(next) => {
                record.membership = next;
                true
            }
            Err(TurnMembershipRefusal::AlreadyLive) => true,
            Err(
                TurnMembershipRefusal::NotLive
                | TurnMembershipRefusal::TurnAlreadySet
                | TurnMembershipRefusal::NotThatTurn,
            ) => false,
        }
    }

    pub fn apply_membership(
        &mut self,
        incarnation: OwnerIncarnation,
        change: TurnMembership,
    ) -> MembershipOutcome {
        use TurnMembershipEvent as Event;

        let (generation, event) = match change {
            TurnMembership::Provisional { run, turn } => (run, Event::Begin { turn }),
            TurnMembership::ObservedEligible { run, turn } => {
                (run, Event::ObserveEligible { turn })
            }
            TurnMembership::RemoveOnRevoke { run, turn } => (run, Event::RemoveOnRevoke { turn }),
            TurnMembership::End { run, turn } => (run, Event::End { turn }),
            TurnMembership::Retire(generation) => {
                return self
                    .retire_run(TransportRun {
                        incarnation,
                        generation,
                    })
                    .map_or(MembershipOutcome::NoSuchRun, MembershipOutcome::Retired);
            }
        };
        let run = TransportRun {
            incarnation,
            generation,
        };
        let Some(record) = self.runs.get_mut(&run) else {
            return MembershipOutcome::NoSuchRun;
        };
        match TurnMembershipMachine::reduce(record.membership, event) {
            Ok(next) => {
                record.membership = next;
                if let Event::End { turn } = event {
                    MembershipOutcome::Ended {
                        run,
                        turn,
                        session: record.session.clone(),
                    }
                } else {
                    MembershipOutcome::Applied
                }
            }
            Err(reason) => MembershipOutcome::Ignored(reason),
        }
    }

    pub fn offer(
        &mut self,
        authorization: DispatchAuthorization,
        new_turn: Option<TurnId>,
        frame: Frame,
    ) -> Result<(), RefusedOffer<Frame>> {
        if let Err(refusal) = self.check_offer(&authorization, new_turn) {
            return Err(RefusedOffer { refusal, frame });
        }
        self.offers
            .insert(authorization, OfferRecord::Offered { frame, new_turn });
        Ok(())
    }

    fn check_offer(
        &self,
        authorization: &DispatchAuthorization,
        new_turn: Option<TurnId>,
    ) -> Result<(), OfferRefusal> {
        let run = authorization_run(authorization);
        self.check_takes_requests(run)?;
        if (authorization.target == AuthorizationTarget::NewTurn) != new_turn.is_some() {
            return Err(OfferRefusal::WorkDoesNotFitAuthorization);
        }
        let mut other_offer_stands = false;
        for (key, slot) in &self.offers {
            if authorization_run(key) != run || key.input_id != authorization.input_id {
                continue;
            }
            if key.authorization_seq == authorization.authorization_seq {
                return Err(OfferRefusal::AuthorizationSequenceOnRecord);
            }
            other_offer_stands |= slot.evidence() == SlotEvidence::Offered;
        }
        if other_offer_stands {
            return Err(OfferRefusal::OtherAuthorizationOffered);
        }
        Ok(())
    }

    pub fn check_hand_over(
        &mut self,
        authorization: &DispatchAuthorization,
    ) -> Result<HandOverCheck<'_, Frame>, NoSuchOffer> {
        let slot = self.offers.get(authorization).ok_or(NoSuchOffer)?;
        let new_turn = match OfferSlotMachine::reduce(slot.state(), OfferSlotEvent::HandOver) {
            Err(OfferSlotRefusal::AlreadyTaken) => {
                return Ok(HandOverCheck::Settled(DispatchResult::AlreadyTaken(
                    authorization.clone(),
                )));
            }
            Err(OfferSlotRefusal::AlreadyRevoked) => {
                return Ok(HandOverCheck::Settled(DispatchResult::DefinitelyUnsent(
                    authorization.clone(),
                )));
            }
            Ok(OfferSlotState::Taken) => {
                let OfferRecord::Offered { new_turn, .. } = slot else {
                    debug_assert!(
                        false,
                        "hand-over reducer accepted a slot that was not offered"
                    );
                    return Err(NoSuchOffer);
                };
                *new_turn
            }
            Ok(OfferSlotState::Offered | OfferSlotState::Revoked) => {
                debug_assert!(false, "hand-over reducer accepted without taking the slot");
                return Err(NoSuchOffer);
            }
        };
        let run = authorization_run(authorization);
        if self.check_takes_requests(run).is_err() {
            self.revoke(authorization);
            return Ok(HandOverCheck::Settled(DispatchResult::DefinitelyUnsent(
                authorization.clone(),
            )));
        }
        let Some(membership) = self.membership(run) else {
            debug_assert!(false, "run that takes requests must have membership");
            return Err(NoSuchOffer);
        };
        let taken_membership = new_turn
            .map(|turn| {
                TurnMembershipMachine::reduce(membership, TurnMembershipEvent::TakeNew { turn })
            })
            .transpose();
        let (true, Ok(taken_membership)) = (
            authorization.matches_route(run.generation, membership.current_turn()),
            taken_membership,
        ) else {
            self.revoke(authorization);
            return Ok(HandOverCheck::Settled(DispatchResult::StaleTarget(
                authorization.clone(),
            )));
        };
        Ok(HandOverCheck::Ready(ReadyHandOver {
            ledger: self,
            authorization: authorization.clone(),
            taken_membership,
        }))
    }

    pub fn revoke(&mut self, authorization: &DispatchAuthorization) -> Option<DispatchResult> {
        let slot = self.offers.get_mut(authorization)?;
        let evidence = revoke_slot(&mut self.runs, authorization, slot);
        Some(if evidence == SlotEvidence::Taken {
            DispatchResult::AlreadyTaken(authorization.clone())
        } else {
            DispatchResult::Revoked(authorization.clone())
        })
    }

    pub fn revoke_incarnation(
        &mut self,
        incarnation: OwnerIncarnation,
    ) -> Vec<(DispatchAuthorization, SlotEvidence)> {
        let runs = &mut self.runs;
        self.offers
            .iter_mut()
            .filter(|(key, _)| key.incarnation == incarnation)
            .map(|(key, slot)| (key.clone(), revoke_slot(runs, key, slot)))
            .collect()
    }

    pub fn give_up_owner(
        &mut self,
        incarnation: OwnerIncarnation,
        asked_inputs: &BTreeSet<InputId>,
    ) -> OwnerEvidence {
        let mut by_input = BTreeMap::<InputId, Vec<_>>::new();
        for (authorization, evidence) in self.revoke_incarnation(incarnation) {
            if asked_inputs.contains(&authorization.input_id) {
                by_input
                    .entry(authorization.input_id.clone())
                    .or_default()
                    .push((authorization, evidence));
            }
        }
        asked_inputs
            .iter()
            .map(|input| {
                let answer = match by_input.remove(input) {
                    None => OwnerInputEvidence::NeverOffered,
                    Some(slots) if slots.iter().any(|(_, slot)| *slot == SlotEvidence::Taken) => {
                        OwnerInputEvidence::Taken { slots }
                    }
                    Some(slots) if slots.iter().all(|(_, slot)| *slot == SlotEvidence::Revoked) => {
                        OwnerInputEvidence::Withdrawn { slots }
                    }
                    Some(slots) => OwnerInputEvidence::NoEvidence { slots },
                };
                (input.clone(), answer)
            })
            .collect()
    }

    pub fn forget(&mut self, authorization: &DispatchAuthorization) {
        self.offers.remove(authorization);
    }

    pub fn check_takes_requests(&self, run: TransportRun) -> Result<(), RouteRefusal> {
        let record = self.runs.get(&run).ok_or(RouteRefusal::RouteUnavailable)?;
        match record.close {
            RouteCloseState::Open => Ok(()),
            RouteCloseState::Closing { .. } => Err(RouteRefusal::Closing),
        }
    }

    pub fn begin_close(&mut self, run: TransportRun) -> Result<ReadyClose<'_>, RouteRefusal> {
        let record = self
            .runs
            .get_mut(&run)
            .ok_or(RouteRefusal::RouteUnavailable)?;
        match &mut record.close {
            close @ RouteCloseState::Open => Ok(ReadyClose(close)),
            RouteCloseState::Closing { .. } => Err(RouteRefusal::Closing),
        }
    }

    pub fn readmit(&mut self, run: TransportRun, attempt: CloseAttempt) -> ReadmitOutcome {
        let Some(record) = self.runs.get_mut(&run) else {
            return ReadmitOutcome::RouteLost;
        };
        match RouteCloseMachine::reduce(record.close, RouteCloseEvent::Readmit { attempt }) {
            Ok(next) => {
                record.close = next;
                ReadmitOutcome::Reopened
            }
            Err(RouteCloseRefusal::MarkerAbsent) => ReadmitOutcome::MarkerAbsent,
            Err(RouteCloseRefusal::DifferentMarker | RouteCloseRefusal::AlreadyClosing) => {
                ReadmitOutcome::DifferentMarker
            }
        }
    }

    #[must_use]
    pub fn current_turn(&self, run: TransportRun) -> Option<TurnId> {
        self.membership(run)?.current_turn()
    }

    #[must_use]
    pub fn provider_started_turn(&self, run: TransportRun) -> Option<TurnId> {
        match self.membership(run)? {
            TurnMembershipState::ProviderStarted { turn } => Some(turn),
            TurnMembershipState::Loading
            | TurnMembershipState::Idle
            | TurnMembershipState::Provisional { .. }
            | TurnMembershipState::Eligible { .. } => None,
        }
    }

    #[must_use]
    pub fn membership(&self, run: TransportRun) -> Option<TurnMembershipState> {
        self.runs.get(&run).map(|record| record.membership)
    }

    #[must_use]
    pub fn session_of(&self, run: TransportRun) -> Option<&str> {
        self.runs.get(&run).map(|record| record.session.as_str())
    }

    pub fn run_keys(&self) -> impl Iterator<Item = TransportRun> + '_ {
        self.runs.keys().copied()
    }

    pub fn runs_named<'ledger>(
        &'ledger self,
        session: &'ledger str,
    ) -> impl Iterator<Item = TransportRun> + 'ledger {
        self.runs
            .iter()
            .filter_map(move |(&run, record)| (record.session == session).then_some(run))
    }

    pub fn runs_of(
        &self,
        incarnation: OwnerIncarnation,
    ) -> impl Iterator<Item = SessionGeneration> + '_ {
        self.runs
            .keys()
            .filter_map(move |run| (run.incarnation == incarnation).then_some(run.generation))
    }

    pub fn owner_sessions(&self) -> impl Iterator<Item = (OwnerIncarnation, &str)> + '_ {
        self.runs
            .iter()
            .map(|(run, record)| (run.incarnation, record.session.as_str()))
    }

    #[must_use]
    pub fn sole_eligible_turn(&self) -> Option<(TransportRun, TurnId)> {
        let mut turns = self
            .runs
            .iter()
            .filter_map(|(&run, record)| Some((run, record.membership.current_turn()?)));
        let turn = turns.next()?;
        turns.next().is_none().then_some(turn)
    }

    #[must_use]
    pub fn closing_marker(&self, run: TransportRun) -> Option<CloseAttempt> {
        match self.runs.get(&run)?.close {
            RouteCloseState::Open => None,
            RouteCloseState::Closing { attempt } => Some(attempt),
        }
    }
}

fn authorization_run(authorization: &DispatchAuthorization) -> TransportRun {
    TransportRun {
        incarnation: authorization.incarnation,
        generation: authorization.run,
    }
}

fn revoke_slot<Frame>(
    runs: &mut BTreeMap<TransportRun, RunRecord>,
    authorization: &DispatchAuthorization,
    slot: &mut OfferRecord<Frame>,
) -> SlotEvidence {
    if let Some(turn) = slot.revoke()
        && let Some(run) = runs.get_mut(&authorization_run(authorization))
        && let Ok(next) = TurnMembershipMachine::reduce(
            run.membership,
            TurnMembershipEvent::RemoveOnRevoke { turn },
        )
    {
        run.membership = next;
    }
    slot.evidence()
}

#[cfg(test)]
#[path = "run_ledger_frame_custody_test.rs"]
mod frame_custody_tests;
#[cfg(test)]
#[path = "run_ledger_model_test.rs"]
mod model_tests;
#[cfg(test)]
#[path = "run_ledger_test_fixtures.rs"]
mod test_fixtures;
#[cfg(test)]
#[path = "run_ledger_test.rs"]
mod tests;
