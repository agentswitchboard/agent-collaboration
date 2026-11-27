//! Connection-local callback identity and turn eligibility.

use crate::transport_run::TransportRun;
use provider_session_machines::{
    session_turn_machine::TurnId,
    tool_ownership::{
        ToolObservation, ToolOwnership, ToolOwnershipEffect, ToolOwnershipEvent, ToolSessionPhase,
        reduce_tool_ownership,
    },
};
use std::collections::{BTreeMap, BTreeSet};
use switchboard_harness_shared_mechanics::run_ledger::{RunLedger, TurnMembershipState};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CallbackKind {
    Continuation,
    Request,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CallbackResolution {
    Exact {
        run: TransportRun,
        turn: TurnId,
    },
    ExactIdle {
        run: TransportRun,
        item_id: String,
    },
    Ambiguous,
    Retired,
    NeverObserved {
        sole_eligible: Option<(TransportRun, TurnId)>,
    },
}

struct ToolClaim {
    run: TransportRun,
    ownership: ToolOwnership,
}

#[derive(Default)]
pub(crate) struct ToolClaims {
    observed: BTreeMap<TransportRun, BTreeSet<String>>,
    claims: BTreeMap<String, BTreeMap<String, ToolClaim>>,
}

impl ToolClaims {
    pub(crate) fn open(&mut self, run: TransportRun) {
        self.observed.entry(run).or_default();
    }

    pub(crate) fn tool_in_current_turn(
        &self,
        session: &str,
        run: TransportRun,
        tool: &str,
        turn: Option<TurnId>,
    ) -> bool {
        let Some(turn) = turn else {
            return false;
        };
        self.claims
            .get(tool)
            .and_then(|claims| claims.get(session))
            .is_some_and(|claim| {
                claim.run == run
                    && matches!(claim.ownership, ToolOwnership::Owned { turn: Some(owner), .. } if owner == turn)
            })
    }

    pub(crate) fn allows_explicit_tool_request(
        &self,
        session: &str,
        run: TransportRun,
        tool: &str,
        turn: Option<TurnId>,
    ) -> bool {
        let Some(turn) = turn else {
            return false;
        };
        self.claims
            .get(tool)
            .and_then(|claims| claims.get(session))
            .is_none_or(|claim| {
                claim.run == run
                    && matches!(claim.ownership,
                        ToolOwnership::Owned { turn: expected, .. }
                        if expected.is_none_or(|expected| expected == turn)
                    )
            })
    }

    pub(crate) fn end_turn(&mut self, session: &str, run: TransportRun, turn: TurnId) {
        let Some(observed) = self.observed.get_mut(&run) else {
            return;
        };
        update_claims(
            observed,
            &mut self.claims,
            session,
            run,
            ToolOwnershipEvent::TurnEnded {
                generation: run.generation,
                turn,
            },
        );
    }

    pub(crate) fn retire(&mut self, session: &str, run: TransportRun) {
        if let Some(mut observed) = self.observed.remove(&run) {
            update_claims(
                &mut observed,
                &mut self.claims,
                session,
                run,
                ToolOwnershipEvent::RunRetired {
                    generation: run.generation,
                },
            );
        }
    }

    pub(crate) fn observe(
        &mut self,
        session: &str,
        run: TransportRun,
        tool_id: &str,
        observation: ToolObservation,
        membership: TurnMembershipState,
    ) -> ToolOwnershipEffect {
        let Some(observed) = self.observed.get_mut(&run) else {
            return ToolOwnershipEffect::None;
        };
        let phase = match membership {
            TurnMembershipState::Loading => ToolSessionPhase::Loading {
                generation: run.generation,
            },
            TurnMembershipState::Eligible { turn }
            | TurnMembershipState::ProviderStarted { turn } => ToolSessionPhase::LiveRunning {
                generation: run.generation,
                turn,
            },
            TurnMembershipState::Idle | TurnMembershipState::Provisional { .. } => {
                ToolSessionPhase::LiveIdle {
                    generation: run.generation,
                }
            }
        };
        let previous = self
            .claims
            .get(tool_id)
            .and_then(|claims| claims.get(session));
        // A new incarnation starts its generation counter again. Only replay
        // can refresh a claim whose previous incarnation is known to this connection.
        let previous = match previous {
            Some(claim) if claim.run.incarnation != run.incarnation => {
                if !matches!(phase, ToolSessionPhase::Loading { .. }) {
                    return ToolOwnershipEffect::None;
                }
                None
            }
            other => other.map(|claim| claim.ownership),
        };
        let (ownership, effect) =
            reduce_tool_ownership(previous, ToolOwnershipEvent::Observe { phase, observation });
        if let Some(ownership) = ownership {
            // Claims rejected by the reducer must keep their original run stamp.
            if matches!(effect, ToolOwnershipEffect::ForwardFrame { .. }) {
                self.claims
                    .entry(tool_id.to_owned())
                    .or_default()
                    .insert(session.to_owned(), ToolClaim { run, ownership });
                observed.insert(tool_id.to_owned());
            }
        }
        effect
    }

    /// A never-observed ID can use exactly one eligible turn. Known IDs are
    /// resolved from retained claims even when only one session is active.
    pub(crate) fn resolve<Frame>(
        &self,
        ledger: &RunLedger<Frame>,
        tool_id: &str,
        kind: CallbackKind,
    ) -> CallbackResolution {
        let Some(claims) = self.claims.get(tool_id) else {
            return CallbackResolution::NeverObserved {
                sole_eligible: ledger.sole_eligible_turn(),
            };
        };
        if claims.len() > 1 {
            return CallbackResolution::Ambiguous;
        }
        let Some(claim) = claims.values().next() else {
            return CallbackResolution::NeverObserved {
                sole_eligible: None,
            };
        };
        let Some(membership) = ledger
            .membership(claim.run)
            .filter(|membership| *membership != TurnMembershipState::Loading)
        else {
            return CallbackResolution::Retired;
        };
        let turn = membership.current_turn();
        match (claim.ownership, turn) {
            (ToolOwnership::Owned { turn: expected, .. }, Some(turn))
                if expected.is_none_or(|expected| expected == turn) =>
            {
                CallbackResolution::Exact {
                    run: claim.run,
                    turn,
                }
            }
            (ToolOwnership::Owned { turn: None, .. } | ToolOwnership::Retired { .. }, None)
                if membership == TurnMembershipState::Idle
                    && kind == CallbackKind::Continuation =>
            {
                CallbackResolution::ExactIdle {
                    run: claim.run,
                    item_id: tool_id.to_owned(),
                }
            }
            _ => CallbackResolution::Retired,
        }
    }
}

fn update_claims(
    observed: &mut BTreeSet<String>,
    claims: &mut BTreeMap<String, BTreeMap<String, ToolClaim>>,
    session: &str,
    run: TransportRun,
    event: ToolOwnershipEvent,
) {
    observed.retain(|tool_id| {
        let Some(claim) = claims
            .get_mut(tool_id)
            .and_then(|owners| owners.get_mut(session))
            .filter(|claim| claim.run == run)
        else {
            return false;
        };
        if let (Some(ownership), _) = reduce_tool_ownership(Some(claim.ownership), event) {
            claim.ownership = ownership;
        }
        claim.ownership.owned_generation().is_some()
    });
}
