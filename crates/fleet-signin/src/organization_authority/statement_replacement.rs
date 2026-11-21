//! The pure stamp reducer uses the retained bytes observed by the applying turn.
use super::{StoredOrganizationStamp, StoredStampRead, VerifiedFleetOrganizationStatement};
use state_machine_core::{
    EventTag, RefusalTag, StateMachine, StateMachineResult, StateTag, Transition, TransitionError,
};

impl StateTag for StoredStampRead {
    fn state_tag(&self) -> &'static str {
        match self {
            Self::Unprovisioned => "absent",
            Self::Stamped(_) => "retained",
        }
    }

    fn is_terminal(&self) -> bool {
        false
    }
}

#[derive(Clone)]
pub(crate) struct OfferStamp<'verified> {
    pub(crate) verified: &'verified VerifiedFleetOrganizationStatement,
    pub(crate) compact: &'verified str,
    pub(crate) replay: ReplayStanding,
}

impl EventTag for OfferStamp<'_> {
    fn event_tag(&self) -> &'static str {
        "offerStamp"
    }
}

/// Standing and the retained expiry sampled by the applying caller.
#[derive(Clone, Copy)]
pub(crate) enum ReplayStanding {
    TrustedAndUnexpired,
    Refused,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum StampEffect {
    InitializeStamp,
    RepairReplay,
    ReplaceStamp {
        predecessor: StoredOrganizationStamp,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub(crate) enum StampRefusal {
    #[error("Fleet organization is immutable")]
    OrganizationMismatch,
    #[error("organization statement predecessor is stale or missing")]
    StaleOrganizationStatement,
}

impl RefusalTag for StampRefusal {
    fn refusal_tag(&self) -> &'static str {
        match self {
            Self::OrganizationMismatch => "organizationMismatch",
            Self::StaleOrganizationStatement => "staleOrganizationStatement",
        }
    }
}

pub(crate) struct StampMachine<'verified>(std::marker::PhantomData<&'verified ()>);

/// The accepted stamp and the exact predecessor guard from one transition.
pub(crate) enum StampMutation {
    Initialize(StoredOrganizationStamp),
    Replace {
        accepted: StoredOrganizationStamp,
        predecessor: StoredOrganizationStamp,
    },
}

impl StampMutation {
    pub(crate) fn accepted(&self) -> &StoredOrganizationStamp {
        match self {
            Self::Initialize(accepted) | Self::Replace { accepted, .. } => accepted,
        }
    }
}

pub(crate) enum StampApplication {
    Write(StampMutation),
    RepairReplay,
}

impl<'verified> StampMachine<'verified> {
    /// Interpret the single effect together with its own reduced state.
    pub(crate) fn applying_plan(
        state: &StoredStampRead,
        offer: OfferStamp<'verified>,
    ) -> Result<StampApplication, TransitionError<StampRefusal>> {
        let (next, effects) = Self::transition(state, offer)?;
        let mut effects = effects.into_iter();
        match (next, effects.next(), effects.next()) {
            (StoredStampRead::Stamped(accepted), Some(StampEffect::InitializeStamp), None) => {
                Ok(StampApplication::Write(StampMutation::Initialize(accepted)))
            }
            (
                StoredStampRead::Stamped(accepted),
                Some(StampEffect::ReplaceStamp { predecessor }),
                None,
            ) => Ok(StampApplication::Write(StampMutation::Replace {
                accepted,
                predecessor,
            })),
            (StoredStampRead::Stamped(_), Some(StampEffect::RepairReplay), None) => {
                Ok(StampApplication::RepairReplay)
            }
            _ => Err(TransitionError::NotAllowed {
                state: state.state_tag(),
                event: "offerStamp",
            }),
        }
    }
}

impl<'verified> StateMachine for StampMachine<'verified> {
    type State = StoredStampRead;
    type Event = OfferStamp<'verified>;
    type Effect = StampEffect;
    type Error = StampRefusal;

    fn transition(
        state: &StoredStampRead,
        event: OfferStamp<'verified>,
    ) -> StateMachineResult<Self> {
        let incoming = event.verified;
        let accepted_stamp = || {
            StoredStampRead::Stamped(StoredOrganizationStamp::new(
                incoming.organization_id().clone(),
                event.compact.to_owned(),
            ))
        };
        let replay = || match event.replay {
            ReplayStanding::TrustedAndUnexpired => {
                Ok((state.clone(), vec![StampEffect::RepairReplay]))
            }
            ReplayStanding::Refused => Err(TransitionError::Domain(
                StampRefusal::StaleOrganizationStatement,
            )),
        };
        match (state, incoming.replaces()) {
            (StoredStampRead::Unprovisioned, None) => {
                Ok((accepted_stamp(), vec![StampEffect::InitializeStamp]))
            }
            (StoredStampRead::Unprovisioned, Some(_)) => Err(TransitionError::Domain(
                StampRefusal::StaleOrganizationStatement,
            )),
            (StoredStampRead::Stamped(stored), _)
                if stored.organization_id() != incoming.organization_id() =>
            {
                Err(TransitionError::Domain(StampRefusal::OrganizationMismatch))
            }
            (StoredStampRead::Stamped(_), None) => replay(),
            (StoredStampRead::Stamped(stored), Some(predecessor)) => {
                let retained_digest = stored.digest();
                if &retained_digest == incoming.digest() {
                    // A byte-identical successor is a retry after a lost reply.
                    replay()
                } else if predecessor == &retained_digest {
                    Ok((
                        accepted_stamp(),
                        vec![StampEffect::ReplaceStamp {
                            predecessor: stored.clone(),
                        }],
                    ))
                } else {
                    Err(TransitionError::Domain(
                        StampRefusal::StaleOrganizationStatement,
                    ))
                }
            }
        }
    }

    fn transitions() -> &'static [Transition] {
        &[
            Transition {
                from: "absent",
                event: "offerStamp",
                to: "retained",
                terminal: false,
            },
            Transition {
                from: "retained",
                event: "offerStamp",
                to: "retained",
                terminal: false,
            },
        ]
    }
}

#[cfg(test)]
#[path = "statement_replacement_test.rs"]
mod tests;
