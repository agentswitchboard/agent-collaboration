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
}

impl EventTag for OfferStamp<'_> {
    fn event_tag(&self) -> &'static str {
        "offerStamp"
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum StampEffect {
    InitializeStamp,
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
            (StoredStampRead::Stamped(_), None) => Ok((state.clone(), vec![])),
            (StoredStampRead::Stamped(stored), Some(predecessor)) => {
                let retained_digest = stored.digest();
                if &retained_digest == incoming.digest() {
                    // A byte-identical successor is a retry after a lost reply.
                    Ok((state.clone(), vec![]))
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
