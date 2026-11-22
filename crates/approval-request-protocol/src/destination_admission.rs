//! Request-local admission used by server reply builders.
use workspace_ids::CentralSessionId;

use crate::{
    Answer, DecidedBy, Ending, InvalidApprovalEnding, Raise,
    approval_holder_endings::{check_decision_actor, requires_fresh_factor},
};

/// The person factor status supplied by Central's applying admission path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DecisionFactor {
    FreshPersonFactor,
    NoFreshPersonFactor,
}

/// Central supplies the current members of this exact request after admission.
/// This binding is neither sent over the wire nor decoded from it.
pub struct AdmittedDestination<'request> {
    raise: &'request Raise,
    actor: &'request DecidedBy,
}

impl<'request> AdmittedDestination<'request> {
    pub fn for_request(
        raise: &'request Raise,
        actor: &'request DecidedBy,
        current_destinations: &[DecidedBy],
    ) -> Result<Self, InvalidApprovalEnding> {
        let requester = raise
            .requesting_session()
            .ok_or(InvalidApprovalEnding::UnsupportedRequestKind)?;
        if let DecidedBy::Session { session } = actor
            && requester == session
        {
            return Err(InvalidApprovalEnding::RequesterIsReader);
        }
        if !current_destinations.contains(actor) {
            return Err(InvalidApprovalEnding::DestinationNotAdmitted);
        }
        Ok(Self { raise, actor })
    }

    pub fn decide(
        &self,
        answer: Answer,
        factor: DecisionFactor,
    ) -> Result<Ending, InvalidApprovalEnding> {
        check_decision_actor(self.raise, &answer, self.actor)?;
        if requires_fresh_factor(self.raise, &answer) && factor != DecisionFactor::FreshPersonFactor
        {
            return Err(InvalidApprovalEnding::FreshFactorRequired);
        }
        Ok(Ending::Decided {
            answer,
            by: self.actor.clone(),
        })
    }

    #[must_use]
    pub const fn raise(&self) -> &'request Raise {
        self.raise
    }

    #[must_use]
    pub const fn actor(&self) -> &'request DecidedBy {
        self.actor
    }

    pub(crate) fn session(&self) -> Result<&'request CentralSessionId, InvalidApprovalEnding> {
        match self.actor {
            DecidedBy::Session { session } => Ok(session),
            DecidedBy::Person { .. } => Err(InvalidApprovalEnding::DestinationNotAdmitted),
        }
    }
}

/// A server projection keeps its checked audience until the reply is built.
pub struct DestinationProjection<'request, View> {
    pub(crate) actor: &'request DecidedBy,
    pub(crate) view: View,
}

impl<'request, View> DestinationProjection<'request, View> {
    #[must_use]
    pub const fn view(&self) -> &View {
        &self.view
    }

    /// Central's person entry can publish this projection to its admitted recipient.
    pub fn for_recipient(
        self,
        admitted_recipient: &DecidedBy,
    ) -> Result<View, InvalidApprovalEnding> {
        if self.actor == admitted_recipient {
            Ok(self.view)
        } else {
            Err(InvalidApprovalEnding::DestinationNotAdmitted)
        }
    }
}
