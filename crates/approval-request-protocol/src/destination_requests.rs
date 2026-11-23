//! Checked parent-session operations on Central's approvals service.
use serde::{Deserialize, Serialize};
use workspace_ids::{ApprovalRequestId, CentralSessionId, RequestId};

use crate::{
    AdmittedDestination, ApprovalDecision, DeliverEndingRequest, EndingView, InvalidApprovalEnding,
    ProviderAnswer, ProviderEndingView, SessionDecider,
    approval_holder_endings::check_decision_actor,
    approval_wire_codec::{approval_wire_record, approval_wire_union},
};

approval_wire_record! {
[deny_unknown_fields]
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct DecideRequest {
    pub request_id: ApprovalRequestId,
    #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), specta(type = String))]
    pub operation_id: RequestId,
    pub value: ProviderAnswer,
}
}

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum DecideReplyWire {
    SavedDecision { decision: ApprovalDecision },
    Standing { ending: ProviderEndingView },
    NotVisible {},
    Invalid {},
    TooLate {},
    OperationConflict {},
}
}

approval_wire_record! {
[deny_unknown_fields]
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct PassOnRequest {
    pub request_id: ApprovalRequestId,
}
}

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum PassOnReply {
    Forwarded {},
    NotVisible {},
    TooLate {},
    Ended {},
}
}

/// A parent-session decision reply. Use the checked request builders to publish it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct DecideReply(DecideReplyWire);

impl DecideReply {
    /// Inspect a reply after decoding it on the receiving side.
    #[must_use]
    pub const fn wire_shape(&self) -> &DecideReplyWire {
        &self.0
    }
}

/// Saved by Central for a single admitted session's successful Decide.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetainedSessionDecision {
    pub request_id: ApprovalRequestId,
    pub session: CentralSessionId,
    pub operation_id: RequestId,
    pub answer: ProviderAnswer,
}

impl DecideRequest {
    /// Project a retained success only for the session and operation that gave it.
    pub fn saved_reply(
        &self,
        admitted: &AdmittedDestination<'_>,
        retained: &RetainedSessionDecision,
    ) -> Result<DecideReply, InvalidApprovalEnding> {
        let session = admitted.session()?;
        if &self.request_id != admitted.raise().request_id()
            || self.request_id != retained.request_id
            || self.operation_id != retained.operation_id
            || self.value != retained.answer
            || session != &retained.session
        {
            return Err(InvalidApprovalEnding::RetainedDecisionMismatch);
        }
        check_decision_actor(
            admitted.raise(),
            &retained.answer.clone().into(),
            admitted.actor(),
        )?;
        Ok(DecideReply(DecideReplyWire::SavedDecision {
            decision: ApprovalDecision {
                answer: retained.answer.clone(),
                by: SessionDecider::Session {
                    session: session.clone(),
                },
            },
        }))
    }

    pub fn standing_reply(
        &self,
        admitted: &AdmittedDestination<'_>,
        ending: &DeliverEndingRequest,
    ) -> Result<DecideReply, InvalidApprovalEnding> {
        admitted.session()?;
        if &self.request_id != admitted.raise().request_id() {
            return Err(InvalidApprovalEnding::BindingMismatch);
        }
        let projection = EndingView::for_destination(admitted, ending)?;
        Ok(DecideReply(DecideReplyWire::Standing {
            ending: projection.for_recipient(admitted.actor())?.try_into()?,
        }))
    }
}

impl DecideReply {
    #[must_use]
    pub const fn not_visible() -> Self {
        Self(DecideReplyWire::NotVisible {})
    }
    #[must_use]
    pub const fn invalid() -> Self {
        Self(DecideReplyWire::Invalid {})
    }
    #[must_use]
    pub const fn too_late() -> Self {
        Self(DecideReplyWire::TooLate {})
    }
    #[must_use]
    pub const fn operation_conflict() -> Self {
        Self(DecideReplyWire::OperationConflict {})
    }
}
