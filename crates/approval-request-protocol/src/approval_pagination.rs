//! Bounded organization-scoped reads page by the immutable Central row order.
use serde::{Deserialize, Serialize};
use workspace_ids::{PersonId, RequestId, SessionRef};

use crate::{RequestView, approval_wire_codec::approval_wire_record};

pub const MAX_APPROVAL_PAGE_SIZE: u8 = 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("an approval page can contain at most {MAX_APPROVAL_PAGE_SIZE} requests")]
pub struct InvalidApprovalPageSize;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "u8", into = "u8")]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct ApprovalPageSize(u8);

impl ApprovalPageSize {
    pub fn new(size: u8) -> Result<Self, InvalidApprovalPageSize> {
        Self::try_from(size)
    }

    #[must_use]
    pub const fn get(self) -> u8 {
        self.0
    }
}

impl TryFrom<u8> for ApprovalPageSize {
    type Error = InvalidApprovalPageSize;

    fn try_from(size: u8) -> Result<Self, Self::Error> {
        if (1..=MAX_APPROVAL_PAGE_SIZE).contains(&size) {
            Ok(Self(size))
        } else {
            Err(InvalidApprovalPageSize)
        }
    }
}

impl From<ApprovalPageSize> for u8 {
    fn from(size: ApprovalPageSize) -> Self {
        size.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "Vec<RequestView>", into = "Vec<RequestView>")]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct ApprovalPageItems(Vec<RequestView>);

impl ApprovalPageItems {
    pub fn new(requests: Vec<RequestView>) -> Result<Self, InvalidApprovalPageSize> {
        Self::try_from(requests)
    }

    #[must_use]
    pub fn as_slice(&self) -> &[RequestView] {
        &self.0
    }
}

impl TryFrom<Vec<RequestView>> for ApprovalPageItems {
    type Error = InvalidApprovalPageSize;

    fn try_from(requests: Vec<RequestView>) -> Result<Self, Self::Error> {
        if requests.len() <= usize::from(MAX_APPROVAL_PAGE_SIZE) {
            Ok(Self(requests))
        } else {
            Err(InvalidApprovalPageSize)
        }
    }
}

impl From<ApprovalPageItems> for Vec<RequestView> {
    fn from(requests: ApprovalPageItems) -> Self {
        requests.0
    }
}

/// Position in ascending stored-row UUIDv7 order. Central does not order the
/// page by the Router-minted request ID or the current lifecycle timestamp.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct ApprovalListPosition(
    #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), specta(type = String))]
    RequestId,
);

impl ApprovalListPosition {
    #[must_use]
    pub const fn from_row_id(row_id: RequestId) -> Self {
        Self(row_id)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("the approval continuation belongs to a different destination")]
pub struct ApprovalContinuationAudienceMismatch;

approval_wire_record! {
    [deny_unknown_fields]
    #[derive(Clone, Debug, Eq, PartialEq)]
    #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
    pub struct PersonApprovalContinuation {
        #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), specta(type = String))]
        person_id: PersonId,
        after_row_id: ApprovalListPosition,
    }
}

impl PersonApprovalContinuation {
    #[must_use]
    pub const fn new(person_id: PersonId, after_row_id: ApprovalListPosition) -> Self {
        Self {
            person_id,
            after_row_id,
        }
    }

    /// Check against the authenticated person inside the current authorized read.
    pub fn after_for(
        &self,
        admitted: &PersonId,
    ) -> Result<&ApprovalListPosition, ApprovalContinuationAudienceMismatch> {
        if &self.person_id == admitted {
            Ok(&self.after_row_id)
        } else {
            Err(ApprovalContinuationAudienceMismatch)
        }
    }
}

approval_wire_record! {
    [deny_unknown_fields]
    #[derive(Clone, Debug, Eq, PartialEq)]
    #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
    pub struct SessionApprovalContinuation {
        #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), specta(type = crate::typescript_shapes::SessionRef))]
        session: SessionRef => "crate::approval_wire_codec::decode_named_map",
        after_row_id: ApprovalListPosition,
    }
}

impl SessionApprovalContinuation {
    #[must_use]
    pub const fn new(session: SessionRef, after_row_id: ApprovalListPosition) -> Self {
        Self {
            session,
            after_row_id,
        }
    }

    /// Check against the admitted session inside the current authorized read.
    pub fn after_for(
        &self,
        admitted: &SessionRef,
    ) -> Result<&ApprovalListPosition, ApprovalContinuationAudienceMismatch> {
        if &self.session == admitted {
            Ok(&self.after_row_id)
        } else {
            Err(ApprovalContinuationAudienceMismatch)
        }
    }
}
