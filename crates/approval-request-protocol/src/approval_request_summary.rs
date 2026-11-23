//! The list projection carries lifecycle information without the offer.

use crate::{
    ApprovalRequestId, CentralTime, DestinationEnding, ParkedCallEndingView, ParkedCallStateView,
    RequestKind, RequestStateView, RequestView, StandingEnding,
    approval_request_views::RequestViewFields,
    approval_wire_codec::{approval_wire_record, approval_wire_union},
};

approval_wire_record! {
[deny_unknown_fields]
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
pub struct RequestSummary {
    pub request_id: ApprovalRequestId,
    pub kind: RequestKind,
    pub state: ApprovalSummaryState,
    /// First saved time as an RFC 3339 CentralTime string.
    pub raised_at: CentralTime,
    /// Deadline as an RFC 3339 CentralTime string.
    pub deadline: CentralTime,
}
}

approval_wire_union! {
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
pub enum ApprovalSummaryState {
    Waiting {},
    Ended { ending: ApprovalSummaryEnding },
}
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum ApprovalSummaryEnding {
    Decided,
    Cancelled,
    TimedOut,
}

impl From<&RequestView> for RequestSummary {
    fn from(view: &RequestView) -> Self {
        let (kind, state) = match view.fields() {
            RequestViewFields::ProviderApproval { state, .. } => (
                RequestKind::ProviderApproval,
                summarize_provider_state(state),
            ),
            RequestViewFields::ProviderQuestion { state, .. } => (
                RequestKind::ProviderQuestion,
                summarize_provider_state(state),
            ),
            RequestViewFields::UnprovenCall { state, .. } => {
                (RequestKind::UnprovenCall, summarize_call_state(state))
            }
        };
        Self {
            request_id: view.request_id().clone(),
            kind,
            state,
            raised_at: view.window().raised_at(),
            deadline: view.window().deadline(),
        }
    }
}

fn summarize_provider_state<Answer, Cancellation>(
    state: &RequestStateView<Answer, Cancellation>,
) -> ApprovalSummaryState {
    let ending = match state {
        RequestStateView::Waiting { .. } => return ApprovalSummaryState::Waiting {},
        RequestStateView::Ended { ending, .. } => match ending {
            DestinationEnding::Giver { .. } => ApprovalSummaryEnding::Decided,
            DestinationEnding::Standing { ending } => match ending {
                StandingEnding::Decided { .. } => ApprovalSummaryEnding::Decided,
                StandingEnding::Cancelled { .. } => ApprovalSummaryEnding::Cancelled,
                StandingEnding::TimedOut {} => ApprovalSummaryEnding::TimedOut,
            },
        },
    };
    ApprovalSummaryState::Ended { ending }
}

fn summarize_call_state(state: &ParkedCallStateView) -> ApprovalSummaryState {
    let ending = match state {
        ParkedCallStateView::Waiting {} => return ApprovalSummaryState::Waiting {},
        ParkedCallStateView::Ended { ending, .. } => match ending {
            ParkedCallEndingView::Decided { .. } | ParkedCallEndingView::Giver { .. } => {
                ApprovalSummaryEnding::Decided
            }
            ParkedCallEndingView::Cancelled { .. } => ApprovalSummaryEnding::Cancelled,
            ParkedCallEndingView::TimedOut {} => ApprovalSummaryEnding::TimedOut,
        },
    };
    ApprovalSummaryState::Ended { ending }
}
