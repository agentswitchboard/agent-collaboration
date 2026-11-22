//! Shared approval request shapes. Central owns admission, lifecycle and delivery debt.

mod answer_validation;
mod approval_answers;
mod approval_digests;
mod approval_endings;
mod approval_holder_endings;
mod approval_offers;
mod approval_pagination;
mod approval_request_views;
mod approval_values;
mod approval_wire_codec;
mod delivery_evidence;
mod destination_requests;
mod holder_requests;
mod question_fields;
#[cfg(all(feature = "wire-export", not(target_arch = "wasm32")))]
mod typescript_shapes;

pub use answer_validation::{InvalidAnswer, check_answer};
pub use approval_answers::{Answer, QuestionAnswer, QuestionAnswerValue, QuestionFieldAnswer};
pub use approval_digests::{
    ApprovalDigestError, EndingDigest, InvalidApprovalDigest, InvocationDigest,
    RequestContentsDigest, canonical_json, sha256_url,
};
pub use approval_endings::{
    ApprovalDecision, CallStopCause, CancelReason, DecidedBy, Delivery, Ending, HolderStopCause,
    InvalidApprovalEnding, ProviderStopCause, RequestState, WaitingStatus,
};
pub use approval_holder_endings::{
    CallCancellation, DeliverEndingRequest, HolderEnding, PrivateEnding, ProviderCancellation,
};
pub use approval_offers::{
    ApprovalOffer, CallOffer, OfferView, OfferedOption, OfferedOptions, OptionEffect,
    OptionLifetime, ParkedCallHolder, ProviderHolder, QuestionOffer, Raise, RequestKind,
    RetainedOptionStorage, TransientOptionStorage,
};
pub use approval_pagination::{
    ApprovalContinuationAudienceMismatch, ApprovalListPosition, ApprovalPageItems,
    ApprovalPageSize, InvalidApprovalPageSize, MAX_APPROVAL_PAGE_SIZE, SessionApprovalContinuation,
};
pub use approval_request_views::{
    DestinationEnding, EndingView, GiverDecision, RequestStateView, RequestView, StandingEnding,
};
pub use approval_values::{
    ApprovalWindow, CallOperationName, CentralTime, DESCRIPTION_MAX_CHARACTERS, Description,
    InvalidApprovalOrdinal, InvalidApprovalText, InvalidApprovalWindow, InvalidQuestionNumber,
    InvalidWaitingRevision, LABEL_MAX_CHARACTERS, Label, MAX_OFFERED_OPTIONS, MAX_QUESTION_FIELDS,
    MAX_QUESTION_NUMBER_LENGTH, OfferedOptionId, QuestionFieldId, QuestionNumber,
    SUBJECT_MAX_CHARACTERS, Subject, TITLE_MAX_CHARACTERS, Title, WaitingRevision,
};
#[doc(hidden)]
pub use approval_wire_codec::decode_named_map;
pub use delivery_evidence::{
    AcpDeliveryEvidence, AcpFlushObservation, DeliveryEvidence, NativeDeliveryEvidence,
    ParkedCallEvidence, ParkedCallResult,
};
pub use destination_requests::{
    DecideReply, DecideRequest, ListSessionRequests, ListSessionRequestsReply, PassOnReply,
    PassOnRequest, ReadSessionRequest, ReadSessionRequestReply, Refusal, WaitingListAtReply,
    WaitingListAtRequest, WaitingRequestView,
};
pub use holder_requests::{
    AuthorityProbeReply, AuthorityProbeRequest, DeliverEndingReply, RaiseReceipt, RaiseReply,
    RegisterRunReply, RegisterRunRequest, ReportDeliveryReply, ReportDeliveryRequest, RunRefusal,
    StopReceiptEnding, StopReply, StopRequest,
};
pub use question_fields::{
    ChoiceOption, ChoiceOptions, InvalidChoiceRange, InvalidOfferedOptions, InvalidQuestionFields,
    MultiChoiceSelection, QuestionField, QuestionFields,
};
pub use workspace_ids::{ApprovalRequestId, RouterRunId};
