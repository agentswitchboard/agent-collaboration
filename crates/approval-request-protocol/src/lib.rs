//! Shared approval request shapes. Central owns admission, lifecycle and delivery debt.

mod answer_validation;
mod approval_answers;
mod approval_endings;
mod approval_holder_endings;
mod approval_offers;
mod approval_pagination;
mod approval_request_views;
mod approval_values;
mod approval_wire_codec;
mod destination_requests;
mod holder_requests;
mod question_fields;
#[cfg(all(feature = "wire-export", not(target_arch = "wasm32")))]
mod typescript_shapes;

pub use answer_validation::{InvalidAnswer, check_answer};
pub use approval_answers::{Answer, QuestionAnswer, QuestionAnswerValue, QuestionFieldAnswer};
pub use approval_endings::{
    ApprovalDecision, CallStopCause, CancelReason, DecidedBy, Delivery, Ending, HolderReport,
    HolderStopCause, InvalidApprovalEnding, ProviderStopCause, RequestState, WaitingStatus,
};
pub use approval_holder_endings::{
    CallCancellation, HolderEnding, OfferEnding, ProviderCancellation,
};
pub use approval_offers::{
    ApprovalOffer, CallOffer, OfferView, OfferedOption, OfferedOptions, OptionEffect,
    OptionLifetime, ParkedCallHolder, ProviderHolder, QuestionOffer, Raise, RequestKind,
    RetainedOptionStorage, TransientOptionStorage,
};
pub use approval_pagination::{
    ApprovalContinuationAudienceMismatch, ApprovalListPosition, ApprovalPageItems,
    ApprovalPageSize, InvalidApprovalPageSize, MAX_APPROVAL_PAGE_SIZE, PersonApprovalContinuation,
    SessionApprovalContinuation,
};
pub use approval_request_views::{DestinationEnding, EndingView, RequestStateView, RequestView};
pub use approval_values::{
    ApprovalWindow, CallOperationName, CentralTime, DESCRIPTION_MAX_CHARACTERS, Description,
    InvalidApprovalOrdinal, InvalidApprovalText, InvalidApprovalWindow, InvalidInvocationDigest,
    InvalidQuestionNumber, InvalidWaitingRevision, InvocationDigest, LABEL_MAX_CHARACTERS, Label,
    MAX_OFFERED_OPTIONS, MAX_QUESTION_FIELDS, OfferedOptionId, QuestionFieldId, QuestionNumber,
    SUBJECT_MAX_CHARACTERS, Subject, TITLE_MAX_CHARACTERS, Title, WaitingRevision,
};
pub use destination_requests::{
    DecidePersonReply, DecidePersonRequest, DecideSessionReply, DecideSessionRequest,
    ListPersonRequests, ListPersonRequestsReply, ListSessionRequests, ListSessionRequestsReply,
    PassOnReply, PassOnRequest, ReadPersonRequest, ReadPersonRequestReply, ReadSessionRequest,
    ReadSessionRequestReply, Refusal, WaitingListAtReply, WaitingListAtRequest, WaitingRequestView,
};
pub use holder_requests::{
    HolderRefusal, RaiseReceipt, RaiseReply, RegisterRunReply, RegisterRunRequest,
    ReportDeliveryReply, ReportDeliveryRequest, RunRefusal, StopReceiptEnding, StopReply,
    StopRequest,
};
pub use question_fields::{
    ChoiceOption, ChoiceOptions, InvalidChoiceRange, InvalidOfferedOptions, InvalidQuestionFields,
    MultiChoiceSelection, QuestionField, QuestionFields,
};
pub use workspace_ids::{ApprovalRequestId, RouterRunId};
