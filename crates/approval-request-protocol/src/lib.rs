//! Shared approval request shapes. Central owns admission, lifecycle and delivery debt.

mod answer_validation;
mod approval_answers;
mod approval_endings;
mod approval_offers;
mod approval_values;
mod destination_requests;
mod holder_requests;
mod question_fields;
#[cfg(all(feature = "wire-export", not(target_arch = "wasm32")))]
mod typescript_shapes;

pub use answer_validation::{InvalidAnswer, check_answer};
pub use approval_answers::{
    Answer, ChoiceSelection, QuestionAnswer, QuestionAnswerValue, QuestionFieldAnswer,
};
pub use approval_endings::{
    CancelReason, DecidedBy, Delivery, Ending, EndingView, HolderReport, HolderStopCause,
    RequestState, RequestStateView, RequestView, WaitingStatus,
};
pub use approval_offers::{
    ApprovalOffer, CallOffer, KeptWhere, OfferView, OfferedOption, OfferedOptions, OptionDuration,
    OptionEffect, ParkedCallHolder, ProviderHolder, QuestionOffer, Raise, RequestKind,
};
pub use approval_values::{
    CallOperationName, CentralTime, DESCRIPTION_MAX_CHARACTERS, Description, InvalidApprovalText,
    InvalidInvocationDigest, InvalidQuestionNumber, InvalidWaitingRevision, InvocationDigest,
    LABEL_MAX_CHARACTERS, Label, OfferedOptionId, QuestionFieldId, QuestionNumber,
    SUBJECT_MAX_CHARACTERS, Subject, TITLE_MAX_CHARACTERS, Title, WaitingRevision,
};
pub use destination_requests::{
    DecidePersonReply, DecidePersonRequest, DecideSessionReply, DecideSessionRequest,
    ListPersonRequests, ListPersonRequestsReply, ListSessionRequests, ListSessionRequestsReply,
    PassOnReply, PassOnRequest, ReadPersonRequest, ReadPersonRequestReply, ReadSessionRequest,
    ReadSessionRequestReply, Refusal, WaitingListAtReply, WaitingListAtRequest, WaitingRequestView,
};
pub use holder_requests::{
    CallStopCause, HolderRefusal, OfferEnding, ProviderStopCause, RaiseReceipt, RaiseReply,
    RegisterRunReply, RegisterRunRequest, ReportDeliveryReply, ReportDeliveryRequest, RunRefusal,
    StopReceiptEnding, StopReply, StopRequest,
};
pub use question_fields::{
    ChoiceOption, InvalidOfferedOptions, InvalidQuestionFields, MAX_OFFERED_OPTIONS,
    MAX_QUESTION_FIELDS, QuestionField, QuestionFields,
};
pub use workspace_ids::{ApprovalRequestId, RouterRunId};
