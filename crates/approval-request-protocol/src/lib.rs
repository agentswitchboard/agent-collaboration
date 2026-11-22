//! Shared approval request shapes. Central owns admission, lifecycle and delivery debt.

mod answer_validation;
mod approval_answers;
mod approval_digests;
mod approval_endings;
mod approval_holder_endings;
mod approval_offers;
mod approval_request_views;
mod approval_values;
mod approval_wire_codec;
mod destination_admission;
mod destination_requests;
mod holder_requests;
mod question_fields;
mod unproven_session_call;

pub use answer_validation::{InvalidAnswer, check_answer};
pub use approval_answers::{Answer, QuestionAnswer, QuestionAnswerValue, QuestionFieldAnswer};
pub use approval_digests::{
    ApprovalDigestError, InvalidApprovalDigest, InvocationDigest, RequestContentsDigest,
    canonical_json, sha256_url,
};
pub use approval_endings::{
    ApprovalDecision, CancelReason, DecidedBy, Delivery, Ending, HolderStopCause,
    InvalidApprovalEnding, ProviderStopCause, RequestState, SessionDecider, WaitingStatus,
};
pub use approval_holder_endings::{
    DeliverEndingRequest, HolderEnding, PrivateEnding, ProviderCancellation,
};
pub use approval_offers::{
    ApprovalOffer, OfferView, OfferedOption, OfferedOptions, OptionEffect, OptionLifetime,
    ProviderHolder, QuestionOffer, Raise, RequestKind, RetainedOptionStorage,
    TransientOptionStorage,
};
pub use approval_request_views::{
    DestinationEnding, EndingView, EndingViewWire, GiverDecision, RequestStateView, RequestView,
    StandingEnding,
};
pub use approval_values::{
    ApprovalWindow, CentralTime, DESCRIPTION_MAX_CHARACTERS, Description, InvalidApprovalOrdinal,
    InvalidApprovalText, InvalidApprovalWindow, InvalidQuestionNumber, LABEL_MAX_CHARACTERS, Label,
    MAX_OFFERED_OPTIONS, MAX_QUESTION_FIELDS, MAX_QUESTION_NUMBER_LENGTH, OfferedOptionId,
    QuestionFieldId, QuestionNumber, SUBJECT_MAX_CHARACTERS, SessionCallFolder, SessionCallMode,
    Subject, TITLE_MAX_CHARACTERS, Title,
};
#[doc(hidden)]
pub use approval_wire_codec::decode_named_map;
pub use destination_admission::{AdmittedDestination, DecisionFactor, DestinationProjection};
pub use destination_requests::{
    DecideReply, DecideReplyWire, DecideRequest, PassOnReply, PassOnRequest,
    RetainedSessionDecision,
};
pub use holder_requests::{
    DeliverEndingReply, RaiseReceipt, RaiseReply, RegisterRunReply, RegisterRunRequest, RunRefusal,
    StopReceiptEnding, StopReply, StopRequest,
};
pub use question_fields::{
    ChoiceOption, ChoiceOptions, InvalidChoiceRange, InvalidOfferedOptions, InvalidQuestionFields,
    MultiChoiceSelection, QuestionField, QuestionFields,
};
pub use unproven_session_call::{
    CallCancellation, CallOffer, CallStopCause, ParkedCallEnding, ParkedCallEndingView,
    ParkedCallHolder, ParkedCallStateView, SessionCallDecision, SessionCallOperation,
    SessionCallProgram,
};
pub use workspace_ids::{ApprovalRequestId, CentralSessionId, RouterRunId};
