//! The local owning store of a direct-message lifecycle.
use crate::direct_message_rows::StoredDirectState;
use central_engine_model::{FailureRecord, ItemFailureClass};
use central_storage::{AlarmPublicationFailure, CorruptReason};
use fleet_contracts::MailDeliveryReady;
use fleet_engine::{CheckedDueFailure, CheckedDueUnit};
use fleet_machine_login::receipt_authority::RestoredReceipt;
use fleet_runtime_ports::{Clock, IdSource};
use futures_util::future::BoxFuture;
use std::sync::Arc;
use workspace_ids::{MachineId, MessageId, RequestId, SessionRef};
use workspace_model::{
    ActorIdentityId, Timestamp,
    central_mail::{MessageRecord, MessageStatus, ReceiptKind, ReceiptResult, SendMessageRequest},
};

#[derive(Clone)]
pub struct DirectSendRequestKey {
    pub request_id: RequestId,
    pub digest: String,
    pub cutoff: Timestamp,
}

pub struct DirectSendLookup {
    pub request: DirectSendRequestKey,
    pub caller: Option<ActorIdentityId>,
}

pub enum RetainedDirectSend {
    Queued { message_id: MessageId },
    RefusedUnknownTarget { target: SessionRef },
}

pub enum RetainedDirectSendRead {
    Missing,
    RequestIdReused,
    Found(RetainedDirectSend),
}

pub struct AdmittedDirectSend {
    pub request: DirectSendRequestKey,
    pub caller: ActorIdentityId,
    pub send: SendMessageRequest,
    pub sent_at: Timestamp,
    pub accepted_at: Timestamp,
    pub ids: Arc<dyn IdSource>,
}

/// Atomic send results exhaust the direct send's own success alternatives.
/// A wake answer cannot be constructed at this service boundary.
///
/// ```compile_fail,E0599
/// use fleet_mail::DirectSendResult;
/// let _ = DirectSendResult::WakeCancelled;
/// ```
pub enum DirectSendResult {
    Committed {
        message_id: MessageId,
        ready: MailDeliveryReady,
    },
    RetainedRefusal {
        target: SessionRef,
    },
}

pub type RestoredMailReceipt = RestoredReceipt<ReceiptKind>;

/// A single receipt admitted on the transaction that read its enrollment.
///
/// ```compile_fail,E0451
/// use fleet_mail::{AdmittedReceipt, RestoredMailReceipt};
/// use fleet_runtime_ports::SyncTime;
///
/// fn admit_without_recheck<T>(transaction: &mut T, restored: RestoredMailReceipt, applying_time: SyncTime) -> AdmittedReceipt<'_, T> {
///     AdmittedReceipt { transaction, restored, applying_time }
/// }
/// ```
///
/// ```compile_fail
/// use fleet_mail::RestoredMailReceipt;
/// use fleet_machine_login::link_authority::ValidatedMachineConnection;
/// use workspace_ids::MessageId;
/// use workspace_model::central_mail::ReceiptKind;
///
/// fn mint_from_connection(
///     connection: ValidatedMachineConnection,
///     message_id: MessageId,
///     receipt: ReceiptKind,
/// ) {
///     let _ = RestoredMailReceipt::new(connection.clone(), message_id, receipt);
/// }
/// ```
///
/// ```compile_fail,E0451
/// use fleet_mail::RestoredMailReceipt;
/// use fleet_machine_login::link_authority::ValidatedMachineConnection;
/// use workspace_ids::MessageId;
/// use workspace_model::central_mail::ReceiptKind;
///
/// fn forge(connection: ValidatedMachineConnection, message_id: MessageId, receipt: ReceiptKind) {
///     let _: RestoredMailReceipt = RestoredMailReceipt { connection, message_id, receipt };
/// }
/// ```
///
/// ```compile_fail
/// use fleet_mail::RestoredMailReceipt;
/// fn duplicate(receipt: RestoredMailReceipt) {
///     let _ = receipt.clone();
/// }
/// ```
///
/// ```compile_fail,E0616
/// use fleet_mail::RestoredMailReceipt;
/// use workspace_model::central_mail::ReceiptKind;
///
/// fn swap_payload(mut receipt: RestoredMailReceipt, other: ReceiptKind) {
///     receipt.receipt = other;
/// }
/// ```
///
/// ```compile_fail,E0382
/// use fleet_mail::AdmittedReceipt;
///
/// fn apply_twice<T>(command: AdmittedReceipt<'_, T>) {
///     let _ = command.into_application();
///     let _ = command.into_application();
/// }
/// ```
///
/// ```compile_fail
/// use fleet_mail::AdmittedReceipt;
/// use workspace_model::central_mail::ReceiptKind;
///
/// fn escape<'loan, T: 'static>(command: AdmittedReceipt<'loan, T>)
///     -> AdmittedReceipt<'static, T> {
///     command
/// }
/// ```
///
/// ```compile_fail,E0451
/// use fleet_contracts::MachineConnectionAttachment;
/// use fleet_machine_login::link_authority::ValidatedMachineConnection;
///
/// fn forge(attachment: MachineConnectionAttachment) {
///     let _ = ValidatedMachineConnection { attachment };
/// }
/// ```
///
/// ```compile_fail,E0277
/// let _: fleet_mail::RestoredMailReceipt = serde_json::from_str("{}").unwrap();
/// ```
///
/// ```compile_fail,E0277
/// use fleet_mail::AdmittedReceipt;
/// use workspace_model::central_mail::ReceiptKind;
/// let _: AdmittedReceipt<'static, ()> = serde_json::from_str("{}").unwrap();
/// ```
///
/// ```compile_fail,E0277
/// use fleet_machine_login::link_authority::ValidatedMachineConnection;
/// let _: ValidatedMachineConnection = serde_json::from_str("{}").unwrap();
/// ```
pub type AdmittedReceipt<'loan, Transaction> =
    fleet_machine_login::receipt_authority::AdmittedReceipt<'loan, Transaction, ReceiptKind>;

pub enum ApplyReceipt {
    Applied { ready: Option<MailDeliveryReady> },
    Stop(ReceiptResult),
    AuthorityLapsed(fleet_contracts::mail_link::MachineLinkCloseCode),
}

pub struct DirectMark {
    pub message_id: MessageId,
    pub occurrence: Timestamp,
    pub now: Timestamp,
    pub ids: Arc<dyn IdSource>,
}

/// Proof of what the applying transaction saw before a rejected due mark.
pub struct RejectedDirectMark {
    pub(super) command: DirectMark,
    pub(super) row: StoredDirectState,
    pub(super) attempt: Option<FailureRecord>,
    pub(super) class: ItemFailureClass,
}

pub type ApplyMark = CheckedDueUnit<Box<RejectedDirectMark>>;

pub struct OwnedDirectStatus {
    pub message_id: MessageId,
    pub sender: ActorIdentityId,
    pub clock: Arc<dyn Clock>,
    pub ids: Arc<dyn IdSource>,
}

pub struct AppliedDirectStatus {
    pub status: MessageStatus,
    pub ready: Option<MailDeliveryReady>,
}

pub struct DirectOfferLookup {
    pub message_id: MessageId,
    pub recipient: MachineId,
    pub now: Timestamp,
}

pub struct OfferPageLookup {
    pub recipient: MachineId,
    pub after: Option<fleet_contracts::mail_link::WelcomeDeliveryCursor>,
    pub now: Timestamp,
}

pub struct DirectOffer {
    pub message: MessageRecord,
    pub sender: workspace_model::ResolvedSender,
    pub cutoff: Timestamp,
}

/// A local read can fail or observe corrupt state.
///
/// ```compile_fail,E0599
/// use fleet_mail::DirectReadFailure;
/// let _ = DirectReadFailure::AnswerLost;
/// ```
#[derive(Debug, thiserror::Error)]
pub enum DirectReadFailure {
    #[error("the direct-message store is unavailable")]
    Unavailable,
    #[error("corrupt direct-message data: {reason:?}")]
    Corrupt { reason: CorruptReason },
}

#[derive(Debug, thiserror::Error)]
pub enum DirectWriteFailure {
    #[error(transparent)]
    Read(#[from] DirectReadFailure),
    #[error("the transaction rolled back on a guard or constraint conflict")]
    Conflict,
    #[error("the direct-message transaction's answer was lost")]
    AnswerLost,
    #[error("the transaction could not publish its alarm: {reason:?}")]
    AlarmUnpublishable { reason: AlarmPublicationFailure },
}

/// Commands and answers for the two Central hosts. The owners lend their local
/// executor to the portable repository without exposing it to runtime callers.
pub trait DirectMessageStore: Send + Sync {
    fn read_retained_send(
        &self,
        lookup: DirectSendLookup,
    ) -> BoxFuture<'_, Result<RetainedDirectSendRead, DirectReadFailure>>;
    fn apply_direct_send(
        &self,
        send: AdmittedDirectSend,
    ) -> BoxFuture<'_, Result<DirectSendResult, DirectWriteFailure>>;
    fn apply_receipt(
        &self,
        receipt: RestoredMailReceipt,
        ids: Arc<dyn IdSource>,
    ) -> BoxFuture<'_, Result<ApplyReceipt, DirectWriteFailure>>;
    fn apply_mark(&self, mark: DirectMark) -> BoxFuture<'_, Result<ApplyMark, CheckedDueFailure>>;
    fn charge_rejected_mark(
        &self,
        rejection: Box<RejectedDirectMark>,
        now: Timestamp,
    ) -> BoxFuture<'_, Result<Option<FailureRecord>, CheckedDueFailure>>;
    fn settle_and_read_status(
        &self,
        status: OwnedDirectStatus,
    ) -> BoxFuture<'_, Result<AppliedDirectStatus, DirectWriteFailure>>;
    fn read_offer_page(
        &self,
        page: OfferPageLookup,
    ) -> BoxFuture<'_, Result<Vec<fleet_contracts::mail_link::WindowedDelivery>, DirectReadFailure>>;
    fn read_offer(
        &self,
        offer: DirectOfferLookup,
    ) -> BoxFuture<'_, Result<Option<DirectOffer>, DirectReadFailure>>;
}
