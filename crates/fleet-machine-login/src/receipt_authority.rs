//! Receipt authority belongs to one restored payload and its applying loan.
use crate::{
    MachineLoginService,
    link_authority::{
        LiveAuthorityRefusal, ValidatedMachineConnection, recheck_lent_link_authority_after_read,
    },
    policy::DecisionTime,
};
use central_storage::StoreFailure;
use fleet_contracts::{MachineConnectionAttachment, mail_link::MachineLinkCloseCode};
use fleet_lent_transaction::CentralTransaction;
use fleet_runtime_ports::{CentralStore, Clock, SyncTime};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};
use workspace_ids::{MachineId, MessageId};

/// One restored receipt. Only a fresh Machine Login restoration can create it.
pub struct RestoredReceipt<Receipt> {
    connection: ValidatedMachineConnection,
    message_id: MessageId,
    receipt: Receipt,
}

impl<Host: CentralStore> MachineLoginService<'_, Host> {
    pub async fn restore_receipt<Receipt>(
        &self,
        attachment: &MachineConnectionAttachment,
        time: DecisionTime,
        message_id: MessageId,
        receipt: Receipt,
    ) -> Result<Result<RestoredReceipt<Receipt>, LiveAuthorityRefusal>, StoreFailure> {
        Ok(self
            .restore_link(attachment, time)
            .await?
            .map(|connection| RestoredReceipt {
                connection,
                message_id,
                receipt,
            }))
    }
}

/// One receipt admitted by the transaction that read its current enrollment.
pub struct AdmittedReceipt<'loan, Transaction, Receipt> {
    transaction: &'loan mut Transaction,
    restored: RestoredReceipt<Receipt>,
    applying_time: SyncTime,
}

impl<Receipt> RestoredReceipt<Receipt> {
    pub async fn admit<'loan, Transaction: CentralTransaction>(
        self,
        transaction: &'loan mut Transaction,
        clock: &dyn Clock,
    ) -> Result<
        Result<AdmittedReceipt<'loan, Transaction, Receipt>, MachineLinkCloseCode>,
        StoreFailure,
    >
    where
        <Transaction::Database as Database>::Arguments: IntoArguments<Transaction::Database>,
        String: Type<Transaction::Database>
            + for<'q> Encode<'q, Transaction::Database>
            + for<'r> Decode<'r, Transaction::Database>,
        Option<String>: Type<Transaction::Database> + for<'r> Decode<'r, Transaction::Database>,
        i64: Type<Transaction::Database> + for<'r> Decode<'r, Transaction::Database>,
        for<'r> &'r str: ColumnIndex<<Transaction::Database as Database>::Row>,
    {
        let (applying_time, lapse) = recheck_lent_link_authority_after_read(
            transaction,
            self.connection.attachment_ref(),
            clock,
            CentralTransaction::after_receipt_enrollment_read,
        )
        .await?;
        match lapse {
            Some(cause) => Ok(Err(cause)),
            None => Ok(Ok(AdmittedReceipt {
                transaction,
                restored: self,
                applying_time,
            })),
        }
    }
}

impl<'loan, Transaction, Receipt> AdmittedReceipt<'loan, Transaction, Receipt> {
    /// Consume the admission and return the same borrowed transaction and checked payload.
    pub fn into_application(
        self,
    ) -> (
        &'loan mut Transaction,
        MachineId,
        MessageId,
        Receipt,
        SyncTime,
    ) {
        (
            self.transaction,
            self.restored.connection.machine_id().clone(),
            self.restored.message_id,
            self.restored.receipt,
            self.applying_time,
        )
    }
}
