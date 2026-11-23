use central_link_client::UntouchedCommand;
use switchboard_machine_core_store::{RowCorruption, StoreScan};

/// A storage diagnostic custody reports to the operator independently of any session's status.
#[derive(Debug, Eq, PartialEq)]
pub enum CustodyOperatorEvent {
    CorruptOutboxRow(RowCorruption),
    CorruptInboxRow(RowCorruption),
    UntouchedBatchFault(UntouchedCommand),
}

pub(crate) fn outbox_scan<T>(scan: StoreScan<T>, events: &mut Vec<CustodyOperatorEvent>) -> T {
    events.extend(
        scan.corrupt_rows
            .into_iter()
            .map(CustodyOperatorEvent::CorruptOutboxRow),
    );
    scan.healthy
}

pub(crate) fn inbox_scan<T>(scan: StoreScan<T>, events: &mut Vec<CustodyOperatorEvent>) -> T {
    events.extend(
        scan.corrupt_rows
            .into_iter()
            .map(CustodyOperatorEvent::CorruptInboxRow),
    );
    scan.healthy
}

#[cfg(test)]
#[path = "custody_operator_test.rs"]
mod tests;
