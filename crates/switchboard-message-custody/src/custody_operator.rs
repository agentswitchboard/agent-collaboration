use central_link_client::{ConnectionAttempt, LinkLocalFault, UntouchedCommand};
use switchboard_machine_core_store::{RowCorruption, StoreScan};

/// A diagnostic custody reports to the operator independently of any session's status.
#[derive(Debug, Eq, PartialEq)]
pub enum CustodyOperatorEvent {
    CorruptOutboxRow(RowCorruption),
    CorruptOutboxPoint {
        request_id: String,
        field: &'static str,
    },
    CorruptInboxRow(RowCorruption),
    UntouchedBatchFault(UntouchedCommand),
    LinkFaulted {
        connection: ConnectionAttempt,
        cause: LinkLocalFault,
    },
    TerminalProbeReapDeferred {
        process_id: u32,
    },
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
