use super::{CustodyOperatorEvent, RowCorruption, StoreScan, inbox_scan, outbox_scan};
use switchboard_machine_core_store::RowCorruptionReason;

#[test]
fn a_row_diagnostic_is_separate_from_the_healthy_work_and_names_its_store() {
    let row = || RowCorruption {
        key: "raw-damaged-key".to_owned(),
        field: "payload_json",
        reason: RowCorruptionReason::InvalidJson,
    };
    let mut events = Vec::new();
    let outbox = outbox_scan(
        StoreScan {
            healthy: ["write"],
            corrupt_rows: vec![row()],
        },
        &mut events,
    );
    let inbox = inbox_scan(
        StoreScan {
            healthy: ["receipt"],
            corrupt_rows: vec![row()],
        },
        &mut events,
    );

    assert_eq!(outbox, ["write"]);
    assert_eq!(inbox, ["receipt"]);
    assert_eq!(
        events,
        [
            CustodyOperatorEvent::CorruptOutboxRow(row()),
            CustodyOperatorEvent::CorruptInboxRow(row())
        ]
    );
}
