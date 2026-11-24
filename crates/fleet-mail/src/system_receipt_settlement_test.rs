use super::*;

#[test]
fn system_receipt_files_fit_the_owning_schema() -> Result<(), Box<dyn std::error::Error>> {
    let message: MessageId = "019f0000-0000-7000-8000-000000000011".parse()?;
    let target: MachineId = "019f0000-0000-7000-8000-000000000012".parse()?;
    let now: Timestamp = "2030-01-01T00:00:00Z".parse()?;
    macro_rules! check_system_read {
        ($file:literal, $($arg:expr),+) => { sqlx::query_file_as!(StoredSystemState, $file, $($arg),+) };
    }
    macro_rules! check_system_update {
        ($file:literal, $($arg:expr),+) => { sqlx::query_file!($file, $($arg),+) };
    }
    let kind = SystemReceiptKind::Notice.storage_tag();
    let _ = system_state_query!(check_system_read, message, target, kind);
    let _ = system_update_query!(
        check_system_update,
        message,
        target,
        kind,
        "delivered",
        None::<String>,
        now
    );
    Ok(())
}

#[test]
fn stored_system_settlement_checks_refusal_and_cancelled_shapes() {
    use SystemReceiptKind::{Automation, DeliveryOutcome, Notice, Wake};

    let decode = |kind, state: &str, reason: Option<&str>| {
        StoredSystemState {
            state: state.to_owned(),
            failure_reason: reason.map(str::to_owned),
        }
        .decode_settlement(kind)
    };
    for kind in [Notice, DeliveryOutcome, Wake, Automation] {
        assert!(matches!(
            decode(kind, "queued", None),
            Ok(SystemSettlement::Queued)
        ));
        assert!(matches!(
            decode(kind, "delivered", None),
            Ok(SystemSettlement::Delivered)
        ));
        assert!(matches!(
            decode(kind, "cancelled", None),
            Ok(SystemSettlement::Cancelled)
        ));
        for (text, refusal) in [
            ("unknownSession", MessageRefusal::UnknownSession),
            ("ambiguousSession", MessageRefusal::AmbiguousSession),
            ("ambiguousSession", MessageRefusal::AmbiguousSession),
        ] {
            assert!(
                matches!(decode(kind, "failed", Some(text)), Ok(SystemSettlement::Refused(stored)) if stored == refusal)
            );
        }
        for state in ["queued", "delivered", "failed", "cancelled"] {
            assert!(decode(kind, state, Some("")).is_err());
            assert!(decode(kind, state, Some("unrecognized")).is_err());
        }
        assert!(decode(kind, "failed", None).is_err());
    }
    for kind in [Notice, DeliveryOutcome] {
        assert!(matches!(
            decode(kind, "cancelled", Some(EXPIRED_24_HOURS)),
            Ok(SystemSettlement::Cancelled)
        ));
    }
    for kind in [Wake, Automation] {
        assert!(decode(kind, "cancelled", Some(EXPIRED_24_HOURS)).is_err());
    }
}
