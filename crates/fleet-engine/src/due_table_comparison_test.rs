use super::{LegacyTime, RetiredItem, StoredAlarmInputs, StoredSource};

#[test]
fn diagnostic_sql_describes_the_rows_read_on_the_owning_transaction() {
    macro_rules! check {
        ($row:ident, $file:literal) => {
            sqlx::query_file_as!($row, $file)
        };
    }
    let _ = comparison_queries!(check);
}

use super::{DueTableDifference, compare_due_table};
use crate::{
    DueEntryKey, DueItemKey, DueKind, DueOwnerTiming, DueTableChange, apply_due_table_change,
};
use central_engine_model::{AlarmInputs, DueBatchPolicy, EngineTime, InstanceKey};
use fleet_lent_transaction::CentralTransaction;
use sqlx::{Connection, Sqlite, SqliteConnection, sqlite::SqliteQueryResult};

struct LendingSqlite<'connection>(&'connection mut SqliteConnection);

impl CentralTransaction for LendingSqlite<'_> {
    type Database = Sqlite;

    fn executor(&mut self) -> impl sqlx::Executor<'_, Database = Self::Database> {
        &mut *self.0
    }

    fn rows_affected(result: SqliteQueryResult) -> u64 {
        result.rows_affected()
    }
}

const SUBJECT: &str = "019f0000-0000-7000-8000-000000000001";
const AT: &str = "1970-01-01T00:00:00.000Z";

#[tokio::test]
async fn due_comparison_identifies_missing_wrong_stale_and_out_of_band_rows() {
    let mut connection = SqliteConnection::connect("sqlite::memory:").await.unwrap();
    fleet_storage_schema::workspace_migrations::central_migrator()
        .run(&mut connection)
        .await
        .unwrap();
    sqlx::query("INSERT INTO signin_organization_identity VALUES ('019f0000-0000-7000-8000-000000000002','fixture',1,1)")
        .execute(&mut connection).await.unwrap();
    let mut tx = connection.begin().await.unwrap();
    sqlx::query("INSERT INTO automation_wakes VALUES (?1,'target','1970-01-01T00:05:00.000Z','wake','active',?2,?2)")
        .bind(SUBJECT).bind(AT).execute(&mut *tx).await.unwrap();
    let epoch = EngineTime::UNIX_EPOCH;
    let due = EngineTime::from_timestamp_millis(300_000).unwrap();
    let policy = DueBatchPolicy::DEFAULT;
    let alarm = AlarmInputs {
        now: epoch,
        last_batch_start: None,
        leftovers_beyond_cap: false,
        earliest_on_grid_due: None,
        earliest_legacy_due: None,
    };
    let differences = compare_due_table(&mut LendingSqlite(&mut tx), due, 1, alarm, &policy)
        .await
        .unwrap();
    assert!(differences.iter().any(|difference| matches!(difference,
        DueTableDifference::MissingItem { kind: DueKind::WakeTimer, reference } if reference.key.0 == SUBJECT
    )));
    assert!(
        differences
            .iter()
            .any(|difference| matches!(difference, DueTableDifference::WrongAlarmDecision { .. }))
    );

    let key = DueItemKey {
        kind: DueKind::WakeTimer,
        subject: InstanceKey(SUBJECT.to_owned()),
    };
    let change = DueTableChange::PublishItem(DueOwnerTiming::at(key.clone(), due).unwrap());
    apply_due_table_change(&mut LendingSqlite(&mut tx), &change, epoch)
        .await
        .unwrap();
    assert!(
        compare_due_table(&mut LendingSqlite(&mut tx), due, 1, alarm, &policy)
            .await
            .unwrap()
            .is_empty()
    );
    let id: String = sqlx::query_scalar("SELECT due_entry_id FROM engine_due_entries")
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert_eq!(uuid::Uuid::parse_str(&id).unwrap().get_version_num(), 7);

    let changed = EngineTime::from_timestamp_millis(600_000).unwrap();
    let change = DueTableChange::PublishItem(DueOwnerTiming::at(key.clone(), changed).unwrap());
    apply_due_table_change(&mut LendingSqlite(&mut tx), &change, epoch)
        .await
        .unwrap();
    let id_after: String = sqlx::query_scalar("SELECT due_entry_id FROM engine_due_entries")
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert_eq!(id, id_after);
    let differences = compare_due_table(&mut LendingSqlite(&mut tx), due, 1, alarm, &policy)
        .await
        .unwrap();
    assert!(differences.iter().any(|difference| matches!(difference,
        DueTableDifference::WrongItemTime { kind: DueKind::WakeTimer, reference, projected }
            if reference.occurrence_ms == 300_000 && projected.occurrence_ms == 600_000
    )));
    assert!(
        differences
            .iter()
            .any(|difference| matches!(difference, DueTableDifference::WrongDispatch { .. }))
    );

    sqlx::query("UPDATE engine_due_entries SET effective_due_ms=123456 WHERE subject_key=?1")
        .bind(SUBJECT)
        .execute(&mut *tx)
        .await
        .unwrap();
    let differences = compare_due_table(&mut LendingSqlite(&mut tx), due, 1, alarm, &policy)
        .await
        .unwrap();
    assert!(differences.iter().any(|difference| matches!(difference,
        DueTableDifference::WrongItemTime { kind: DueKind::WakeTimer, projected, .. }
            if projected.effective_due_ms == 123456
    )));

    sqlx::query("DELETE FROM automation_wakes WHERE wake_id=?1")
        .bind(SUBJECT)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("INSERT INTO mail_notification_windows VALUES ('019f0000-0000-7000-8000-000000000003','recipient',?1,'not-canonical',?1,?1)")
        .bind(AT).execute(&mut *tx).await.unwrap();
    let differences = compare_due_table(&mut LendingSqlite(&mut tx), due, 1, alarm, &policy)
        .await
        .unwrap();
    assert!(differences.iter().any(|difference| matches!(difference,
        DueTableDifference::ExtraItem { kind: DueKind::WakeTimer, projected } if projected.key.0 == SUBJECT
    )));
    assert!(differences.iter().any(|difference| matches!(difference,
        DueTableDifference::NonCanonicalLegacyTime { kind: DueKind::LegacyNoticeWindows, key, stored_time }
            if key.ends_with("000003") && stored_time == "not-canonical"
    )));
    sqlx::query("INSERT INTO automation_wakes VALUES (?1,'target','1970-01-01T00:05:00.000Z','wake','active',?2,?2)")
        .bind(SUBJECT).bind(AT).execute(&mut *tx).await.unwrap();
    sqlx::query("INSERT INTO engine_work_attempts VALUES ('019f0000-0000-7000-8000-000000000004','wake_timer',?1,'retired',4,NULL,'refused',NULL,?2,?2,?2,?2,?2)")
        .bind(SUBJECT).bind(AT).execute(&mut *tx).await.unwrap();
    let differences = compare_due_table(&mut LendingSqlite(&mut tx), due, 1, alarm, &policy)
        .await
        .unwrap();
    assert!(differences.iter().any(|difference| matches!(difference,
        DueTableDifference::RetiredItemMismatch { key, reference: true, projected: false, .. }
            if key == SUBJECT
    )));
    let change = DueTableChange::RefreshAttempt(DueEntryKey::Item(key));
    apply_due_table_change(&mut LendingSqlite(&mut tx), &change, epoch)
        .await
        .unwrap();
    let differences = compare_due_table(&mut LendingSqlite(&mut tx), due, 1, alarm, &policy)
        .await
        .unwrap();
    assert!(
        !differences
            .iter()
            .any(|difference| matches!(difference, DueTableDifference::RetiredItemMismatch { .. }))
    );
    tx.rollback().await.unwrap();
    assert!(
        compare_due_table(&mut LendingSqlite(&mut connection), due, 1, alarm, &policy)
            .await
            .unwrap()
            .is_empty()
    );
}
