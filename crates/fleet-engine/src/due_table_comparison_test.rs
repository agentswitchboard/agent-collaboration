#![cfg(test)]

use super::{
    LegacyTime, OwnerItem, ReferenceLegacyInputs, StoredAlarmInputs, StoredProjection, StoredSource,
};

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
            if projected.effective_due_ms == Some(123456)
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
        DueTableDifference::WrongItemTime { reference, projected, .. }
            if reference.key.0 == SUBJECT && reference.effective_due_ms.is_none() && projected.effective_due_ms.is_some()
    )));
    let normal = DueTableChange::PublishItem(DueOwnerTiming::at(key.clone(), due).unwrap());
    apply_due_table_change(&mut LendingSqlite(&mut tx), &normal, epoch)
        .await
        .unwrap();
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
            .any(|difference| matches!(difference, DueTableDifference::WrongItemTime { .. }))
    );
    tx.rollback().await.unwrap();
    assert!(
        compare_due_table(&mut LendingSqlite(&mut connection), due, 1, alarm, &policy)
            .await
            .unwrap()
            .is_empty()
    );
}

const AT_5M: &str = "1970-01-01T00:05:00.000Z";
const STRAY: &str = "019f0000-0000-7000-8000-000000000008";

async fn migrated() -> SqliteConnection {
    let mut connection = SqliteConnection::connect("sqlite::memory:").await.unwrap();
    fleet_storage_schema::workspace_migrations::central_migrator()
        .run(&mut connection)
        .await
        .unwrap();
    sqlx::query("INSERT INTO signin_organization_identity VALUES ('019f0000-0000-7000-8000-000000000002','fixture',1,1)")
        .execute(&mut connection).await.unwrap();
    connection
}

async fn differences(connection: &mut SqliteConnection) -> Vec<DueTableDifference> {
    let horizon = EngineTime::from_timestamp_millis(900_000).unwrap();
    compare_due_table(
        &mut LendingSqlite(connection),
        horizon,
        1,
        AlarmInputs {
            now: EngineTime::UNIX_EPOCH,
            last_batch_start: None,
            leftovers_beyond_cap: false,
            earliest_on_grid_due: None,
            earliest_legacy_due: None,
        },
        &DueBatchPolicy::DEFAULT,
    )
    .await
    .unwrap()
}

async fn seed_wake(connection: &mut SqliteConnection, subject: &str, due: &str) -> DueItemKey {
    sqlx::query("INSERT INTO automation_wakes VALUES (?1,'target',?2,'wake','active',?3,?3)")
        .bind(subject)
        .bind(due)
        .bind(AT)
        .execute(&mut *connection)
        .await
        .unwrap();
    let key = DueItemKey {
        kind: DueKind::WakeTimer,
        subject: InstanceKey(subject.to_owned()),
    };
    apply_due_table_change(
        &mut LendingSqlite(connection),
        &DueTableChange::PublishItem(
            DueOwnerTiming::at(key.clone(), due.parse().unwrap()).unwrap(),
        ),
        EngineTime::UNIX_EPOCH,
    )
    .await
    .unwrap();
    key
}

async fn retire_wake(connection: &mut SqliteConnection, key: &DueItemKey) {
    sqlx::query("INSERT INTO engine_work_attempts VALUES ('019f0000-0000-7000-8000-000000000004','wake_timer',?1,'retired',4,NULL,'refused',NULL,?2,?2,?2,?2,?2)")
        .bind(&key.subject.0).bind(AT).execute(&mut *connection).await.unwrap();
    apply_due_table_change(
        &mut LendingSqlite(connection),
        &DueTableChange::RefreshAttempt(DueEntryKey::Item(key.clone())),
        EngineTime::UNIX_EPOCH,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn diagnostic_reports_every_invalid_projection_shape_and_a_hidden_base() {
    use super::InvalidDueEntry::{Kind, LegacyItem, Role, TimingRange, TimingShape};
    let mut connection = migrated().await;
    seed_wake(&mut connection, SUBJECT, AT_5M).await;
    seed_wake(&mut connection, STRAY, "1970-01-01T00:10:00.000Z").await;
    assert!(differences(&mut connection).await.is_empty());

    for (kind, role, key, occurrence, base, effective, reason) in [
        (
            "invented",
            "item",
            STRAY,
            Some(600_000_i64),
            Some(600_000_i64),
            Some(600_000_i64),
            Kind,
        ),
        (
            "wake_timer",
            "invented",
            STRAY,
            Some(600_000),
            Some(600_000),
            Some(600_000),
            Role,
        ),
        (
            "legacy_notice_windows",
            "item",
            STRAY,
            Some(600_000),
            Some(600_000),
            Some(600_000),
            LegacyItem,
        ),
        (
            "wake_timer",
            "item",
            "019f0000-0000-7000-8000-000000000009",
            Some(9_223_372_036_855),
            Some(9_223_372_036_855),
            Some(9_223_372_036_855),
            TimingRange,
        ),
        (
            "wake_timer",
            "source_control",
            "wake_timer",
            Some(600_000),
            Some(600_000),
            None,
            TimingShape,
        ),
    ] {
        let mut tx = connection.begin().await.unwrap();
        sqlx::query("INSERT INTO engine_due_entries VALUES ('019f0000-0000-7000-8000-000000000010',?1,?2,?3,?4,?5,?6,?7,?7)")
            .bind(kind).bind(role).bind(key).bind(occurrence).bind(base).bind(effective).bind(AT)
            .execute(&mut *tx).await.unwrap();
        let report = differences(&mut tx).await;
        assert!(
            report
                .iter()
                .any(|item| matches!(item, DueTableDifference::InvalidEntry {
            key: stored_key, reason: stored_reason, ..
        } if stored_key == key && *stored_reason == reason)),
            "{report:?}"
        );
        tx.rollback().await.unwrap();
    }

    let mut tx = connection.begin().await.unwrap();
    sqlx::query("UPDATE engine_due_entries SET base_due_ms = 600001 WHERE subject_key=?")
        .bind(STRAY)
        .execute(&mut *tx)
        .await
        .unwrap();
    let report = differences(&mut tx).await;
    assert!(report.iter().any(|item| matches!(item, DueTableDifference::WrongItemTime { reference, projected, .. }
        if reference.key.0 == STRAY && reference.base_due_ms == 600_000 && projected.base_due_ms == 600_001 &&
            reference.effective_due_ms == projected.effective_due_ms)), "{report:?}");
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn retired_item_owner_timing_is_compared_before_and_after_retry_restores_the_base() {
    let mut connection = migrated().await;
    let key = seed_wake(&mut connection, SUBJECT, AT_5M).await;
    retire_wake(&mut connection, &key).await;
    assert!(differences(&mut connection).await.is_empty());
    sqlx::query("UPDATE engine_due_entries SET occurrence_ms=300001, base_due_ms=300001 WHERE subject_key=?")
        .bind(SUBJECT).execute(&mut connection).await.unwrap();

    let report = differences(&mut connection).await;
    assert!(report.iter().any(|item| matches!(item, DueTableDifference::WrongItemTime { reference, projected, .. }
        if reference.key.0 == SUBJECT && reference.occurrence_ms == 300_000 && projected.occurrence_ms == 300_001 &&
            reference.base_due_ms == 300_000 && projected.base_due_ms == 300_001 &&
            reference.effective_due_ms.is_none() && projected.effective_due_ms.is_none()
    )), "{report:?}");

    sqlx::query("DELETE FROM engine_work_attempts WHERE machine_kind='wake_timer' AND subject_key=? AND state='retired'")
        .bind(SUBJECT).execute(&mut connection).await.unwrap();
    apply_due_table_change(
        &mut LendingSqlite(&mut connection),
        &DueTableChange::RefreshAttempt(DueEntryKey::Item(key)),
        EngineTime::UNIX_EPOCH,
    )
    .await
    .unwrap();
    let report = differences(&mut connection).await;
    assert!(report.iter().any(|item| matches!(item, DueTableDifference::WrongItemTime { reference, projected, .. }
        if reference.effective_due_ms == Some(300_000) && projected.effective_due_ms == Some(300_001)
    )), "{report:?}");
}

async fn legacy_contributors(window_due: &str, drain_due: &str, masked: Option<DueKind>) {
    let mut connection = migrated().await;
    sqlx::query("INSERT INTO mail_notification_windows VALUES ('019f0000-0000-7000-8000-000000000003','recipient',?1,?2,?1,?1)")
        .bind(AT).bind(window_due).execute(&mut connection).await.unwrap();
    sqlx::query("INSERT INTO directory_workspaces VALUES ('019f0000-0000-7000-8000-000000000006','workspace','','active',?1,?1)")
        .bind(AT).execute(&mut connection).await.unwrap();
    sqlx::query("INSERT INTO mail_workspace_drains VALUES ('019f0000-0000-7000-8000-000000000006',0,?1,?2,?2)")
        .bind(drain_due).bind(AT).execute(&mut connection).await.unwrap();
    if let Some(kind) = masked {
        sqlx::query("INSERT INTO engine_work_attempts VALUES ('019f0000-0000-7000-8000-000000000004',?1,'all','retired',4,NULL,'refused',NULL,?2,?2,?2,?2,?2)")
            .bind(kind.tag()).bind(AT).execute(&mut connection).await.unwrap();
        apply_due_table_change(
            &mut LendingSqlite(&mut connection),
            &DueTableChange::RefreshAttempt(DueEntryKey::SourceControl(kind)),
            EngineTime::UNIX_EPOCH,
        )
        .await
        .unwrap();
    }
    let report = differences(&mut connection).await;
    assert!(report.is_empty(), "{report:?}");
}

#[tokio::test]
async fn window_contribution_remains_visible_behind_an_earlier_drain() {
    legacy_contributors(AT_5M, AT, None).await;
}
#[tokio::test]
async fn drain_contribution_remains_visible_behind_an_earlier_window() {
    legacy_contributors(AT, AT_5M, None).await;
}
#[tokio::test]
async fn isolated_window_contribution_survives_a_null_drain_control() {
    legacy_contributors(AT_5M, AT, Some(DueKind::LegacyWorkspaceDrains)).await;
}
#[tokio::test]
async fn isolated_drain_contribution_survives_a_null_window_control() {
    legacy_contributors(AT, AT_5M, Some(DueKind::LegacyNoticeWindows)).await;
}
