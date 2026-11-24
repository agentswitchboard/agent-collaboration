use sqlx::{Connection, SqliteConnection, sqlite::SqliteConnectOptions};
use state_machine_core::{
    TransitionCheckError, check_declared_transitions, check_state_machine_transitions,
};
use switchboard_machine_core_store::{MachineLocalStore, StoredEnrollmentEnd};
use switchboard_machine_enrollment::{EnrollmentKey, InvalidReason, MachineId, SignInDeployment};

use super::*;
use crate::enrollment_row_mapping::enrollment_metadata;

const MACHINE: &str = "019a0000-0000-7000-8000-000000000003";
const ORGANIZATION: &str = "019a0000-0000-7000-8000-000000000001";
const ENROLLMENT: &str = "019a0000-0000-7000-8000-000000000002";
const REENROLLMENT: &str = "019a0000-0000-7000-8000-000000000004";
const DEPLOYMENT: &str = "https://signin.example/";
const REVOKED_JSON: &str = r#"{"kind":"revoked"}"#;

/// Stored reason objects at 0cbd66054. Keep these independent of the codec.
const ENDING_JSON: [(EnrollmentEnd, &str); 16] = [
    (EnrollmentEnd::Denied, r#"{"kind":"denied"}"#),
    (EnrollmentEnd::Expired, r#"{"kind":"expired"}"#),
    (
        EnrollmentEnd::RefreshRefused,
        r#"{"kind":"refreshRefused"}"#,
    ),
    (
        EnrollmentEnd::RefreshClientDisabled,
        r#"{"kind":"refreshClientDisabled"}"#,
    ),
    (
        EnrollmentEnd::EndedByCentral,
        r#"{"kind":"endedByCentral"}"#,
    ),
    (
        EnrollmentEnd::LocalCredentialLost,
        r#"{"kind":"localCredentialLost"}"#,
    ),
    (EnrollmentEnd::Revoked, REVOKED_JSON),
    (EnrollmentEnd::Inactive, r#"{"kind":"inactive"}"#),
    (
        EnrollmentEnd::Invalid(InvalidReason::GrantRefused),
        r#"{"kind":"invalid","reason":"grantRefused"}"#,
    ),
    (
        EnrollmentEnd::Invalid(InvalidReason::InvalidClient),
        r#"{"kind":"invalid","reason":"invalidClient"}"#,
    ),
    (
        EnrollmentEnd::Invalid(InvalidReason::InvalidRequest),
        r#"{"kind":"invalid","reason":"invalidRequest"}"#,
    ),
    (
        EnrollmentEnd::Invalid(InvalidReason::UnknownError),
        r#"{"kind":"invalid","reason":"unknownError"}"#,
    ),
    (
        EnrollmentEnd::Invalid(InvalidReason::MalformedAnswer),
        r#"{"kind":"invalid","reason":"malformedAnswer"}"#,
    ),
    (
        EnrollmentEnd::Invalid(InvalidReason::UnexpectedStatus),
        r#"{"kind":"invalid","reason":"unexpectedStatus"}"#,
    ),
    (
        EnrollmentEnd::Invalid(InvalidReason::WrongEnrollment),
        r#"{"kind":"invalid","reason":"wrongEnrollment"}"#,
    ),
    (
        EnrollmentEnd::Invalid(InvalidReason::OversizedAnswer),
        r#"{"kind":"invalid","reason":"oversizedAnswer"}"#,
    ),
];

fn context() -> EnrollmentContext {
    EnrollmentContext {
        machine_id: MACHINE.parse().unwrap(),
        organization_id: ORGANIZATION.parse().unwrap(),
        deployment: SignInDeployment::new(DEPLOYMENT.parse().unwrap()).unwrap(),
    }
}

fn binding_generation(generation: i64) -> BindingGeneration {
    BindingGeneration::from_persisted(u64::try_from(generation).unwrap()).unwrap()
}

fn active(generation: i64, enrollment: &str) -> StoredEnrollmentRow {
    let mut sealed = vec![1];
    sealed.resize(30, 42);
    StoredEnrollmentRow::Active {
        generation,
        organization_id: ORGANIZATION.into(),
        deployment: DEPLOYMENT.into(),
        enrollment_id: enrollment.into(),
        sealed,
    }
}

fn ended(generation: i64) -> StoredEnrollmentRow {
    StoredEnrollmentRow::Ended {
        generation,
        organization_id: ORGANIZATION.into(),
        deployment: DEPLOYMENT.into(),
        enrollment_id: ENROLLMENT.into(),
        why: StoredEnrollmentEnd::Revoked {},
    }
}

#[test]
fn declared_table_covers_each_reachable_edge_and_detects_both_bad_tables() {
    let context = context();
    let empty = EnrollmentSnapshot::Empty;
    let active_snapshot =
        EnrollmentSnapshot::from_row(active(1, ENROLLMENT), &context.machine_id).unwrap();
    let ended_snapshot = EnrollmentSnapshot::from_row(ended(2), &context.machine_id).unwrap();
    let active_metadata = EnrollmentMetadata::Active {
        key: EnrollmentKey {
            organization_id: context.organization_id,
            machine_id: context.machine_id.clone(),
            enrollment_id: ENROLLMENT.parse().unwrap(),
        },
        deployment: context.deployment.clone(),
        generation: binding_generation(1),
    };
    let end = EnrollmentRowEvent::End {
        expected: binding_generation(1),
        locked: &active_metadata,
        why: EnrollmentEnd::Revoked,
    };
    let cases = || {
        [&empty, &active_snapshot, &ended_snapshot]
            .into_iter()
            .enumerate()
            .map(|(generation, locked)| {
                let generation = i64::try_from(generation).unwrap();
                let event = EnrollmentRowEvent::Replace {
                    expected: binding_generation(generation),
                    locked,
                    context: &context,
                    prepared: Some(Box::new(PreparedReplacement {
                        witness: locked.clone(),
                        row: active(generation + 1, ENROLLMENT),
                    })),
                };
                (event.phase(), event)
            })
            .chain([(end.phase(), end.clone())])
    };
    check_state_machine_transitions::<EnrollmentRowMachine<'_>>(cases()).unwrap();

    let declared = EnrollmentRowMachine::transitions();
    let omitted = Transition {
        from: "active",
        event: "end",
        to: "ended",
        terminal: false,
    };
    let mut missing = declared.to_vec();
    missing.retain(|edge| *edge != omitted);
    let apply = |state: &EnrollmentRowPhase, event| {
        EnrollmentRowMachine::transition(state, event)
            .ok()
            .map(|(next, _)| next)
    };
    assert_eq!(
        check_declared_transitions(cases(), &missing, apply),
        Err(TransitionCheckError::Undeclared(omitted))
    );

    let unwitnessed = Transition {
        from: "empty",
        event: "end",
        to: "ended",
        terminal: false,
    };
    let mut extra = declared.to_vec();
    extra.push(unwitnessed);
    assert_eq!(
        check_declared_transitions(cases(), &extra, apply),
        Err(TransitionCheckError::Unreachable(unwitnessed))
    );
}

const NOW: i64 = 1_795_000_000_123;
type RowColumns = (String, i64, i64, i64, Option<Vec<u8>>, Option<Vec<u8>>);

async fn bytes(database: &std::path::Path) -> RowColumns {
    let mut read = SqliteConnection::connect_with(
        &SqliteConnectOptions::new()
            .filename(database)
            .read_only(true),
    )
    .await
    .unwrap();
    let row = sqlx::query_as("SELECT machine_id, created_at, updated_at, enrollment_generation, CAST(enrollment_json AS BLOB), enrollment_sealed FROM machine_identity")
        .fetch_one(&mut read).await.unwrap();
    read.close().await.unwrap();
    row
}

async fn seeded(database: &std::path::Path) -> MachineLocalStore {
    let store = MachineLocalStore::open(database).await.unwrap();
    let mut seed = SqliteConnection::connect_with(&SqliteConnectOptions::new().filename(database))
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO machine_identity (machine_id, created_at, updated_at) VALUES (?, ?, ?)",
    )
    .bind(MACHINE)
    .bind(NOW)
    .bind(NOW)
    .execute(&mut seed)
    .await
    .unwrap();
    seed.close().await.unwrap();
    store
}

enum Write {
    Replace {
        expected: i64,
        enrollment: &'static str,
    },
    End {
        expected: i64,
        why: EnrollmentEnd,
    },
}

impl Write {
    fn expected(&self) -> i64 {
        match self {
            Self::Replace { expected, .. } | Self::End { expected, .. } => *expected,
        }
    }
}

fn main_replace(
    locked: Result<EnrollmentSnapshot, EnrollmentRowCorruption>,
    context: &EnrollmentContext,
    prepared: Option<Box<PreparedReplacement>>,
) -> EnrollmentRowChange<CredentialStoreError> {
    // The locked writer at 0cbd66054 checks scope, then the complete witness.
    let locked = match locked {
        Ok(locked) => locked,
        Err(reason) => return EnrollmentRowChange::Corrupt(reason),
    };
    let in_scope = match &locked {
        EnrollmentSnapshot::Empty => true,
        EnrollmentSnapshot::Active {
            key, deployment, ..
        }
        | EnrollmentSnapshot::Ended {
            key, deployment, ..
        } => key.organization_id == context.organization_id && *deployment == context.deployment,
    };
    if !in_scope {
        return EnrollmentRowChange::Abort(CredentialStoreError::EnrollmentScopeMismatch);
    }
    match prepared {
        Some(replacement) if locked == replacement.witness => {
            EnrollmentRowChange::Write(replacement.row)
        }
        _ => EnrollmentRowChange::Abort(CredentialStoreError::Replace),
    }
}

fn main_end(
    locked: Result<EnrollmentMetadata, EnrollmentRowCorruption>,
    why: EnrollmentEnd,
) -> EnrollmentRowChange<CredentialStoreError> {
    // The metadata-only writer at 0cbd66054 builds the ending without opening the sealed field.
    let locked = match locked {
        Ok(locked) => locked,
        Err(reason) => return EnrollmentRowChange::Corrupt(reason),
    };
    let EnrollmentMetadata::Active {
        key,
        deployment,
        generation,
    } = locked
    else {
        return EnrollmentRowChange::Abort(CredentialStoreError::Remove);
    };
    let Some(generation) = generation
        .successor()
        .and_then(|next| i64::try_from(next.get()).ok())
    else {
        return EnrollmentRowChange::Abort(CredentialStoreError::Remove);
    };
    EnrollmentRowChange::Write(StoredEnrollmentRow::Ended {
        generation,
        organization_id: key.organization_id.to_string(),
        deployment: deployment.origin().as_str().to_owned(),
        enrollment_id: key.enrollment_id.to_string(),
        why: stored_end(why),
    })
}

async fn apply_write(
    store: &mut MachineLocalStore,
    context: &EnrollmentContext,
    operation: &Write,
    machine: bool,
    now: i64,
) -> Result<
    switchboard_machine_core_store::EnrollmentRowWrite<CredentialStoreError>,
    switchboard_machine_core_store::EnrollmentRowStoreError,
> {
    let machine_id: &MachineId = &context.machine_id;
    let expected = operation.expected();
    match operation {
        Write::Replace { enrollment, .. } => {
            let witness = EnrollmentSnapshot::from_row(
                store.read_enrollment_row(machine_id).await?,
                machine_id,
            )?;
            let prepared = PreparedReplacement {
                witness,
                row: active(expected + 1, enrollment),
            };
            store
                .write_enrollment_row(machine_id, expected, now, |row| {
                    let locked = EnrollmentSnapshot::from_row(row, machine_id);
                    if machine {
                        replace_locked(
                            locked,
                            binding_generation(expected),
                            context,
                            Some(Box::new(prepared)),
                        )
                    } else {
                        main_replace(locked, context, Some(Box::new(prepared)))
                    }
                })
                .await
        }
        Write::End { why, .. } => {
            store
                .write_enrollment_ending(machine_id, expected, now, |row| {
                    let locked = enrollment_metadata(row, machine_id);
                    if machine {
                        end_locked(locked, binding_generation(expected), *why)
                    } else {
                        main_end(locked, *why)
                    }
                })
                .await
        }
    }
}

#[tokio::test]
async fn each_machine_write_matches_main_columns_and_json_bytes() {
    use switchboard_machine_core_store::EnrollmentRowWrite;

    let directory = tempfile::tempdir().unwrap();
    let actual_database = directory.path().join("machine.sqlite");
    let reference_database = directory.path().join("main.sqlite");
    let mut actual = seeded(&actual_database).await;
    let mut reference = seeded(&reference_database).await;
    let context = context();
    assert_eq!(
        bytes(&actual_database).await,
        (MACHINE.into(), NOW, NOW, 0, None, None)
    );

    let mut now = NOW;
    for (operation, enrollment, state) in [
        (
            Write::Replace {
                expected: 0,
                enrollment: ENROLLMENT,
            },
            ENROLLMENT,
            "active",
        ),
        (
            Write::Replace {
                expected: 1,
                enrollment: ENROLLMENT,
            },
            ENROLLMENT,
            "active",
        ),
        (
            Write::End {
                expected: 2,
                why: EnrollmentEnd::Revoked,
            },
            ENROLLMENT,
            "ended",
        ),
        (
            Write::Replace {
                expected: 3,
                enrollment: REENROLLMENT,
            },
            REENROLLMENT,
            "active",
        ),
    ] {
        now += 1;
        assert!(matches!(
            apply_write(&mut reference, &context, &operation, false, now).await,
            Ok(EnrollmentRowWrite::Applied)
        ));
        assert!(matches!(
            apply_write(&mut actual, &context, &operation, true, now).await,
            Ok(EnrollmentRowWrite::Applied)
        ));
        let row = bytes(&actual_database).await;
        assert_eq!(row, bytes(&reference_database).await);
        assert_eq!(row.3, operation.expected() + 1);
        assert_json(
            &row,
            state,
            enrollment,
            (state == "ended").then_some(REVOKED_JSON),
        );
    }

    // A valid active row may rotate into MAX-1, and its ending consumes MAX.
    for database in [&actual_database, &reference_database] {
        let mut seed =
            SqliteConnection::connect_with(&SqliteConnectOptions::new().filename(database))
                .await
                .unwrap();
        sqlx::query("UPDATE machine_identity SET enrollment_generation = ?")
            .bind(i64::MAX - 2)
            .execute(&mut seed)
            .await
            .unwrap();
        seed.close().await.unwrap();
    }
    for (operation, state) in [
        (
            Write::Replace {
                expected: i64::MAX - 2,
                enrollment: REENROLLMENT,
            },
            "active",
        ),
        (
            Write::End {
                expected: i64::MAX - 1,
                why: EnrollmentEnd::Revoked,
            },
            "ended",
        ),
    ] {
        now += 1;
        assert!(matches!(
            apply_write(&mut reference, &context, &operation, false, now).await,
            Ok(EnrollmentRowWrite::Applied)
        ));
        assert!(matches!(
            apply_write(&mut actual, &context, &operation, true, now).await,
            Ok(EnrollmentRowWrite::Applied)
        ));
        let row = bytes(&actual_database).await;
        assert_eq!(row, bytes(&reference_database).await);
        assert_eq!(row.3, operation.expected() + 1);
        assert_json(
            &row,
            state,
            REENROLLMENT,
            (state == "ended").then_some(REVOKED_JSON),
        );
    }
    let at_max = bytes(&actual_database).await;
    let repeated_end = Write::End {
        expected: i64::MAX,
        why: EnrollmentEnd::Revoked,
    };
    for (store, machine) in [(&mut actual, true), (&mut reference, false)] {
        assert!(matches!(
            apply_write(store, &context, &repeated_end, machine, now + 1).await,
            Ok(EnrollmentRowWrite::Aborted(CredentialStoreError::Remove))
        ));
    }
    assert_eq!(bytes(&actual_database).await, at_max);
    assert_eq!(bytes(&reference_database).await, at_max);
    actual.close().await.unwrap();
    reference.close().await.unwrap();
}

#[tokio::test]
async fn every_ending_reason_matches_main_columns_and_pinned_json_bytes() {
    use switchboard_machine_core_store::EnrollmentRowWrite;

    let directory = tempfile::tempdir().unwrap();
    let actual_database = directory.path().join("machine.sqlite");
    let reference_database = directory.path().join("main.sqlite");
    let mut actual = seeded(&actual_database).await;
    let mut reference = seeded(&reference_database).await;
    let context = context();

    for (index, (why, why_json)) in ENDING_JSON.into_iter().enumerate() {
        let expected = i64::try_from(index).unwrap() * 2;
        let replace = Write::Replace {
            expected,
            enrollment: ENROLLMENT,
        };
        let end = Write::End {
            expected: expected + 1,
            why,
        };
        for (store, machine) in [(&mut actual, true), (&mut reference, false)] {
            assert!(matches!(
                apply_write(store, &context, &replace, machine, NOW + expected + 1).await,
                Ok(EnrollmentRowWrite::Applied)
            ));
            assert!(
                matches!(
                    apply_write(store, &context, &end, machine, NOW + expected + 2).await,
                    Ok(EnrollmentRowWrite::Applied)
                ),
                "end {why:?}"
            );
        }
        let row = bytes(&actual_database).await;
        assert_eq!(row, bytes(&reference_database).await, "end {why:?}");
        assert_eq!(row.3, expected + 2);
        assert!(row.5.is_none());
        assert_json(&row, "ended", ENROLLMENT, Some(why_json));
    }
    actual.close().await.unwrap();
    reference.close().await.unwrap();
}

fn assert_json(row: &RowColumns, state: &str, enrollment: &str, why_json: Option<&str>) {
    let why = why_json.map_or_else(String::new, |json| format!(r#","why":{json}"#));
    let pinned = format!(
        r#"{{"state":"{state}","organizationId":"{ORGANIZATION}","deployment":"{DEPLOYMENT}","enrollmentId":"{enrollment}"{why}}}"#
    );
    assert_eq!(
        row.4.as_deref(),
        Some(pinned.as_bytes()),
        "pinned enrollment_json bytes"
    );
}
