use collaboration_protocol::{
    ConversationBindingIdentity, OperationId, ProviderIdentity, ProviderOperationEffect,
    ProviderOperationKind, ProviderOperationStage, ProviderPromptStopReason,
    ProviderReconciliationState, ProviderRequestedPolicy, RouterAccess, SessionRef,
};
use serde_json::json;
use sqlx::{Connection, SqliteConnection, sqlite::SqliteConnectOptions};
use std::{fs, os::unix::fs::MetadataExt as _, path::Path};
use switchboard_session_store::{
    ProviderOperationRecord, ProviderOperationStore, ProviderSessionRecord,
    ProviderSessionResumeInputs,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const HISTORICAL_DATABASE: &[u8] =
    include_bytes!("../fixtures/c172f363b-provider-operations.sqlite");
const SERVICE_ID: &str = "019abcde-f001-7a23-8b45-6789abcdef20";
const MIGRATION_VERSIONS: [i64; 5] = [
    202609200001,
    202609240001,
    202609240002,
    202609250001,
    202611170001,
];

#[derive(Debug, Eq, PartialEq, sqlx::FromRow)]
struct AppliedMigration {
    version: i64,
    description: String,
    installed_on: String,
    success: bool,
    checksum: Vec<u8>,
    execution_time: i64,
}

async fn applied_migrations(path: &Path) -> TestResult<Vec<AppliedMigration>> {
    let mut connection = SqliteConnection::connect_with(
        &SqliteConnectOptions::new()
            .filename(path)
            .read_only(true)
            .immutable(true),
    )
    .await?;
    let history = sqlx::query_as(
        "SELECT version, description, installed_on, success, checksum, execution_time
         FROM _sqlx_migrations ORDER BY version",
    )
    .fetch_all(&mut connection)
    .await?;
    connection.close().await?;
    Ok(history)
}

fn session_ref(endpoint_id: &str, session_id: &str) -> TestResult<SessionRef> {
    Ok(serde_json::from_value(json!({
        "endpoint": { "endpointId": endpoint_id, "serviceId": SERVICE_ID },
        "sessionId": session_id,
    }))?)
}

fn expected_sessions() -> TestResult<[ProviderSessionRecord; 2]> {
    let policy = ProviderRequestedPolicy {
        access: RouterAccess::WriteRestricted,
    };
    Ok([
        ProviderSessionRecord {
            target: session_ref("claude-local", "saved-with-setup")?,
            working_directory: "/v5-historical/project-a".to_owned().try_into()?,
            requested_policy: policy.clone(),
            created_by: session_ref("codex-local", "historical-creator")?.into(),
            approver: serde_json::from_value::<ProviderIdentity>(
                json!({ "humanId": "historical-approver" }),
            )?,
            updated_at_ms: 1_795_000_000_010,
            resume: ProviderSessionResumeInputs {
                requested_mode: Some("auto".into()),
                work_roots: Some(vec![
                    "/v5-historical/project-a".to_owned().try_into()?,
                    "/v5-historical/shared".to_owned().try_into()?,
                ]),
                scratch_folder: Some("/v5-historical/scratch/session-a".to_owned().try_into()?),
            },
        },
        ProviderSessionRecord {
            target: session_ref("claude-local", "saved-without-setup")?,
            working_directory: "/v5-historical/project-b".to_owned().try_into()?,
            requested_policy: policy,
            created_by: serde_json::from_value::<ProviderIdentity>(
                json!({ "humanId": "historical-creator" }),
            )?,
            approver: session_ref("codex-local", "historical-approver")?.into(),
            updated_at_ms: 1_795_000_000_044,
            resume: ProviderSessionResumeInputs {
                requested_mode: None,
                work_roots: None,
                scratch_folder: None,
            },
        },
    ])
}

fn expected_operation() -> TestResult<ProviderOperationRecord> {
    let target = session_ref("claude-local", "saved-with-setup")?;
    let binding: ConversationBindingIdentity = serde_json::from_value(json!({
        "kind": "externalProvider",
        "binding": {
            "endpoint": {
                "endpointId": "claude-local",
                "serviceId": SERVICE_ID,
            },
            "bindingId": "historical-claude-binding",
            "runtime": {
                "provider": "claudeCode",
                "runtimeName": "historical-provider",
                "runtimeVersion": "2.4.1",
            },
            "transport": "stdioAcp",
            "generation": {
                "serviceEpoch": "019abcde-f001-7a23-8b45-6789abcdef21",
                "generation": 7,
            },
            "capabilities": [{
                "name": "prompt",
                "status": "supported",
                "evidence": "advertised",
            }],
        },
    }))?;
    Ok(ProviderOperationRecord {
        operation_id: OperationId::try_from("019abcde-f001-7a23-8b45-6789abcdef22".to_owned())?,
        operation_kind: ProviderOperationKind::ConversationPrompt,
        binding,
        target: Some(target),
        stage: ProviderOperationStage::Terminal,
        effect: ProviderOperationEffect::Applied,
        reconciliation_state: ProviderReconciliationState::Confirmed,
        admitted_at_ms: 1_795_000_000_011,
        dispatched_at_ms: Some(1_795_000_000_022),
        terminal_at_ms: Some(1_795_000_000_033),
        terminal_stop_reason: Some(ProviderPromptStopReason::MaxTokens),
        updated_at_ms: 1_795_000_000_033,
    })
}

#[tokio::test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "Comparison failures show the changed historic row or migration field."
)]
async fn keeps_all_applied_migrations_and_typed_records() -> TestResult {
    let root = tempfile::tempdir()?;
    let path = root.path().join("provider-operations.sqlite");
    fs::write(&path, HISTORICAL_DATABASE)?;
    let existing_file = fs::metadata(&path)?;
    let before = applied_migrations(&path).await?;
    let expected_operation = expected_operation()?;
    let [with_setup, without_setup] = expected_sessions()?;
    assert_eq!(
        before
            .iter()
            .map(|migration| migration.version)
            .collect::<Vec<_>>(),
        MIGRATION_VERSIONS
    );
    assert!(before.iter().all(|migration| migration.success));

    let mut store = ProviderOperationStore::open(&path).await?;
    let opened_operation = store.inspect(&expected_operation.operation_id).await?;
    let opened_with_setup = store.session_record(&with_setup.target).await?;
    let opened_without_setup = store.session_record(&without_setup.target).await?;
    let inventory = store.list_sessions(&with_setup.target.endpoint).await?;
    store.close().await?;
    let reopened_file = fs::metadata(&path)?;
    let after = applied_migrations(&path).await?;

    assert_eq!(
        (reopened_file.dev(), reopened_file.ino()),
        (existing_file.dev(), existing_file.ino()),
        "the store must open the existing file",
    );
    assert_eq!(after.len(), before.len(), "zero migrations applied");
    assert_eq!(after, before, "all six migration history columns");
    assert_eq!(opened_operation.as_ref(), Some(&expected_operation));
    assert_eq!(opened_with_setup.as_ref(), Some(&with_setup));
    assert_eq!(opened_without_setup.as_ref(), Some(&without_setup));
    assert_eq!(inventory, vec![without_setup, with_setup]);
    Ok(())
}
