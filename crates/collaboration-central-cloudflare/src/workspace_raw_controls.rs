//! Raw fixture state and physical alarm receipts for the owning Durable Object.
use super::{engine_due_migration_fixture, error};
use crate::workspace_central_object::{
    AlarmFailure, AlarmPolicy, CentralObject, OrganizationId, statement_store,
};
use crate::{store_wire::OwnedStatement, workspace_rpc_fixture};
use serde::{Deserialize, Serialize};
use worker::{Request, Response, Result};
const CLOCK_KEY: &str = "_workspace_test_clock_ms";
const RECEIPT_KEY: &str = "_workspace_test_alarm_receipt";
const ALARM_COUNT_KEY: &str = "_workspace_test_alarm_count";

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AlarmReceipt {
    organization: Option<OrganizationId>,
    initialized_cold: bool,
    alarm_before: Option<i64>,
    alarm_after: Option<i64>,
    result: AlarmReceiptResult,
}

#[derive(Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
enum AlarmReceiptResult {
    Applied {
        wakes_fired: usize,
        automations_fired: usize,
        windows_fired: usize,
        workspaces_drained: usize,
    },
    Error {
        message: String,
        failure: Option<central_storage::StoreFailure>,
    },
    Unprovisioned,
}

pub(in crate::workspace_central_object) async fn raw_control(
    object: &CentralObject,
    mut request: Request,
) -> Result<Response> {
    let storage = object.state.storage();
    match request.path().as_str() {
        "/test/raw-engine-due-migration" => {
            let command = request
                .json::<engine_due_migration_fixture::MigrationCommand>()
                .await?;
            if matches!(
                command,
                engine_due_migration_fixture::MigrationCommand::Reset
            ) {
                object
                    .state
                    .block_concurrency_while(async {
                        Err::<(), worker::Error>(error("due migration fixture reset"))
                    })
                    .await?;
                return Response::error("object did not reset", 500);
            }
            Response::from_json(
                &engine_due_migration_fixture::run(&object.connection, command).await,
            )
        }
        "/test/raw-legacy-workspace-call-count" => Response::from_json(
            &object
                .controls
                .legacy_workspace_calls
                .load(std::sync::atomic::Ordering::SeqCst),
        ),
        "/test/raw-workspace-storage-state" => {
            let workspace = request.json().await?;
            let state =
                workspace_rpc_fixture::workspace_storage_state(&object.env, workspace).await?;
            Response::from_json(&state)
        }
        "/test/raw-workspace-rpc-probe" => {
            workspace_rpc_fixture::probe(&object.env, request.json().await?).await
        }
        "/test/raw-release-deferred-due-work" => {
            if let Some(held) = object
                .controls
                .central_holds
                .borrow_mut()
                .remove(&super::Point::BeforeDeferredDueWork)
            {
                held.release();
            }
            Response::from_json(&true)
        }
        "/test/raw-reply-due-activity" => Response::from_json(&object.reply_due_activity.counts()),
        "/test/raw-alarm-count" => {
            Response::from_json(&storage.get::<u64>(ALARM_COUNT_KEY).await?.unwrap_or(0))
        }
        "/test/raw-dispatch-due" => {
            let now: i64 = request.json().await?;
            // The platform scheduler uses its own time. Advance the test's one
            // injected clock and dispatch only its actually retained deadline.
            // A missing, later or cleared alarm must never fabricate a callback.
            let due = storage.get_alarm().await?;
            if due.is_none_or(|due| due > now) {
                return Response::from_json(&false);
            }
            storage.put(CLOCK_KEY, &now).await?;
            storage.delete(RECEIPT_KEY).await?;
            object.clock.hold(now);
            storage.set_alarm(0_i64).await?;
            Response::from_json(&true)
        }
        "/test/raw-alarm-status" => {
            Response::from_json(&storage.get::<AlarmReceipt>(RECEIPT_KEY).await?)
        }
        "/test/raw-until-alarm" => {
            let (sender, receiver) = futures_channel::oneshot::channel();
            object.controls.alarm_waiters.borrow_mut().push(sender);
            if let Some(receipt) = storage.get::<AlarmReceipt>(RECEIPT_KEY).await? {
                return Response::from_json(&receipt);
            }
            receiver
                .await
                .map_err(|_| error("alarm receipt wait interrupted"))?;
            let receipt = storage
                .get::<AlarmReceipt>(RECEIPT_KEY)
                .await?
                .ok_or_else(|| error("alarm receipt missing after notification"))?;
            Response::from_json(&receipt)
        }
        "/test/raw-alarm-time" => Response::from_json(&storage.get_alarm().await?),
        "/test/raw-drop-alarm" => {
            storage.delete_alarm().await?;
            Response::from_json(&true)
        }
        "/test/raw-probe-alarm" => {
            let milliseconds: i64 = request.json().await?;
            let prior = storage.get_alarm().await?;
            let result: std::result::Result<Option<i64>, String> =
                match chrono::DateTime::from_timestamp_millis(milliseconds) {
                    Some(at) => match storage.set_alarm(at).await {
                        Ok(()) => storage
                            .get_alarm()
                            .await
                            .map_err(|failure| failure.to_string()),
                        Err(failure) => Err(failure.to_string()),
                    },
                    None => Err("timestamp cannot be represented".to_owned()),
                };
            match prior {
                Some(previous) => {
                    let at = chrono::DateTime::from_timestamp_millis(previous)
                        .ok_or_else(|| error("previous alarm timestamp cannot be represented"))?;
                    storage.set_alarm(at).await?;
                }
                None => storage.delete_alarm().await?,
            }
            Response::from_json(&result)
        }
        "/test/raw-read" => {
            let statement: OwnedStatement = request.json().await?;
            if ![
                "SELECT (SELECT COUNT(*) FROM automation_wakes WHERE state='active'), (SELECT COUNT(*) FROM automation_wakes WHERE state='done'), (SELECT COUNT(*) FROM mail_messages WHERE message_kind='wake')",
                "SELECT id,organization_statement,created_at,updated_at FROM signin_organization_identity ORDER BY id",
                "SELECT * FROM signin_change_records ORDER BY id",
            ].contains(&statement.sql.as_str())
                || !statement.args.is_empty()
            {
                return Err(error(
                    "raw read only permits named corruption snapshots",
                ));
            }
            Response::from_json(&statement_store::read(&object.connection, statement).await)
        }
        "/test/raw-clear-alarm-receipt" => Response::from_json(&storage.delete(RECEIPT_KEY).await?),
        "/test/raw-set-stamps" => {
            let rows: Vec<(String, String)> = request.json().await?;
            if rows.len() > 2 {
                return Err(error("at most two fixture stamps"));
            }
            let mut statements = vec![central_storage::GuardedStatement {
                statement: central_storage::Statement {
                    sql: "DELETE FROM signin_organization_identity",
                    args: vec![],
                },
                expect: central_storage::RowExpectation::Any,
            }];
            for (id, statement) in rows {
                statements.push(central_storage::GuardedStatement {
                    statement: central_storage::Statement { sql: "INSERT INTO signin_organization_identity(id,organization_statement,created_at,updated_at) VALUES (?,?,0,0)", args: vec![central_storage::SqlValue::Text(id), central_storage::SqlValue::Text(statement)] },
                    expect: central_storage::RowExpectation::Exactly(1),
                });
            }
            let batch = central_storage::StatementBatch {
                statements,
                alarm: central_storage::AlarmUpdate::Unchanged,
            };
            let result =
                statement_store::run_batch(&object.connection, batch.into(), AlarmPolicy::Ignore)
                    .await;
            Response::from_json(&result)
        }
        "/test/raw-arm-alarm" | "/test/raw-arm-cold-alarm" => {
            let cold = request.path() == "/test/raw-arm-cold-alarm";
            let now: i64 = request.json().await?;
            storage.put(CLOCK_KEY, &now).await?;
            storage.delete(RECEIPT_KEY).await?;
            object.clock.hold(now);
            storage.set_alarm(0_i64).await?;
            if cold {
                object
                    .state
                    .block_concurrency_while(async {
                        Err::<(), worker::Error>(error("cold alarm fixture reset"))
                    })
                    .await?;
                return Response::error("object did not reset", 500);
            }
            Response::from_json(&true)
        }
        _ => Response::error("unknown raw control", 404),
    }
}

pub(in crate::workspace_central_object) async fn alarm_with_receipt(
    object: &CentralObject,
) -> Result<Response> {
    let storage = object.state.storage();
    let count = storage.get::<u64>(ALARM_COUNT_KEY).await?.unwrap_or(0);
    storage
        .put(ALARM_COUNT_KEY, &count.saturating_add(1))
        .await?;
    let alarm_before = storage.get_alarm().await?;
    if let Some(now) = storage.get::<i64>(CLOCK_KEY).await? {
        object.clock.hold(now);
    }
    let initialized_cold = object.runtime.lock().await.is_none();
    let mut organization = None;
    let outcome = async {
        let Some((org, runtime)) = object.alarm_runtime().await? else {
            return Ok(None);
        };
        organization = Some(org);
        let result = runtime.after_event().await;
        object.flush_ready_mail(&runtime).await;
        result
            .map(Some)
            .map_err(|error| AlarmFailure::Store(error.failure))
    }
    .await;
    let alarm_after = storage.get_alarm().await?;
    let result = match &outcome {
        Ok(Some(result)) => AlarmReceiptResult::Applied {
            wakes_fired: result.work.wakes_fired,
            automations_fired: result.work.automations_fired,
            windows_fired: result.work.windows_fired,
            workspaces_drained: result.work.workspaces_drained,
        },
        Ok(None) => AlarmReceiptResult::Unprovisioned,
        Err(AlarmFailure::Store(failure)) => AlarmReceiptResult::Error {
            message: failure.to_string(),
            failure: Some(failure.clone()),
        },
        Err(AlarmFailure::Platform(failure)) => AlarmReceiptResult::Error {
            message: failure.to_string(),
            failure: None,
        },
    };
    storage
        .put(
            RECEIPT_KEY,
            &AlarmReceipt {
                organization,
                initialized_cold,
                alarm_before,
                alarm_after,
                result,
            },
        )
        .await?;
    for waiter in std::mem::take(&mut *object.controls.alarm_waiters.borrow_mut()) {
        let _ = waiter.send(());
    }
    match outcome {
        Ok(_) => {}
        Err(AlarmFailure::Store(failure)) => return Err(error(failure)),
        Err(AlarmFailure::Platform(failure)) => return Err(failure),
    }
    Response::ok("alarm applied")
}
