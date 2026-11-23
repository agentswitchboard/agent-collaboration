//! Workspace store, migration and turn controls for the test-support bundle.
use super::super::{AlarmPolicy, OrgRuntime};
use super::{
    CentralObject, ObjectAddress, OwnedStatement, Pause, PausePoint, Store, Wait, error,
    statement_store,
};
use crate::store_wire::OwnedBatch;
use worker::{Headers, Method, Request, RequestInit, Response, Result};

pub(super) async fn control(
    object: &CentralObject,
    runtime: &OrgRuntime,
    path: &str,
    mut request: Request,
) -> Result<Response> {
    match path {
        "/test/central-batch" => {
            let batch = request.json::<OwnedBatch>().await?;
            let policy = match &batch.alarm {
                crate::store_wire::OwnedAlarmUpdate::Engine(_) => AlarmPolicy::Engine(
                    object.state.storage(),
                    object.clock.clone(),
                    fleet_runtime::engine_alarm_decider(),
                ),
                crate::store_wire::OwnedAlarmUpdate::Unchanged
                | crate::store_wire::OwnedAlarmUpdate::SetFromQuery(_) => AlarmPolicy::Ignore,
            };
            Response::from_json(
                &statement_store::run_batch(&object.connection, batch, policy).await,
            )
        }
        "/test/engine-due-schedules" | "/test/engine-due-system-expiries" => {
            let horizon_ms = request.json::<i64>().await?;
            let horizon = fleet_runtime_ports::EngineTime::from_timestamp_millis(horizon_ms)
                .ok_or_else(|| error("test list horizon is outside the supported range"))?;
            let kind = if path == "/test/engine-due-schedules" {
                fleet_runtime_ports::DueListKind::Schedules
            } else {
                fleet_runtime_ports::DueListKind::SystemExpiries
            };
            let rows =
                fleet_engine::read_due_rows(object.statement_host().as_ref(), kind, horizon, 250)
                    .await
                    .map_err(|_| error("typed due list failed"))?;
            let rows: Vec<_> = rows
                .into_iter()
                .map(|row| (row.key.0, row.occurrence_ms, row.effective_due_ms))
                .collect();
            Response::from_json(&rows)
        }
        "/test/central-read" => Response::from_json(
            &statement_store::read(&object.connection, request.json::<OwnedStatement>().await?)
                .await,
        ),
        "/test/store" => {
            let store: Store = request.json().await?;
            Response::from_json(
                &crate::cloudflare_transport::call_store(&object.env, store.workspace, store.call)
                    .await,
            )
        }
        "/test/store-failing-upgrade" => {
            let store: Store = request.json().await?;
            Response::from_json(
                &crate::cloudflare_transport::call_failing_workspace_store(
                    &object.env,
                    store.workspace,
                    store.call,
                )
                .await,
            )
        }
        "/test/reset-store" => {
            let workspace: workspace_ids::WorkspaceId = request.json().await?;
            let address = ObjectAddress::WorkspaceStore(workspace);
            let headers = Headers::new();
            crate::cloudflare_transport::address_header(&headers, &address)?;
            let mut init = RequestInit::new();
            init.with_method(Method::Post).with_headers(headers);
            let response = crate::cloudflare_transport::stub(&object.env, &address)?
                .fetch_with_request(Request::new_with_init(
                    "https://central.internal/test/crash",
                    &init,
                )?)
                .await;
            Response::from_json(
                &response
                    .map(|response| response.status_code() != 200)
                    .unwrap_or(true),
            )
        }
        "/test/central-failing-upgrade" => Response::from_json(
            &statement_store::migrate(
                &object.connection,
                std::sync::Arc::new(fleet_storage_schema::workspace_migrations::failing_upgrade(
                    fleet_storage_schema::workspace_migrations::central_migrator(),
                )),
            )
            .await
            .map_err(|error| error.to_string()),
        ),
        "/test/lose-read" => {
            object
                .controls
                .lose_read
                .store(true, std::sync::atomic::Ordering::SeqCst);
            Response::from_json(&true)
        }
        "/test/lose-reply" => {
            runtime
                .runtime()
                .lose_reply_after_apply(request.json().await?);
            Response::from_json(&true)
        }
        "/test/hold" => {
            let target: Pause = request.json().await?;
            let held = runtime
                .runtime()
                .pause_points()
                .hold(&target.workspace, target.point.into());
            object
                .controls
                .holds
                .borrow_mut()
                .insert((target.workspace, target.point), held);
            Response::from_json(&true)
        }
        "/test/until-held" => {
            let target: Pause = request.json().await?;
            let key = (target.workspace, target.point);
            let mut held = object
                .controls
                .holds
                .borrow_mut()
                .remove(&key)
                .ok_or_else(|| error("point not armed"))?;
            held.until_held().await;
            object.controls.holds.borrow_mut().insert(key, held);
            Response::from_json(&true)
        }
        "/test/guard-next-read" => {
            let wait: Wait = request.json().await?;
            object.controls.observations.borrow_mut().insert(
                wait.workspace.clone(),
                runtime
                    .runtime()
                    .pause_points()
                    .hold(&wait.workspace, PausePoint::AfterReads),
            );
            Response::from_json(&true)
        }
        "/test/waiting-before-read" => {
            let wait: Wait = request.json().await?;
            let mut held = object
                .controls
                .observations
                .borrow_mut()
                .remove(&wait.workspace)
                .ok_or_else(|| error("read observation not armed"))?;
            let before_read = {
                let read = held.until_held();
                let waiting = runtime
                    .runtime()
                    .workspace_turns()
                    .until_waiting(&wait.workspace, wait.count);
                futures_util::pin_mut!(read, waiting);
                matches!(
                    futures_util::future::select(read, waiting).await,
                    futures_util::future::Either::Right(_)
                )
            };
            Response::from_json(&before_read)
        }
        "/test/crash" => {
            object
                .state
                .block_concurrency_while(async {
                    Err::<(), worker::Error>(error("workspace fixture crash"))
                })
                .await?;
            Response::error("object did not reset", 500)
        }
        "/test/until-waiting" => {
            let wait: Wait = request.json().await?;
            runtime
                .runtime()
                .workspace_turns()
                .until_waiting(&wait.workspace, wait.count)
                .await;
            Response::from_json(&true)
        }
        "/test/release" => {
            let target: Pause = request.json().await?;
            let held = object
                .controls
                .holds
                .borrow_mut()
                .remove(&(target.workspace, target.point));
            if let Some(held) = held {
                held.release();
            }
            Response::from_json(&true)
        }
        _ => Response::error("unknown workspace control", 404),
    }
}
