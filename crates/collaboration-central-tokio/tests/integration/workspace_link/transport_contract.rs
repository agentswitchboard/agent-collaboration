//! Real links retain each connection's grant, correlate concurrent calls and replay writes.
use super::fixture::{Fixture, INSTALLATION, receive, send, start_fixture};
use super::link_support::{
    caller, connect_client, correlation, create_workspace, created_workspace, workspace_reply,
};
use central_link_protocol::{
    CorrelationId, DataFrame, LinkFrame, ReplyPayload, RequestPayload, RequestScope,
    RequestStatusAnswer, RequestStatusQuery,
};
use central_storage::{
    AlarmUpdate, BatchOutcome, GuardedStatement, RowExpectation, SqlValue, Statement,
    StatementBatch,
};
use collaboration_central_conformance::fixture_link_reader::read_fixture_frame;
use collaboration_central_conformance::{ConformanceResult as Result, signin_fixtures as signed};
use collaboration_central_tokio::ObjectStorage;
use reqwest_websocket::{CloseCode, Message};
use serde_json::json;
use std::{collections::BTreeMap, sync::Arc};
use workspace_model::{ItemRef, WorkspaceAnswer, WorkspaceId, WorkspaceResult};

fn workspace_store(fixture: &Fixture, workspace: &WorkspaceId) -> ObjectStorage {
    ObjectStorage::new(
        fixture
            .root_path()
            .join("modern/workspace-stores")
            .join(format!("{workspace}.sqlite")),
        Arc::new(fleet_storage_schema::workspace_migrations::workspace_migrator()),
    )
}

#[expect(
    clippy::panic_in_result_fn,
    reason = "Two outstanding requests must retain their own network correlation and payload."
)]
#[tokio::test]
async fn two_in_flight_requests_keep_distinct_correlations_and_payloads() -> Result<()> {
    let fixture = start_fixture().await?;
    let (mut client, attempt, mut socket, _) = connect_client(&fixture, &fixture.token).await?;
    let first = client.begin_request(
        caller()?,
        create_workspace("019f0000-0000-7000-8000-00000000d201", "First outstanding")?,
    )?;
    let second = client.begin_request(
        caller()?,
        create_workspace("019f0000-0000-7000-8000-00000000d202", "Second outstanding")?,
    )?;
    let first_id = correlation(&first)?;
    let second_id = correlation(&second)?;
    assert_ne!(first_id, second_id);
    // Both frames cross the public socket before the client consumes either answer.
    send(&mut socket, &first).await?;
    send(&mut socket, &second).await?;
    let mut replies = BTreeMap::new();
    for _ in 0..2 {
        let (correlation, reply) = workspace_reply(&mut client, &attempt, &mut socket).await?;
        assert!(replies.insert(correlation, reply).is_none());
    }
    assert_eq!(
        created_workspace(replies.get(&first_id).ok_or("first correlation lost")?)?
            .name
            .as_str(),
        "First outstanding"
    );
    assert_eq!(
        created_workspace(replies.get(&second_id).ok_or("second correlation lost")?)?
            .name
            .as_str(),
        "Second outstanding"
    );
    let runtime = fixture
        .host
        .central(&signed::ORGANIZATION.to_owned().try_into()?)
        .await?;
    let rows = runtime
        .runtime()
        .statement_host()
        .read(Statement {
            sql: "SELECT name FROM directory_workspaces ORDER BY name",
            args: vec![],
        })
        .await?;
    assert_eq!(
        rows.rows,
        vec![
            vec![SqlValue::Text("First outstanding".into())],
            vec![SqlValue::Text("Second outstanding".into())]
        ]
    );
    socket
        .close(reqwest_websocket::CloseCode::Normal, None)
        .await?;
    fixture.close().await
}

#[expect(
    clippy::panic_in_result_fn,
    reason = "Lost reply and replay assertions compare the real socket with its retained SQLite answer."
)]
#[tokio::test]
async fn lost_reply_after_commit_replays_one_durable_write_on_a_new_correlation() -> Result<()> {
    // Arrange: create a real workspace and establish its explicit allowance precondition.
    let fixture = start_fixture().await?;
    let (mut client, attempt, mut socket, _) = connect_client(&fixture, &fixture.token).await?;
    let create = client.begin_request(
        caller()?,
        create_workspace(
            "019f0000-0000-7000-8000-00000000d301",
            "Replay owning store",
        )?,
    )?;
    send(&mut socket, &create).await?;
    let (_, reply) = workspace_reply(&mut client, &attempt, &mut socket).await?;
    let workspace = created_workspace(&reply)?;
    let runtime = fixture
        .host
        .central(&signed::ORGANIZATION.to_owned().try_into()?)
        .await?;
    let allowance = runtime
        .runtime()
        .statement_host()
        .run_batch(StatementBatch {
            statements: vec![GuardedStatement {
                statement: Statement {
                    sql: collaboration_central_conformance::REPLACE_FIXTURE_ALLOWANCES_SQL,
                    args: vec![
                        SqlValue::Text(serde_json::to_string(&vec![
                            workspace.workspace_id.clone(),
                        ])?),
                        SqlValue::Text(INSTALLATION.into()),
                    ],
                },
                expect: RowExpectation::Exactly(1),
            }],
            alarm: AlarmUpdate::Unchanged,
        })
        .await?;
    if !matches!(allowance, BatchOutcome::Applied { .. }) {
        return Err("workspace allowance fixture did not apply".into());
    }
    let write_id = "019f0000-0000-7000-8000-00000000d302";
    let payload = RequestPayload::Workspace(serde_json::from_value(json!({
        "requestId":write_id,"workspaceId":workspace.workspace_id,
        "operation":{"kind":"createArea","name":"Exactly one area","description":"Test description"}
    }))?);
    runtime.runtime().lose_reply_after_apply(write_id.parse()?);

    // Act: the response is unknown only after the actual workspace batch committed.
    let write = client.begin_request(caller()?, payload.clone())?;
    send(&mut socket, &write).await?;
    let (first_id, first_reply) = workspace_reply(&mut client, &attempt, &mut socket).await?;
    assert_eq!(first_id, correlation(&write)?);
    assert_eq!(first_reply.result, WorkspaceResult::OutcomeUnknown);
    let store = workspace_store(&fixture, &workspace.workspace_id);
    let committed = store
        .read(Statement {
            sql: "SELECT request_id,answer FROM workspace_request_log",
            args: vec![],
        })
        .await?;
    let [row] = committed.rows.as_slice() else {
        return Err("expected one committed request row before replay".into());
    };
    let [SqlValue::Text(stored_id), SqlValue::Text(answer)] = row.as_slice() else {
        return Err("committed request answer shape".into());
    };
    assert_eq!(stored_id, write_id);
    let durable_answer: WorkspaceResult = serde_json::from_str(answer)?;
    let WorkspaceResult::Done(WorkspaceAnswer::Created {
        item: ItemRef::Area(area),
        position,
    }) = &durable_answer
    else {
        return Err("committed answer was not the created area".into());
    };
    assert_eq!(position.get(), 1);
    let expected_area = vec![vec![
        SqlValue::Text(area.to_string()),
        SqlValue::Text("Exactly one area".into()),
    ]];
    assert_eq!(
        store
            .read(Statement {
                sql: "SELECT area_id,name FROM workspace_areas",
                args: vec![]
            })
            .await?
            .rows,
        expected_area
    );

    // Assert: a new transport correlation with the same durable id replays that exact answer.
    let retry = client.begin_request(caller()?, payload)?;
    assert_ne!(correlation(&retry)?, first_id);
    send(&mut socket, &retry).await?;
    let (retry_id, replay) = workspace_reply(&mut client, &attempt, &mut socket).await?;
    assert_eq!(retry_id, correlation(&retry)?);
    assert_eq!(replay.result, durable_answer);
    assert_eq!(
        store
            .read(Statement {
                sql: "SELECT area_id,name FROM workspace_areas",
                args: vec![]
            })
            .await?
            .rows,
        expected_area
    );
    assert_eq!(
        store
            .read(Statement {
                sql: "SELECT request_id,answer FROM workspace_request_log",
                args: vec![]
            })
            .await?,
        committed
    );
    let status_correlation: CorrelationId = "019f0000-0000-7000-8000-00000000d303".parse()?;
    let query = RequestStatusQuery {
        request_id: write_id.parse()?,
        scope: RequestScope::Workspace(workspace.workspace_id.clone()),
    };
    send(
        &mut socket,
        &LinkFrame::Data(DataFrame::RequestStatus {
            correlation_id: status_correlation.clone(),
            caller: caller()?,
            query: query.clone(),
        }),
    )
    .await?;
    assert_eq!(
        receive(&mut socket).await?,
        LinkFrame::Data(DataFrame::RequestStatusReply {
            correlation_id: status_correlation,
            result: Box::new(RequestStatusAnswer::Answered(ReplyPayload::Workspace(
                replay
            ))),
        })
    );
    use collaboration_central_conformance::machine_link_boundary::{
        expect_pong, oversized_retained_refusal, retained_answer_read,
    };
    let (refusal, seed) = oversized_retained_refusal(&query.request_id)?;
    assert!(matches!(
        store.run_batch(seed).await?,
        BatchOutcome::Applied { .. }
    ));
    for ordinal in [0xd306_u32, 0xd307] {
        let correlation_id: CorrelationId =
            format!("019f0000-0000-7000-8000-{ordinal:012x}").parse()?;
        if ordinal == 0xd306 {
            fixture
                .host
                .central(&fixture.organization_id)
                .await?
                .fail_machine_restore_after(0, central_storage::StoreFailure::Unavailable);
        }
        send(
            &mut socket,
            &LinkFrame::Data(DataFrame::RequestStatus {
                correlation_id: correlation_id.clone(),
                caller: caller()?,
                query: query.clone(),
            }),
        )
        .await?;
        assert_eq!(
            receive(&mut socket).await?,
            LinkFrame::Data(DataFrame::RequestStatusReply {
                correlation_id,
                result: Box::new(RequestStatusAnswer::CentralUnavailable {}),
            })
        );
    }
    expect_pong(&mut socket).await?;
    assert_eq!(
        store
            .read(retained_answer_read(&query.request_id))
            .await?
            .rows,
        vec![vec![SqlValue::Text(refusal)]],
    );
    let altered = store
        .run_batch(StatementBatch {
            statements: vec![GuardedStatement {
                statement: Statement {
                    sql: "UPDATE workspace_request_log SET caller=?, answer=? WHERE request_id=?",
                    args: vec![
                        SqlValue::Text("019f0000-0000-7000-8000-00000000d305".into()),
                        SqlValue::Blob(vec![255]),
                        SqlValue::Text(write_id.into()),
                    ],
                },
                expect: RowExpectation::Exactly(1),
            }],
            alarm: AlarmUpdate::Unchanged,
        })
        .await?;
    assert!(matches!(altered, BatchOutcome::Applied { .. }));
    send(
        &mut socket,
        &LinkFrame::Data(DataFrame::RequestStatus {
            correlation_id: "019f0000-0000-7000-8000-00000000d304".parse()?,
            caller: caller()?,
            query,
        }),
    )
    .await?;
    let closed = read_fixture_frame(&mut socket, "link close").await?;
    assert!(
        matches!(
            &closed,
            Some(Ok(Message::Close { code: CloseCode::Error, reason }))
                if reason == "central unavailable"
        ),
        "the damaged receipt must close the native link: {closed:?}"
    );
    store.close().await;
    fixture.close().await
}
