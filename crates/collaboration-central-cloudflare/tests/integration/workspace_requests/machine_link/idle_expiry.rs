use super::fixture::MACHINE_ID;
use super::fixture::{ConnectedLink, Fixture, ORGANIZATION, receive, send};
use central_link_protocol::{
    DataFrame, MachineLinkCloseCode, TokenRenewalRefusal, TokenRenewalResult,
};
use central_storage::{
    AlarmUpdate, GuardedStatement, RowExpectation, SqlValue, Statement, StatementBatch,
};
use collaboration_central_conformance::{ConformanceResult as Result, signin_fixtures as signed};
use futures_util::{FutureExt, SinkExt, StreamExt};
use reqwest_websocket::{Message, RequestBuilderExt, WebSocket};

async fn connect(fixture: &Fixture) -> Result<WebSocket> {
    Ok(fixture
        .client
        .get(fixture.link_url(ORGANIZATION))
        .bearer_auth(&fixture.token)
        .upgrade()
        .send()
        .await?
        .into_websocket()
        .await?)
}

const WORKSPACE: &str = "019f0000-0000-7000-8000-00000000ce6f";

async fn seed_idle_grant(fixture: &Fixture, elapsed_seconds: i64) -> Result<()> {
    let idle_at = chrono::DateTime::from_timestamp(signed::NOW_SECONDS - elapsed_seconds, 0)
        .ok_or("invalid fixture clock")?
        .to_rfc3339();
    let result = fixture
        .runtime()
        .run_batch(StatementBatch {
            statements: vec![
          GuardedStatement {
              statement: Statement {
                  sql: "INSERT OR IGNORE INTO directory_workspaces(workspace_id,name,state,created_at,updated_at) VALUES (?,'Idle fixture workspace','active','1970-01-01T00:00:00Z','1970-01-01T00:00:00Z')",
                  args: vec![SqlValue::Text(WORKSPACE.to_owned())],
              },
              expect: RowExpectation::Any,
          },
          GuardedStatement {
              statement: Statement {
                  sql: collaboration_central_conformance::REPLACE_FIXTURE_ALLOWANCES_SQL,
                  args: vec![
                      SqlValue::Text(serde_json::to_string(&[WORKSPACE])?),
                      SqlValue::Text(MACHINE_ID.to_owned()),
                  ],
              },
              expect: RowExpectation::Exactly(1),
          },
          GuardedStatement {
                statement: Statement {
                    sql: "UPDATE login_machine_approvals SET last_grant_at=? WHERE approval_id=?",
                    args: vec![
                        SqlValue::Text(idle_at),
                        SqlValue::Text(fixture.enrollment.as_str().to_owned()),
                    ],
                },
                expect: RowExpectation::Exactly(1),
            }],
            alarm: AlarmUpdate::Unchanged,
        })
        .await?;
    if !matches!(result, central_storage::BatchOutcome::Applied { .. }) {
        return Err("could not set the idle deadline fixture".into());
    }
    Ok(())
}

async fn assert_idle_commit(fixture: &Fixture) -> Result<()> {
    let rows = fixture.runtime().read(Statement {
        sql: "SELECT state,end_reason,(SELECT count(*) FROM signin_change_records WHERE kind='machineExpired') AS expiry_count,(SELECT count(*) FROM login_machine_allowances WHERE state='withdrawn'),(SELECT count(*) FROM signin_change_records WHERE kind='machineAllowanceChanged' AND allowance_state='withdrawn') FROM login_machine_approvals WHERE approval_id=?",
        args: vec![SqlValue::Text(fixture.enrollment.as_str().to_owned())],
    }).await?.rows;
    if rows
        != [vec![
            SqlValue::Text("ended".into()),
            SqlValue::Text("idle".into()),
            SqlValue::Integer(1_i64.try_into()?),
            SqlValue::Integer(1_i64.try_into()?),
            SqlValue::Integer(1_i64.try_into()?),
        ]]
    {
        return Err(format!("expected one committed idle expiry: {rows:?}").into());
    }
    Ok(())
}

async fn login_rows(fixture: &Fixture) -> Result<Vec<Vec<Vec<SqlValue>>>> {
    let store = fixture.runtime();
    let mut rows = Vec::new();
    for sql in [
        "SELECT * FROM login_machine_approvals ORDER BY approval_id",
        "SELECT * FROM login_machines ORDER BY machine_id",
        "SELECT * FROM login_machine_allowances ORDER BY allowance_id",
        "SELECT * FROM signin_change_records ORDER BY id",
    ] {
        rows.push(store.read(Statement { sql, args: vec![] }).await?.rows);
    }
    Ok(rows)
}

async fn schema_statement(fixture: &Fixture, sql: &'static str) -> Result<()> {
    let store = fixture.runtime();
    let outcome = store
        .run_batch(StatementBatch {
            statements: vec![GuardedStatement {
                statement: Statement { sql, args: vec![] },
                expect: RowExpectation::Any,
            }],
            alarm: AlarmUpdate::Unchanged,
        })
        .await?;
    if !matches!(outcome, central_storage::BatchOutcome::Applied { .. }) {
        return Err(format!("idle failure fixture DDL did not apply: {outcome:?}").into());
    }
    Ok(())
}

async fn renew_and_receive(
    fixture: &Fixture,
    socket: &mut WebSocket,
) -> Result<TokenRenewalResult> {
    let expected_correlation: central_link_protocol::CorrelationId =
        "019f0000-0000-7000-8000-00000000ce68".parse()?;
    send(
        socket,
        &DataFrame::RenewMachineToken {
            correlation_id: expected_correlation.clone(),
            token: fixture.token.clone().try_into()?,
        },
    )
    .await?;
    let reply = receive(socket).await?;
    match reply {
        DataFrame::MachineTokenRenewal {
            correlation_id,
            result,
        } if correlation_id == expected_correlation => Ok(result),
        other => Err(format!("expected a correlated renewal response, got {other:?}").into()),
    }
}

async fn require_inactive_close(socket: &mut WebSocket) -> Result<()> {
    let frame = socket.next().await;
    if !matches!(frame, Some(Ok(Message::Close { code, .. })) if MachineLinkCloseCode::from_code(code.into()) == Some(MachineLinkCloseCode::Inactive))
    {
        return Err(format!("expected Inactive close, got {frame:?}").into());
    }
    Ok(())
}

#[tokio::test]
async fn idle_renewal_commits_before_its_correlated_inactive_reply_and_close() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = async {
        let mut connection = ConnectedLink::open(&fixture).await?;
        seed_idle_grant(&fixture, signed::MACHINE_IDLE_LIMIT_SECONDS).await?;
        let runtime = fixture.runtime();
        let pause = "afterIdleExpiryCommit";
        if !runtime.control::<bool>("hold-central", &pause).await? {
            return Err("could not arm idle commit barrier".into());
        }
        let correlation_id: central_link_protocol::CorrelationId =
            "019f0000-0000-7000-8000-00000000cf66".parse()?;
        send(
            &mut connection.socket,
            &DataFrame::RenewMachineToken {
                correlation_id: correlation_id.clone(),
                token: fixture.token.clone().try_into()?,
            },
        )
        .await?;
        tokio::select! {
            held = runtime.control::<bool>("until-held-central", &pause) => {
                if !held? {
                    return Err("idle expiry did not commit".into());
                }
            }
            early = connection.socket.next() => {
                return Err(format!("renewal emitted a frame before reaching the idle commit: {early:?}").into());
            }
        }
        let store = assert_idle_commit(&fixture).await;
        let premature = connection.socket.next().now_or_never().is_some();
        if !runtime.control::<bool>("release-central", &pause).await? {
            return Err("could not release idle expiry".into());
        }
        store?;
        if premature {
            return Err("renewal emitted a frame before the commit barrier released".into());
        }
        let result = TokenRenewalResult::Refused {
            reason: TokenRenewalRefusal::ConnectionEnrollmentEnded,
        };
        if receive(&mut connection.socket).await?
            != (DataFrame::MachineTokenRenewal {
                correlation_id,
                result,
            })
        {
            return Err("expected the correlated inactive renewal response".into());
        }
        require_inactive_close(&mut connection.socket).await?;
        require_inactive_close(&mut connect(&fixture).await?).await
    }
    .await;
    fixture.close().await?;
    result
}

#[tokio::test]
async fn two_idle_connects_close_without_welcome_and_expire_only_once() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = async {
        let mut existing = ConnectedLink::open(&fixture).await?;
        seed_idle_grant(&fixture, signed::MACHINE_IDLE_LIMIT_SECONDS).await?;
        let runtime = fixture.runtime();
        let pause = "afterIdleExpiryCommit";
        if !runtime.control::<bool>("hold-central", &pause).await? {
            return Err("could not arm the first connect's idle commit barrier".into());
        }
        let mut first = std::pin::pin!(connect(&fixture));
        let mut second = std::pin::pin!(connect(&fixture));
        let observation: Result<()> = async {
            tokio::select! {
                held = runtime.control::<bool>("until-held-central", &pause) => {
                    if !held? {
                        return Err("first connect did not reach its idle commit".into());
                    }
                }
                _ = &mut first => return Err("first connect finished before its idle commit was observed".into()),
            }
            tokio::select! {
                waiting = runtime.control::<bool>("until-admission-waiting", &1_usize) => {
                    if !waiting? {
                        return Err("second connect was not observed waiting for admission".into());
                    }
                }
                _ = &mut first => return Err("first connect upgraded before idle commit release".into()),
                _ = &mut second => return Err("second connect bypassed the held idle commit".into()),
            }
            assert_idle_commit(&fixture).await?;
            if first.as_mut().now_or_never().is_some()
                || second.as_mut().now_or_never().is_some()
                || existing.socket.next().now_or_never().is_some()
            {
                return Err("an upgrade or socket frame appeared before idle commit release".into());
            }
            Ok(())
        }
        .await;
        if !runtime.control::<bool>("release-central", &pause).await? {
            return Err("could not release the first connect's idle commit".into());
        }
        observation?;
        let (first, second) = tokio::join!(first, second);
        require_inactive_close(&mut first?).await?;
        require_inactive_close(&mut second?).await?;
        require_inactive_close(&mut existing.socket).await?;
        assert_idle_commit(&fixture).await
    }
    .await;
    fixture.close().await?;
    result
}

async fn connect_replacement(fixture: &Fixture) -> Result<(WebSocket, String)> {
    use super::fixture::{approve, grant};
    use fleet_contracts::MachineTokenDecision;

    let enrollment = approve(
        &fixture.host,
        &fixture.client,
        "019f0000-0000-7000-8000-00000000cf69",
        signed::NOW_SECONDS,
    )
    .await?;
    let MachineTokenDecision::Grant {
        grant_expires_at, ..
    } = grant(&fixture.host, &fixture.client, &enrollment).await?
    else {
        return Err("replacement grant refused".into());
    };
    let bearer = signed::machine_token(
        ORGANIZATION,
        MACHINE_ID,
        enrollment.as_str(),
        signed::NOW_SECONDS,
        grant_expires_at.0,
    )?;
    let socket = fixture
        .client
        .get(fixture.link_url(ORGANIZATION))
        .bearer_auth(&bearer)
        .upgrade()
        .send()
        .await?
        .into_websocket()
        .await?;
    Ok((socket, bearer))
}

#[tokio::test]
async fn idle_reply_send_failure_still_closes_only_the_ended_enrollment() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = async {
        let mut old = ConnectedLink::open(&fixture).await?;
        seed_idle_grant(&fixture, signed::MACHINE_IDLE_LIMIT_SECONDS).await?;
        let runtime = fixture.runtime();
        let before = "beforeIdleEnrollmentCleanup";
        let after = "afterIdleEnrollmentCleanup";
        if !runtime
            .control::<bool>("fail-idle-renewal-send", &())
            .await?
            || !runtime.control::<bool>("hold-central", &before).await?
            || !runtime.control::<bool>("hold-central", &after).await?
        {
            return Err("could not arm the idle-send failure and cleanup barriers".into());
        }
        send(
            &mut old.socket,
            &DataFrame::RenewMachineToken {
                correlation_id: "019f0000-0000-7000-8000-00000000cf67".parse()?,
                token: fixture.token.clone().try_into()?,
            },
        )
        .await?;
        if !runtime
            .control::<bool>("until-held-central", &before)
            .await?
        {
            return Err("renewal did not reach cleanup".into());
        }
        require_inactive_close(&mut old.socket).await?;
        assert_idle_commit(&fixture).await?;
        let replacement = connect_replacement(&fixture).await;
        if !runtime.control::<bool>("release-central", &before).await?
            || !runtime
                .control::<bool>("until-held-central", &after)
                .await?
            || !runtime.control::<bool>("release-central", &after).await?
        {
            return Err("did not observe completed idle cleanup".into());
        }
        let (mut replacement, _) = replacement?;
        if !matches!(receive(&mut replacement).await?, DataFrame::Welcome { .. }) {
            return Err("replacement enrollment must receive Welcome".into());
        }
        require_inactive_close(&mut connect(&fixture).await?).await?;
        replacement.send(Message::Text("ping".into())).await?;
        if !matches!(replacement.next().await, Some(Ok(Message::Text(text))) if text == "pong") {
            return Err("ended enrollment cleanup affected the replacement".into());
        }
        Ok(())
    }
    .await;
    fixture.close().await?;
    result
}

#[tokio::test]
async fn peer_close_after_idle_commit_exercises_the_real_worker_send_error() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = idle_renewal_send_after_peer_close(&fixture).await;
    fixture.close().await?;
    result
}

async fn idle_renewal_send_after_peer_close(fixture: &Fixture) -> Result<()> {
    let mut original = ConnectedLink::open(fixture).await?.socket;
    seed_idle_grant(fixture, signed::MACHINE_IDLE_LIMIT_SECONDS).await?;
    let runtime = fixture.runtime();
    let commit = "afterIdleExpiryCommit";
    let send_error = "afterIdleRenewalPlatformSendError";
    let cleaned_up = "afterIdleEnrollmentCleanup";
    for point in [commit, send_error, cleaned_up] {
        if !runtime.control::<bool>("hold-central", &point).await? {
            return Err(format!("could not arm {point}").into());
        }
    }
    send(
        &mut original,
        &DataFrame::RenewMachineToken {
            correlation_id: "019f0000-0000-7000-8000-00000000cf6a".parse()?,
            token: fixture.token.clone().try_into()?,
        },
    )
    .await?;
    let disconnect: Result<()> = async {
        tokio::select! {
            held = runtime.control::<bool>("until-held-central", &commit) => {
                if !held? {
                    return Err("idle terminalization did not commit".into());
                }
            }
            early = original.next() => {
                return Err(format!("the Worker emitted a frame ahead of the idle commit hold: {early:?}").into());
            }
        }
        assert_idle_commit(fixture).await?;
        if original.next().now_or_never().is_some() {
            return Err("the Worker emitted a frame before idle commit release".into());
        }
        // Wait for the DO close callback's reply before resuming the suspended
        // message callback. That callback closes the actual hibernating endpoint.
        original
            .send(Message::Close {
                code: reqwest_websocket::CloseCode::Normal,
                reason: "renewal caller closed".into(),
            })
            .await?;
        let ack = original.next().await;
        if !matches!(&ack, Some(Ok(Message::Close { code: reqwest_websocket::CloseCode::Normal, reason })) if reason == "closed") {
            return Err(format!("the peer must receive the Workerd close callback's reply: {ack:?}").into());
        }
        Ok(())
    }
    .await;
    if !runtime.control::<bool>("release-central", &commit).await? {
        return Err("could not release the committed idle renewal".into());
    }
    disconnect?;
    let mut cleanup = std::pin::pin!(runtime.control::<bool>("until-held-central", &cleaned_up));
    tokio::select! {
        failed = runtime.control::<bool>("until-held-central", &send_error) => {
            if !failed? {
                return Err("the real Worker send did not report an error".into());
            }
        }
        cleaned = &mut cleanup => {
            if cleaned? {
                let _ = runtime.control::<bool>("release-central", &cleaned_up).await?;
            }
            return Err("the Worker reached cleanup without a real send error".into());
        }
    }
    let replacement = connect_replacement(fixture).await;
    if !runtime
        .control::<bool>("release-central", &send_error)
        .await?
        || !cleanup.await?
        || !runtime
            .control::<bool>("release-central", &cleaned_up)
            .await?
    {
        return Err("did not observe cleanup after the real Worker send error".into());
    }
    assert_idle_commit(fixture).await?;
    let (mut replacement, bearer) = replacement?;
    if !matches!(receive(&mut replacement).await?, DataFrame::Welcome { .. }) {
        return Err("replacement enrollment must receive Welcome".into());
    }
    let expected: central_link_protocol::CorrelationId =
        "019f0000-0000-7000-8000-00000000cf6b".parse()?;
    send(
        &mut replacement,
        &DataFrame::RenewMachineToken {
            correlation_id: expected.clone(),
            token: bearer.try_into()?,
        },
    )
    .await?;
    let answer = receive(&mut replacement).await?;
    if !matches!(&answer, DataFrame::MachineTokenRenewal {
        correlation_id,
        result: TokenRenewalResult::Renewed { .. },
    } if correlation_id == &expected)
    {
        return Err(
            format!("real send failure cleanup disturbed the replacement: {answer:?}").into(),
        );
    }
    Ok(())
}

#[tokio::test]
async fn connect_allows_one_second_before_idle_expiry_and_rejects_one_second_after() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = async {
        seed_idle_grant(&fixture, signed::MACHINE_IDLE_LIMIT_SECONDS - 1).await?;
        let mut before = connect(&fixture).await?;
        if !matches!(receive(&mut before).await?, DataFrame::Welcome { .. }) {
            return Err("the link must admit before the idle boundary".into());
        }
        before
            .close(reqwest_websocket::CloseCode::Normal, None)
            .await?;

        seed_idle_grant(&fixture, signed::MACHINE_IDLE_LIMIT_SECONDS + 1).await?;
        require_inactive_close(&mut connect(&fixture).await?).await?;
        assert_idle_commit(&fixture).await
    }
    .await;
    fixture.close().await?;
    result
}

#[tokio::test]
async fn renewal_allows_one_second_before_idle_expiry_and_rejects_one_second_after() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = async {
        let mut connection = ConnectedLink::open(&fixture).await?;
        seed_idle_grant(&fixture, signed::MACHINE_IDLE_LIMIT_SECONDS - 1).await?;
        let before = login_rows(&fixture).await?;
        if !matches!(
            renew_and_receive(&fixture, &mut connection.socket).await?,
            TokenRenewalResult::Renewed { .. }
        ) || login_rows(&fixture).await? != before
        {
            return Err(
                "renewal below the idle limit must renew without changing login rows".into(),
            );
        }

        seed_idle_grant(&fixture, signed::MACHINE_IDLE_LIMIT_SECONDS + 1).await?;
        let actual = renew_and_receive(&fixture, &mut connection.socket).await?;
        let expected = TokenRenewalResult::Refused {
            reason: TokenRenewalRefusal::ConnectionEnrollmentEnded,
        };
        if actual != expected {
            return Err(
                format!("expected inactive renewal after the idle limit, got {actual:?}").into(),
            );
        }
        require_inactive_close(&mut connection.socket).await?;
        assert_idle_commit(&fixture).await
    }
    .await;
    fixture.close().await?;
    result
}

#[tokio::test]
async fn late_idle_expiry_abort_preserves_login_rows_and_the_existing_link() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = async {
        let mut connection = ConnectedLink::open(&fixture).await?;
        seed_idle_grant(&fixture, signed::MACHINE_IDLE_LIMIT_SECONDS).await?;
        let before = login_rows(&fixture).await?;
        // The withdrawal audit is last, after the enrollment end, machine update,
        // expiry audit and allowance withdrawal have executed in the batch.
        schema_statement(
            &fixture,
            "CREATE TRIGGER fail_idle_withdrawal_audit BEFORE INSERT ON signin_change_records WHEN NEW.kind='machineAllowanceChanged' AND NEW.allowance_state='withdrawn' BEGIN SELECT RAISE(ABORT,'fixture late idle expiry failure'); END",
        )
        .await?;

        let response = fixture
            .client
            .get(fixture.link_url(ORGANIZATION))
            .bearer_auth(&fixture.token)
            .upgrade()
            .send()
            .await?;
        if response.status().as_u16() != 503 {
            return Err(format!("idle connect batch abort must return 503, got {}", response.status()).into());
        }
        if connection.socket.next().now_or_never().is_some() || login_rows(&fixture).await? != before {
            return Err("failed idle connect changed login rows or sent an existing-link frame".into());
        }

        let actual = renew_and_receive(&fixture, &mut connection.socket).await?;
        let expected = TokenRenewalResult::Refused {
            reason: TokenRenewalRefusal::CentralUnavailable,
        };
        if actual != expected {
            return Err(format!("idle renewal batch abort must report unavailability, got {actual:?}").into());
        }
        if connection.socket.next().now_or_never().is_some() || login_rows(&fixture).await? != before {
            return Err("failed idle renewal changed login rows or sent a terminal frame".into());
        }

        schema_statement(&fixture, "DROP TRIGGER fail_idle_withdrawal_audit").await?;
        // Retrying on this same socket proves the failed renewal left it usable.
        let actual = renew_and_receive(&fixture, &mut connection.socket).await?;
        let expected = TokenRenewalResult::Refused {
            reason: TokenRenewalRefusal::ConnectionEnrollmentEnded,
        };
        if actual != expected {
            return Err(format!("healthy idle renewal retry must end the enrollment, got {actual:?}").into());
        }
        require_inactive_close(&mut connection.socket).await?;
        assert_idle_commit(&fixture).await
    }
    .await;
    fixture.close().await?;
    result
}
