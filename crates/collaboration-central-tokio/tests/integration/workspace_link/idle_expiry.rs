use super::fixture::INSTALLATION;
use super::fixture::{Fixture, receive, send};
use central_link_protocol::{
    DataFrame, LinkFrame, MachineLinkCloseCode, TokenRenewalRefusal, TokenRenewalResult,
};
use central_storage::{
    AlarmUpdate, GuardedStatement, RowExpectation, SqlValue, Statement, StatementBatch,
};
use collaboration_central_conformance::{ConformanceResult as Result, signin_fixtures as signed};
use fleet_machine_login::policy::IDLE_EXPIRY_SECONDS;
use fleet_runtime::PausePoint;
use futures_util::{FutureExt, StreamExt};
use reqwest_websocket::RequestBuilderExt;
use reqwest_websocket::{Message, WebSocket};

const WORKSPACE: &str = "019f0000-0000-7000-8000-00000000ce6f";

async fn seed_idle_grant(fixture: &Fixture, elapsed_seconds: i64) -> Result<()> {
    let runtime = fixture
        .host
        .central(&signed::ORGANIZATION.to_owned().try_into()?)
        .await?;
    let idle_at = chrono::DateTime::from_timestamp(signed::NOW_SECONDS - elapsed_seconds, 0)
        .ok_or("invalid fixture clock")?
        .to_rfc3339();
    let result = runtime
        .runtime()
        .statement_host()
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
                      SqlValue::Text(INSTALLATION.to_owned()),
                  ],
              },
              expect: RowExpectation::Exactly(1),
          },
          GuardedStatement {
                statement: Statement {
                    sql: "UPDATE login_machine_approvals SET last_grant_at=? WHERE approval_id=?",
                    args: vec![
                        SqlValue::Text(idle_at),
                        SqlValue::Text(fixture.enrollment_id.as_str().to_owned()),
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
    let runtime = fixture
        .host
        .central(&signed::ORGANIZATION.to_owned().try_into()?)
        .await?;
    let rows = runtime.runtime().statement_host().read(Statement {
        sql: "SELECT state,end_reason,(SELECT count(*) FROM signin_change_records WHERE kind='machineExpired') AS expiry_count,(SELECT count(*) FROM login_machine_allowances WHERE state='withdrawn'),(SELECT count(*) FROM signin_change_records WHERE kind='machineAllowanceChanged' AND allowance_state='withdrawn') FROM login_machine_approvals WHERE approval_id=?",
        args: vec![SqlValue::Text(fixture.enrollment_id.as_str().to_owned())],
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
        let mut socket = fixture.connect().await?;
        receive(&mut socket).await?;
        seed_idle_grant(&fixture, IDLE_EXPIRY_SECONDS).await?;
        let runtime = fixture
            .host
            .central(&signed::ORGANIZATION.to_owned().try_into()?)
            .await?;
        let mut barrier = runtime
            .runtime()
            .pause_points()
            .hold_central(PausePoint::AfterIdleExpiryCommit);
        let correlation_id: central_link_protocol::CorrelationId =
            "019f0000-0000-7000-8000-00000000ce66".parse()?;
        send(
            &mut socket,
            &LinkFrame::Data(DataFrame::RenewMachineToken {
                correlation_id: correlation_id.clone(),
                token: fixture.token.clone().try_into()?,
            }),
        )
        .await?;
        barrier.until_held().await;
        let store = assert_idle_commit(&fixture).await;
        let premature = socket.next().now_or_never().is_some();
        barrier.release();
        store?;
        if premature {
            return Err("renewal emitted a frame before the commit barrier released".into());
        }
        let result = TokenRenewalResult::Refused {
            reason: TokenRenewalRefusal::EnrollmentNotActive,
        };
        if receive(&mut socket).await?
            != LinkFrame::Data(DataFrame::MachineTokenRenewal {
                correlation_id,
                result,
            })
        {
            return Err("expected the correlated inactive renewal response".into());
        }
        require_inactive_close(&mut socket).await?;
        require_inactive_close(&mut fixture.connect().await?).await
    }
    .await;
    fixture.close().await?;
    result
}

#[tokio::test]
async fn two_idle_connects_close_without_welcome_and_expire_only_once() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = async {
        let mut existing = fixture.connect().await?;
        receive(&mut existing).await?;
        seed_idle_grant(&fixture, IDLE_EXPIRY_SECONDS).await?;
        let (first, second) = tokio::join!(fixture.connect(), fixture.connect());
        require_inactive_close(&mut first?).await?;
        require_inactive_close(&mut second?).await?;
        require_inactive_close(&mut existing).await?;
        assert_idle_commit(&fixture).await
    }
    .await;
    fixture.close().await?;
    result
}

async fn connect_replacement(fixture: &Fixture) -> Result<WebSocket> {
    use fleet_contracts::{DeviceLoginDecision, MachineTokenDecision};
    use serde_json::json;

    let runtime = fixture
        .host
        .central(&signed::ORGANIZATION.to_owned().try_into()?)
        .await?;
    let request = serde_json::from_value(json!({
        "organizationId": signed::ORGANIZATION, "approver": signed::OWNER,
        "permissionChecked": "machine:enroll",
        "requestId": "019f0000-0000-7000-8000-00000000ce69",
        "machineId": INSTALLATION, "label": "Replacement",
        "allowedWorkspaces": [], "approverSecondFactorAt": signed::NOW_SECONDS - 60,
        "codeExpiresAt": signed::NOW_SECONDS + 600,
    }))?;
    let DeviceLoginDecision::Approved { enrollment_id, .. } =
        runtime.decide_device_login(&request).await?
    else {
        return Err("replacement approval refused".into());
    };
    let grant = serde_json::from_value(json!({
        "organizationId": signed::ORGANIZATION, "enrollmentId": enrollment_id,
        "clientId": enrollment_id,
    }))?;
    let MachineTokenDecision::Grant {
        grant_expires_at, ..
    } = runtime.decide_machine_token(&grant).await?
    else {
        return Err("replacement grant refused".into());
    };
    let bearer = signed::machine_token(
        signed::ORGANIZATION,
        INSTALLATION,
        enrollment_id.as_str(),
        signed::NOW_SECONDS,
        grant_expires_at.0,
    )?;
    Ok(fixture
        .client()?
        .get(fixture.url(signed::ORGANIZATION))
        .bearer_auth(bearer)
        .upgrade()
        .send()
        .await?
        .into_websocket()
        .await?)
}

#[tokio::test]
async fn idle_reply_send_failure_still_closes_only_the_ended_enrollment() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = async {
        let mut old = fixture.connect().await?;
        receive(&mut old).await?;
        seed_idle_grant(&fixture, IDLE_EXPIRY_SECONDS).await?;
        let runtime = fixture
            .host
            .central(&signed::ORGANIZATION.to_owned().try_into()?)
            .await?;
        runtime.fail_next_idle_renewal_send();
        let mut before = runtime
            .runtime()
            .pause_points()
            .hold_central(PausePoint::BeforeIdleEnrollmentCleanup);
        let mut after = runtime
            .runtime()
            .pause_points()
            .hold_central(PausePoint::AfterIdleEnrollmentCleanup);
        send(
            &mut old,
            &LinkFrame::Data(DataFrame::RenewMachineToken {
                correlation_id: "019f0000-0000-7000-8000-00000000ce67".parse()?,
                token: fixture.token.clone().try_into()?,
            }),
        )
        .await?;
        before.until_held().await;
        require_inactive_close(&mut old).await?;
        assert_idle_commit(&fixture).await?;
        let mut replacement = connect_replacement(&fixture).await?;
        if !matches!(
            receive(&mut replacement).await?,
            LinkFrame::Data(DataFrame::Welcome { .. })
        ) {
            return Err("replacement enrollment must receive Welcome".into());
        }
        before.release();
        after.until_held().await;
        after.release();
        require_inactive_close(&mut fixture.connect().await?).await?;
        send(&mut replacement, &LinkFrame::Ping).await?;
        if receive(&mut replacement).await? != LinkFrame::Pong {
            return Err("ended enrollment cleanup affected the replacement".into());
        }
        Ok(())
    }
    .await;
    fixture.close().await?;
    result
}

#[tokio::test]
async fn connect_allows_one_second_before_idle_expiry_and_rejects_one_second_after() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = async {
        seed_idle_grant(&fixture, IDLE_EXPIRY_SECONDS - 1).await?;
        let mut before = fixture.connect().await?;
        if !matches!(
            receive(&mut before).await?,
            LinkFrame::Data(DataFrame::Welcome { .. })
        ) {
            return Err("the link must admit before the idle boundary".into());
        }
        before
            .close(reqwest_websocket::CloseCode::Normal, None)
            .await?;

        seed_idle_grant(&fixture, IDLE_EXPIRY_SECONDS + 1).await?;
        require_inactive_close(&mut fixture.connect().await?).await?;
        assert_idle_commit(&fixture).await
    }
    .await;
    fixture.close().await?;
    result
}
