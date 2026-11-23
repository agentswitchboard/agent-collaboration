#[path = "approval_login_settlement.rs"]
mod approval_login_settlement;

use super::fixture::{INSTALLATION, send, signed_clock};
use super::typed_calls::{connect, expect_central_reply};
use central_link_protocol::{
    CentralLinkRpc, DataFrame, LinkFrame, TypedCallResult,
    approval_rpc::{ApprovalServiceTarget, RegisterRunReply, RegisterRunRpc},
};
use central_storage::{SqlValue, Statement};
use collaboration_central_conformance::ConformanceResult as Result;
use collaboration_central_conformance::approval_authority_scenarios::approval_authority_refusals_leave_store_unchanged;
use collaboration_central_tokio::test_support::start_session_link_fixture;
use fleet_rpc::Envelope;
use serde_json::json;

const RUN: &str = "019f0000-0000-7000-8000-000000005401";

#[tokio::test]
async fn production_link_registers_run_in_tokios_owning_approval_store() -> Result<()> {
    let fixture = start_session_link_fixture(signed_clock()?).await?;
    let observed = async {
        let mut socket = connect(&fixture).await?;
        let owner = fixture.host.central(&fixture.organization_id).await?;
        let store = owner.runtime().statement_host();
        let stored_run = Statement {
            sql: "SELECT organization_id,machine_id,enrollment_id,router_run_id FROM approval_router_runs ORDER BY registration_order",
            args: vec![],
        };
        if !store.read(stored_run.clone()).await?.rows.is_empty() {
            return Err("the fixture must start without a registered run".into());
        }

        let correlation: central_link_protocol::CorrelationId =
            "019f0000-0000-7000-8000-000000005402".parse()?;
        let target = ApprovalServiceTarget { organization_id: fixture.organization_id.clone() };
        let request = RegisterRunRpc::request(Envelope {
            target: target.clone(),
            body: serde_json::from_value(json!({ "routerRun": RUN }))?,
        });
        send(&mut socket, &LinkFrame::Data(DataFrame::CallCentral {
            correlation_id: correlation.clone(),
            request: Box::new(request),
        })).await?;

        expect_central_reply(&mut socket, correlation, TypedCallResult::Answered {
            reply: RegisterRunRpc::reply(Envelope { target, body: RegisterRunReply::Registered {} }),
        }).await?;
        let expected = [fixture.organization_id.as_str(), INSTALLATION, fixture.enrollment_id.as_str(), RUN]
            .map(|field| SqlValue::Text(field.to_owned()));
        if store.read(stored_run).await?.rows != [expected] {
            return Err("RegisterRun did not commit the exact admitted run row".into());
        }
        Ok(())
    }.await;
    fixture.close().await?;
    observed
}

use super::fixture::Fixture;
use super::receipt_frames::{EnrollmentAtApply, stored_ending_batch};
use central_link_protocol::approval_rpc::ApprovalHolderTarget;
use collaboration_central_conformance::approval_link_scenarios::{
    ApprovalLinkFixture, DeadlineDelivery, ROUTER_RUN, approval_deadline_delivery, receive_frame,
    registered_stop_and_replay,
};
use reqwest_websocket::{RequestBuilderExt, WebSocket};
use std::{
    cell::{Cell, RefCell},
    time::Duration,
};

struct NativeApprovalFixture {
    link: Fixture,
    before_alarm: Cell<u64>,
    delivery_answer: RefCell<Option<fleet_runtime::HeldPause>>,
    before_write: RefCell<Option<fleet_runtime::HeldPause>>,
    notice_recovery: RefCell<Option<fleet_runtime::HeldPause>>,
}

impl ApprovalLinkFixture for NativeApprovalFixture {
    fn now_seconds(&self) -> i64 {
        self.link.clock.utc_now().timestamp()
    }

    fn target(&self) -> Result<ApprovalHolderTarget> {
        Ok(ApprovalHolderTarget {
            organization_id: self.link.organization_id.clone(),
            machine_id: INSTALLATION.parse()?,
            enrollment_id: self.link.enrollment_id.clone(),
            router_run: ROUTER_RUN.parse()?,
        })
    }

    async fn connect(&self) -> Result<WebSocket> {
        let token = self.refresh().await?;
        let mut socket = self
            .link
            .client()?
            .get(self.link.url(self.link.organization_id.as_str()))
            .bearer_auth(token)
            .upgrade()
            .send()
            .await?
            .into_websocket()
            .await?;
        if !matches!(
            receive_frame(&mut socket).await?,
            LinkFrame::Data(DataFrame::Welcome { .. })
        ) {
            return Err("missing welcomed socket".into());
        }
        Ok(socket)
    }

    async fn set_wall_seconds(&self, seconds: i64) -> Result<()> {
        let time = self.link.clock.utc_now();
        self.link
            .clock
            .set_utc(time + chrono::Duration::seconds(seconds - time.timestamp()));
        Ok(())
    }

    async fn fixture_write(&self, statement: Statement) -> Result<()> {
        let owner = self.link.host.central(&self.link.organization_id).await?;
        let result = owner
            .runtime()
            .statement_host()
            .run_batch(central_storage::StatementBatch {
                statements: vec![central_storage::GuardedStatement {
                    statement,
                    expect: central_storage::RowExpectation::Exactly(1),
                }],
                alarm: central_storage::AlarmUpdate::Unchanged,
            })
            .await?;
        if !matches!(result, central_storage::BatchOutcome::Applied { .. }) {
            return Err("approval fixture update failed".into());
        }
        Ok(())
    }

    async fn read(&self, statement: Statement) -> Result<central_storage::ReadOutcome> {
        let owner = self.link.host.central(&self.link.organization_id).await?;
        Ok(owner.runtime().statement_host().read(statement).await?)
    }

    async fn advance(&self, seconds: u64) -> Result<()> {
        let owner = self.link.host.central(&self.link.organization_id).await?;
        self.before_alarm
            .set(owner.runtime().alarm_completion_count());
        self.link.clock.advance(Duration::from_secs(seconds))?;
        Ok(())
    }

    async fn refresh(&self) -> Result<String> {
        self.link.mint_machine_token().await
    }

    async fn observe_due_alarm(&self) -> Result<()> {
        let owner = self.link.host.central(&self.link.organization_id).await?;
        owner
            .runtime()
            .until_alarm_completed(self.before_alarm.get() + 1)
            .await;
        Ok(())
    }

    async fn arm_notice_recovery(&self) -> Result<()> {
        let runtime = self.link.host.central(&self.link.organization_id).await?;
        let hold = runtime
            .runtime()
            .pause_points()
            .hold_central(PausePoint::AfterApprovalNoticeRecovery);
        self.notice_recovery.replace(Some(hold));
        Ok(())
    }

    async fn await_notice_recovery(&self) -> Result<()> {
        let mut hold = self
            .notice_recovery
            .take()
            .ok_or("notice recovery pause is not armed")?;
        hold.until_held().await;
        hold.release();
        Ok(())
    }

    async fn fail_next_applying_write(&self) -> Result<()> {
        let owner = self.link.host.central(&self.link.organization_id).await?;
        owner
            .runtime()
            .statement_host()
            .fail_next_approval_after_write();
        Ok(())
    }

    async fn decide_provider_approval(
        &self,
        request_id: &workspace_ids::ApprovalRequestId,
    ) -> Result<()> {
        let mut decision = fixture_decision(request_id)?;
        decision.answer = collaboration_central_core::FixtureApprovalAnswer::ProviderApproval;
        Ok(self
            .link
            .host
            .fixture_decide_approval(&self.link.organization_id, decision)
            .await?)
    }

    async fn hold_delivery_before_write(&self) -> Result<()> {
        let runtime = self.link.host.central(&self.link.organization_id).await?;
        let held = runtime
            .runtime()
            .pause_points()
            .hold_central(PausePoint::BeforeApprovalEndingEnqueue);
        self.before_write.replace(Some(held));
        Ok(())
    }

    async fn wait_delivery_before_write(&self) -> Result<()> {
        let mut held = self
            .before_write
            .take()
            .ok_or("approval send was not held")?;
        held.until_held().await;
        self.before_write.replace(Some(held));
        Ok(())
    }

    async fn release_delivery_before_write(&self) -> Result<()> {
        self.before_write
            .take()
            .ok_or("approval send was not held")?
            .release();
        Ok(())
    }

    async fn hold_delivery_answer(&self) -> Result<()> {
        let owner = self.link.host.central(&self.link.organization_id).await?;
        let held = owner
            .runtime()
            .pause_points()
            .hold_central(PausePoint::ApprovalEndingAnswerApplied);
        self.delivery_answer.replace(Some(held));
        Ok(())
    }

    async fn observe_delivery_answer(&self) -> Result<()> {
        let mut held = self
            .delivery_answer
            .take()
            .ok_or("delivery-answer pause was not armed")?;
        held.until_held().await;
        held.release();
        Ok(())
    }

    async fn end_enrollment_through_login(
        &self,
        event: collaboration_central_conformance::approval_link_scenarios::EnrollmentLoginEnd,
    ) -> Result<()> {
        approval_login_settlement::settle_enrollment(self, event).await
    }

    async fn end_enrollment(&self) -> Result<()> {
        let owner = self.link.host.central(&self.link.organization_id).await?;
        let ended = stored_ending_batch(
            INSTALLATION,
            self.link.enrollment_id.as_str(),
            EnrollmentAtApply::OwnerRevoked
                .stored_end_reason()
                .ok_or("revoke end reason")?,
        )?;
        if !matches!(
            owner.runtime().statement_host().run_batch(ended).await?,
            central_storage::BatchOutcome::Applied { .. }
        ) {
            return Err("approval fixture enrollment did not end".into());
        }
        Ok(())
    }
}

#[tokio::test]
async fn tokio_approval_raise_replay_and_stop_use_the_store() -> Result<()> {
    let fixture = NativeApprovalFixture {
        link: start_session_link_fixture(signed_clock()?).await?,
        before_alarm: Cell::new(0),
        delivery_answer: RefCell::default(),
        before_write: RefCell::default(),
        notice_recovery: RefCell::default(),
    };
    let result = registered_stop_and_replay(&fixture).await;
    fixture.link.close().await?;
    result
}

#[tokio::test]
async fn tokio_approval_authority_refusals_write_nothing() -> Result<()> {
    let fixture = NativeApprovalFixture {
        link: start_session_link_fixture(signed_clock()?).await?,
        before_alarm: Cell::new(0),
        delivery_answer: RefCell::default(),
        before_write: RefCell::default(),
        notice_recovery: RefCell::default(),
    };
    let result = approval_authority_refusals_leave_store_unchanged(&fixture).await;
    fixture.link.close().await?;
    result
}

#[tokio::test]
async fn tokio_engine_delivers_or_recovers_approval_timeout_and_applies_stop_owed() -> Result<()> {
    for scenario in [
        DeadlineDelivery::Connected,
        DeadlineDelivery::RecoverAfterReconnect,
        DeadlineDelivery::StopOwedAtCapacity,
    ] {
        let fixture = NativeApprovalFixture {
            link: start_session_link_fixture(signed_clock()?).await?,
            before_alarm: Cell::new(0),
            delivery_answer: RefCell::default(),
            before_write: RefCell::default(),
            notice_recovery: RefCell::default(),
        };
        let result = approval_deadline_delivery(&fixture, scenario).await;
        fixture.link.close().await?;
        result?;
    }
    Ok(())
}

use collaboration_central_conformance::approval_link_scenarios::{
    FIXTURE_DECIDER, FIXTURE_DECISION_OPERATION, heartbeat, next_ending, read_request,
    receive_fixture_decision, register_run, registered_waiting_request, require_owed,
};
use collaboration_central_core::FixtureApprovalDecision;
use fleet_runtime::PausePoint;
use router_clock::RuntimeClock;

fn fixture_decision(
    request_id: &workspace_ids::ApprovalRequestId,
) -> Result<FixtureApprovalDecision> {
    Ok(FixtureApprovalDecision {
        answer: collaboration_central_core::FixtureApprovalAnswer::HeldStart,
        request_id: request_id.clone(),
        operation_id: FIXTURE_DECISION_OPERATION.parse()?,
        person_id: FIXTURE_DECIDER.parse()?,
    })
}

#[tokio::test]
async fn tokio_fixture_decide_offers_only_after_commit_and_records_received() -> Result<()> {
    let fixture = NativeApprovalFixture {
        link: start_session_link_fixture(signed_clock()?).await?,
        before_alarm: Cell::new(0),
        delivery_answer: RefCell::default(),
        before_write: RefCell::default(),
        notice_recovery: RefCell::default(),
    };
    let observed = async {
        let (target, mut socket, request) = registered_waiting_request(&fixture).await?;
        let host = &fixture.link.host;
        let runtime = host.central(&target.organization_id).await?;
        let mut before_commit = runtime
            .runtime()
            .statement_host()
            .hold_before_approval_commit();
        fixture.fail_next_applying_write().await?;
        let decide = host.fixture_decide_approval(
            &target.organization_id,
            fixture_decision(request.request_id())?,
        );
        let observe = async {
            before_commit.until_held().await;
            let result = heartbeat(&mut socket).await;
            before_commit.release();
            result
        };
        let (failed, no_offer) = tokio::join!(decide, observe);
        no_offer?;
        if failed.is_ok() {
            let (_, ending) = next_ending(&mut socket, &target).await?;
            return Err(format!(
                "uncommitted approval ending reached the holder for request {}",
                ending.request_id(),
            )
            .into());
        }
        if !matches!(failed, Err(central_storage::StoreFailure::Unavailable)) {
            return Err(format!("expected a known rollback, got {failed:?}").into());
        }
        heartbeat(&mut socket).await?;
        let row = read_request(&fixture, &request).await?;
        if row != [SqlValue::Text("waiting".to_owned()), SqlValue::Null] {
            return Err(format!("rollback changed the waiting row: {row:?}").into());
        }
        host.fixture_decide_approval(
            &target.organization_id,
            fixture_decision(request.request_id())?,
        )
        .await?;
        receive_fixture_decision(&fixture, &mut socket, target, &request).await
    }
    .await;
    fixture.link.close().await?;
    observed
}

#[tokio::test]
async fn tokio_queued_approval_ending_never_writes_on_a_replacement() -> Result<()> {
    let fixture = NativeApprovalFixture {
        link: start_session_link_fixture(signed_clock()?).await?,
        before_alarm: Cell::new(0),
        delivery_answer: RefCell::default(),
        before_write: RefCell::default(),
        notice_recovery: RefCell::default(),
    };
    let observed = async {
        let (target, _original, request) = registered_waiting_request(&fixture).await?;
        let host = &fixture.link.host;
        let runtime = host.central(&target.organization_id).await?;
        let pauses = runtime.runtime().pause_points();
        let mut before_write = pauses.hold_central(PausePoint::BeforeApprovalEndingWrite);
        let mut never_reached = pauses.hold_central(PausePoint::ApprovalEndingNeverReached);
        host.fixture_decide_approval(
            &target.organization_id,
            fixture_decision(request.request_id())?,
        )
        .await?;
        before_write.until_held().await;

        let mut replacement = fixture.connect().await?;
        before_write.release();
        never_reached.until_held().await;
        heartbeat(&mut replacement).await?;
        require_owed(&fixture, &request).await?;
        never_reached.release();

        let registered = register_run(
            &mut replacement,
            &target.organization_id,
            &target.router_run,
        )
        .await?;
        if registered != (RegisterRunReply::Registered {}) {
            return Err(format!("replacement's registration was {registered:?}").into());
        }
        receive_fixture_decision(&fixture, &mut replacement, target, &request).await
    }
    .await;
    fixture.link.close().await?;
    observed
}

#[tokio::test]
async fn tokio_provider_session_end_and_routing() -> Result<()> {
    let fixture = NativeApprovalFixture {
        link: start_session_link_fixture(signed_clock()?).await?,
        before_alarm: Cell::new(0),
        delivery_answer: RefCell::default(),
        before_write: RefCell::default(),
        notice_recovery: RefCell::default(),
    };
    let result = collaboration_central_conformance::approval_provider_scenarios::provider_session_end_and_refusals(&fixture).await;
    fixture.link.close().await?;
    result
}

#[tokio::test]
async fn tokio_provider_deadline_precedes_actions_and_ends_on_cadence() -> Result<()> {
    use collaboration_central_conformance::approval_provider_scenarios::{
        ProviderAsk, ProviderExpiryTrigger, provider_deadline,
    };
    for (ask, trigger) in [
        (ProviderAsk::Approval, ProviderExpiryTrigger::Stop),
        (ProviderAsk::Question, ProviderExpiryTrigger::Engine),
        (ProviderAsk::Approval, ProviderExpiryTrigger::Registration),
    ] {
        let fixture = NativeApprovalFixture {
            link: start_session_link_fixture(signed_clock()?).await?,
            before_alarm: Cell::new(0),
            delivery_answer: RefCell::default(),
            before_write: RefCell::default(),
            notice_recovery: RefCell::default(),
        };
        let result = provider_deadline(&fixture, ask, trigger).await;
        fixture.link.close().await?;
        result?;
    }
    Ok(())
}

#[tokio::test]
async fn tokio_approval_calls_are_bound_to_the_registered_carrier() -> Result<()> {
    let fixture = NativeApprovalFixture {
        link: start_session_link_fixture(signed_clock()?).await?,
        before_alarm: Cell::new(0),
        delivery_answer: RefCell::default(),
        before_write: RefCell::default(),
        notice_recovery: RefCell::default(),
    };
    let result =
        collaboration_central_conformance::approval_authority_scenarios::approval_carrier_binding(
            &fixture,
        )
        .await;
    fixture.link.close().await?;
    result
}

#[tokio::test]
async fn tokio_enrollment_end_settles_the_approval_run() -> Result<()> {
    use collaboration_central_conformance::approval_link_scenarios::EnrollmentLoginEnd;
    for (event, overdue) in [
        (EnrollmentLoginEnd::Revoke, false),
        (EnrollmentLoginEnd::Revoke, true),
        (EnrollmentLoginEnd::Supersede, false),
        (EnrollmentLoginEnd::Idle, false),
    ] {
        let fixture = NativeApprovalFixture {
            link: start_session_link_fixture(signed_clock()?).await?,
            before_alarm: Cell::new(0),
            delivery_answer: RefCell::default(),
            before_write: RefCell::default(),
            notice_recovery: RefCell::default(),
        };
        let result = collaboration_central_conformance::approval_authority_scenarios::enrollment_end_settles_approval_run(&fixture, event, overdue).await;
        fixture.link.close().await?;
        result?;
    }
    Ok(())
}

#[tokio::test]
async fn tokio_durable_floor_wins_after_clock_rollback() -> Result<()> {
    let fixture = NativeApprovalFixture {
        link: start_session_link_fixture(signed_clock()?).await?,
        before_alarm: Cell::new(0),
        delivery_answer: RefCell::default(),
        before_write: RefCell::default(),
        notice_recovery: RefCell::default(),
    };
    let result = collaboration_central_conformance::approval_authority_scenarios::durable_floor_wins_after_clock_rollback(&fixture).await;
    fixture.link.close().await?;
    result
}

#[tokio::test]
async fn tokio_corrupt_approval_charges_only_its_own_attempt() -> Result<()> {
    let fixture = NativeApprovalFixture {
        link: start_session_link_fixture(signed_clock()?).await?,
        before_alarm: Cell::new(0),
        delivery_answer: RefCell::default(),
        before_write: RefCell::default(),
        notice_recovery: RefCell::default(),
    };
    let result = collaboration_central_conformance::approval_authority_scenarios::corrupt_approval_charges_only_its_own_attempt(&fixture).await;
    fixture.link.close().await?;
    result
}

#[tokio::test]
async fn tokio_renewing_the_same_holder_socket_keeps_the_pending_ending() -> Result<()> {
    use collaboration_central_conformance::approval_provider_scenarios::{
        RenewalDuringDelivery, same_socket_renewal_keeps_delivery,
    };
    for timing in [
        RenewalDuringDelivery::BeforeWrite,
        RenewalDuringDelivery::BeforeAnswer,
    ] {
        let fixture = NativeApprovalFixture {
            link: start_session_link_fixture(signed_clock()?).await?,
            before_alarm: Cell::new(0),
            delivery_answer: RefCell::default(),
            before_write: RefCell::default(),
            notice_recovery: RefCell::default(),
        };
        let result = same_socket_renewal_keeps_delivery(&fixture, timing).await;
        fixture.link.close().await?;
        result?;
    }
    Ok(())
}

#[path = "approval_carrier_replacement.rs"]
mod approval_carrier_replacement;

#[path = "parent_approval_calls.rs"]
mod parent_approval_calls;
