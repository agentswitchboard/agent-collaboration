#[path = "approval_login_settlement.rs"]
mod approval_login_settlement;

use super::fixture::{ConnectedLink, Fixture, MACHINE_ID, receive, send};
use central_link_protocol::{
    CentralLinkRpc, DataFrame, TypedCallResult,
    approval_rpc::{ApprovalServiceTarget, RegisterRunReply, RegisterRunRpc},
};
use central_storage::{SqlValue, Statement};
use collaboration_central_conformance::ConformanceResult as Result;
use collaboration_central_conformance::approval_authority_scenarios::approval_authority_refusals_leave_store_unchanged;
use fleet_rpc::Envelope;
use serde_json::json;

const RUN: &str = "019f0000-0000-7000-8000-000000005401";

#[tokio::test]
async fn production_link_registers_run_in_workerds_owning_approval_store() -> Result<()> {
    let fixture = Fixture::with_production_dispatcher().await?;
    let observed = async {
        let mut link = ConnectedLink::open(&fixture).await?;
        let store = fixture.runtime();
        let stored_run = Statement {
            sql: "SELECT organization_id,machine_id,enrollment_id,router_run_id FROM approval_router_runs ORDER BY registration_order",
            args: vec![],
        };
        if !store.read(stored_run.clone()).await?.rows.is_empty() {
            return Err("the fixture must start without a registered run".into());
        }

        let correlation: central_link_protocol::CorrelationId =
            "019f0000-0000-7000-8000-000000005402".parse()?;
        let target = ApprovalServiceTarget { organization_id: fixture.organization.clone() };
        let request = RegisterRunRpc::request(Envelope {
            target: target.clone(),
            body: serde_json::from_value(json!({ "routerRun": RUN }))?,
        });
        send(&mut link.socket, &DataFrame::CallCentral {
            correlation_id: correlation.clone(),
            request: Box::new(request),
        }).await?;

        let actual = receive(&mut link.socket).await?;
        let expected = DataFrame::CentralCallReply {
            correlation_id: correlation,
            result: Box::new(TypedCallResult::Answered {
                reply: RegisterRunRpc::reply(Envelope { target, body: RegisterRunReply::Registered {} }),
            }),
        };
        if actual != expected {
            return Err(format!("registered run reply was {actual:?}, expected {expected:?}").into());
        }
        let expected = [fixture.organization.as_str(), MACHINE_ID, fixture.enrollment.as_str(), RUN]
            .map(|field| SqlValue::Text(field.to_owned()));
        if store.read(stored_run).await?.rows != [expected] {
            return Err("RegisterRun did not commit the exact admitted run row".into());
        }
        link.close().await
    }.await;
    fixture.close().await?;
    observed
}

use central_link_protocol::{LinkFrame, approval_rpc::ApprovalHolderTarget};
use collaboration_central_conformance::{
    approval_link_scenarios::{
        ApprovalLinkFixture, DeadlineDelivery, ROUTER_RUN, approval_deadline_delivery,
        receive_frame, registered_stop_and_replay,
    },
    signin_fixtures as signed,
};
use fleet_contracts::MachineTokenDecision;
use reqwest_websocket::{RequestBuilderExt, WebSocket};
use router_clock::RuntimeClock;

impl ApprovalLinkFixture for Fixture {
    fn now_seconds(&self) -> i64 {
        self.clock.utc_now().timestamp()
    }

    fn target(&self) -> Result<ApprovalHolderTarget> {
        Ok(ApprovalHolderTarget {
            organization_id: self.organization.clone(),
            machine_id: MACHINE_ID.parse()?,
            enrollment_id: self.enrollment.clone(),
            router_run: ROUTER_RUN.parse()?,
        })
    }

    async fn connect(&self) -> Result<WebSocket> {
        let token = self.refresh().await?;
        let mut socket = self
            .client
            .get(self.link_url(self.organization.as_str()))
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

    async fn reopen_after_clock_rollback(&self) -> Result<()> {
        self.host.c2_evict(&self.organization).await?;
        let _ = self.host.central(&self.organization).await;
        Ok(())
    }

    async fn set_wall_seconds(&self, seconds: i64) -> Result<()> {
        let time = self.clock.utc_now();
        self.clock
            .set_utc(time + chrono::Duration::seconds(seconds - time.timestamp()));
        self.host.set_external_time_ms(seconds * 1000).await?;
        if !self
            .runtime()
            .control::<bool>("clock", &(seconds * 1000))
            .await?
        {
            return Err("worker did not set the corrected wall clock".into());
        }
        Ok(())
    }

    async fn fixture_write(&self, statement: Statement) -> Result<()> {
        let result = self
            .runtime()
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
        Ok(self.runtime().read(statement).await?)
    }

    async fn advance(&self, seconds: u64) -> Result<()> {
        super::renewal::advance(self, seconds).await
    }

    async fn refresh(&self) -> Result<String> {
        let grant = super::fixture::grant(&self.host, &self.client, &self.enrollment).await?;
        let MachineTokenDecision::Grant {
            decided_at,
            grant_expires_at,
            ..
        } = grant
        else {
            return Err("the live enrollment could not refresh its grant".into());
        };
        signed::machine_token(
            self.organization.as_str(),
            MACHINE_ID,
            self.enrollment.as_str(),
            decided_at.0,
            grant_expires_at.0,
        )
    }

    async fn observe_due_alarm(&self) -> Result<()> {
        let runtime = self.runtime();
        if runtime.dispatch_due_alarm().await?.is_none() {
            return Err("the object did not dispatch a due approval alarm".into());
        }
        Ok(())
    }

    async fn fail_next_applying_write(&self) -> Result<()> {
        if !self
            .runtime()
            .control::<bool>("fail-approval-after-write", &true)
            .await?
        {
            return Err("could not arm approval apply rollback".into());
        }
        Ok(())
    }

    async fn decide_provider_approval(
        &self,
        request_id: &workspace_ids::ApprovalRequestId,
    ) -> Result<()> {
        let result: bool = self
            .runtime()
            .control(
                "approval-fixture-decide",
                &json!({
                    "requestId": request_id.as_str(),
                    "operationId": FIXTURE_DECISION_OPERATION,
                    "personId": FIXTURE_DECIDER,
                    "answer": "providerApproval",
                }),
            )
            .await?;
        if !result {
            return Err("provider approval was not decided".into());
        }
        Ok(())
    }

    async fn hold_delivery_before_write(&self) -> Result<()> {
        self.delivery_write_control("hold-central").await
    }

    async fn wait_delivery_before_write(&self) -> Result<()> {
        self.delivery_write_control("until-held-central").await
    }

    async fn release_delivery_before_write(&self) -> Result<()> {
        self.delivery_write_control("release-central").await
    }

    async fn hold_delivery_answer(&self) -> Result<()> {
        if !self
            .runtime()
            .control::<bool>("hold-central", &"approvalEndingAnswerApplied")
            .await?
        {
            return Err("could not hold the worker after applying the delivery answer".into());
        }
        Ok(())
    }

    async fn observe_delivery_answer(&self) -> Result<()> {
        let runtime = self.runtime();
        let point = "approvalEndingAnswerApplied";
        if !runtime
            .control::<bool>("until-held-central", &point)
            .await?
            || !runtime.control::<bool>("release-central", &point).await?
        {
            return Err("the worker did not apply the delivery answer".into());
        }
        Ok(())
    }

    async fn end_enrollment_through_login(
        &self,
        event: collaboration_central_conformance::approval_link_scenarios::EnrollmentLoginEnd,
    ) -> Result<()> {
        approval_login_settlement::settle_enrollment(self, event).await
    }

    async fn end_enrollment(&self) -> Result<()> {
        use super::receipt_frames::{EnrollmentAtApply, stored_ending_batch};
        let ended = stored_ending_batch(
            MACHINE_ID,
            self.enrollment.as_str(),
            EnrollmentAtApply::OwnerRevoked
                .stored_end_reason()
                .ok_or("revoke end reason")?,
        )?;
        if !matches!(
            self.runtime().run_batch(ended).await?,
            central_storage::BatchOutcome::Applied { .. }
        ) {
            return Err("approval fixture enrollment did not end".into());
        }
        Ok(())
    }
}

#[tokio::test]
async fn workerd_approval_raise_replay_and_stop_use_the_store() -> Result<()> {
    let fixture = Fixture::with_production_dispatcher().await?;
    let result = registered_stop_and_replay(&fixture).await;
    fixture.close().await?;
    result
}

#[tokio::test]
async fn workerd_approval_authority_refusals_write_nothing() -> Result<()> {
    let fixture = Fixture::with_production_dispatcher().await?;
    let result = approval_authority_refusals_leave_store_unchanged(&fixture).await;
    fixture.close().await?;
    result
}

#[tokio::test]
async fn workerd_engine_delivers_or_recovers_approval_timeout_and_applies_stop_owed() -> Result<()>
{
    for scenario in [
        DeadlineDelivery::Connected,
        DeadlineDelivery::RecoverAfterReconnect,
        DeadlineDelivery::StopOwedAtCapacity,
    ] {
        let fixture = Fixture::with_production_dispatcher().await?;
        let result = approval_deadline_delivery(&fixture, scenario).await;
        fixture.close().await?;
        result?;
    }
    Ok(())
}

use collaboration_central_conformance::approval_link_scenarios::{
    FIXTURE_DECIDER, FIXTURE_DECISION_OPERATION, heartbeat, receive_fixture_decision, register_run,
    registered_waiting_request, require_owed,
};

#[tokio::test]
async fn workerd_fixture_decide_delivers_and_records_received() -> Result<()> {
    let fixture = Fixture::with_production_dispatcher().await?;
    let observed = async {
        let (target, mut socket, request) = registered_waiting_request(&fixture).await?;
        decide_fixture_request(&fixture, request.request_id().as_str()).await?;
        receive_fixture_decision(&fixture, &mut socket, target, &request).await
    }
    .await;
    fixture.close().await?;
    observed
}

#[tokio::test]
async fn workerd_in_object_drop_before_first_write_leaves_approval_owed() -> Result<()> {
    workerd_approval_loses_first_write(FirstWriteLoss::DropInObject).await
}

#[tokio::test]
async fn workerd_queued_approval_ending_never_writes_on_a_replacement() -> Result<()> {
    workerd_approval_loses_first_write(FirstWriteLoss::ReplaceSocket).await
}

enum FirstWriteLoss {
    DropInObject,
    ReplaceSocket,
}

async fn workerd_approval_loses_first_write(loss: FirstWriteLoss) -> Result<()> {
    let fixture = Fixture::with_production_dispatcher().await?;
    let observed = async {
        let (target, original, request) = registered_waiting_request(&fixture).await?;
        let runtime = fixture.runtime();
        assert!(
            runtime
                .control::<bool>("hold-central", &"beforeApprovalEndingWrite")
                .await?
        );
        assert!(
            runtime
                .control::<bool>("hold-central", &"approvalEndingNeverReached")
                .await?
        );
        decide_fixture_request(&fixture, request.request_id().as_str()).await?;
        assert!(
            runtime
                .control::<bool>("until-held-central", &"beforeApprovalEndingWrite")
                .await?
        );

        let mut recovery_socket = match loss {
            FirstWriteLoss::DropInObject => {
                assert!(
                    runtime
                        .control::<bool>("drop-approval-before-write", &true)
                        .await?
                );
                original
            }
            FirstWriteLoss::ReplaceSocket => {
                let replacement = fixture.connect().await?;
                drop(original);
                replacement
            }
        };
        assert!(
            runtime
                .control::<bool>("release-central", &"beforeApprovalEndingWrite")
                .await?
        );
        assert!(
            runtime
                .control::<bool>("until-held-central", &"approvalEndingNeverReached")
                .await?
        );
        heartbeat(&mut recovery_socket).await?;
        require_owed(&fixture, &request).await?;
        assert!(
            runtime
                .control::<bool>("release-central", &"approvalEndingNeverReached")
                .await?
        );

        assert_eq!(
            register_run(
                &mut recovery_socket,
                &target.organization_id,
                &target.router_run
            )
            .await?,
            RegisterRunReply::Registered {},
        );
        receive_fixture_decision(&fixture, &mut recovery_socket, target, &request).await
    }
    .await;
    fixture.close().await?;
    observed
}

async fn decide_fixture_request(fixture: &Fixture, request_id: &str) -> Result<()> {
    assert!(
        fixture
            .runtime()
            .control::<bool>(
                "approval-fixture-decide",
                &json!({
                    "requestId": request_id,
                    "operationId": FIXTURE_DECISION_OPERATION,
                    "personId": FIXTURE_DECIDER,
                })
            )
            .await?
    );
    Ok(())
}

#[tokio::test]
async fn workerd_provider_session_end_and_routing() -> Result<()> {
    let fixture = Fixture::with_production_dispatcher().await?;
    let result = collaboration_central_conformance::approval_provider_scenarios::provider_session_end_and_refusals(&fixture).await;
    fixture.close().await?;
    result
}

#[tokio::test]
async fn workerd_provider_deadline_precedes_actions_and_ends_on_cadence() -> Result<()> {
    use collaboration_central_conformance::approval_provider_scenarios::{
        ProviderAsk, ProviderExpiryTrigger, provider_deadline,
    };
    for (ask, trigger) in [
        (ProviderAsk::Approval, ProviderExpiryTrigger::Stop),
        (ProviderAsk::Question, ProviderExpiryTrigger::Engine),
        (ProviderAsk::Approval, ProviderExpiryTrigger::Registration),
    ] {
        let fixture = Fixture::with_production_dispatcher().await?;
        let result = provider_deadline(&fixture, ask, trigger).await;
        fixture.close().await?;
        result?;
    }
    Ok(())
}

#[tokio::test]
async fn workerd_approval_calls_are_bound_to_the_registered_carrier() -> Result<()> {
    let fixture = Fixture::with_production_dispatcher().await?;
    let result =
        collaboration_central_conformance::approval_authority_scenarios::approval_carrier_binding(
            &fixture,
        )
        .await;
    fixture.close().await?;
    result
}

#[tokio::test]
async fn workerd_enrollment_end_settles_the_approval_run() -> Result<()> {
    use collaboration_central_conformance::approval_link_scenarios::EnrollmentLoginEnd;
    for (event, overdue) in [
        (EnrollmentLoginEnd::Revoke, false),
        (EnrollmentLoginEnd::Revoke, true),
        (EnrollmentLoginEnd::Supersede, false),
        (EnrollmentLoginEnd::Idle, false),
    ] {
        let fixture = Fixture::with_production_dispatcher().await?;
        let result = collaboration_central_conformance::approval_authority_scenarios::enrollment_end_settles_approval_run(&fixture, event, overdue).await;
        fixture.close().await?;
        result?;
    }
    Ok(())
}

#[tokio::test]
async fn workerd_durable_floor_wins_after_clock_rollback() -> Result<()> {
    let fixture = Fixture::with_production_dispatcher().await?;
    let result = collaboration_central_conformance::approval_authority_scenarios::durable_floor_wins_after_clock_rollback(&fixture).await;
    fixture.close().await?;
    result
}

#[tokio::test]
async fn workerd_corrupt_approval_charges_only_its_own_attempt() -> Result<()> {
    let fixture = Fixture::with_production_dispatcher().await?;
    let result = collaboration_central_conformance::approval_authority_scenarios::corrupt_approval_charges_only_its_own_attempt(&fixture).await;
    fixture.close().await?;
    result
}

impl Fixture {
    async fn delivery_write_control(&self, operation: &str) -> Result<()> {
        if !self
            .runtime()
            .control::<bool>(operation, &"beforeApprovalEndingWrite")
            .await?
        {
            return Err(format!("worker delivery write control {operation} failed").into());
        }
        Ok(())
    }
}

#[tokio::test]
async fn workerd_renewing_the_same_holder_socket_keeps_the_pending_ending() -> Result<()> {
    use collaboration_central_conformance::approval_provider_scenarios::{
        RenewalDuringDelivery, same_socket_renewal_keeps_delivery,
    };
    for timing in [
        RenewalDuringDelivery::BeforeWrite,
        RenewalDuringDelivery::BeforeAnswer,
    ] {
        let fixture = Fixture::with_production_dispatcher().await?;
        let result = same_socket_renewal_keeps_delivery(&fixture, timing).await;
        fixture.close().await?;
        result?;
    }
    Ok(())
}

#[path = "approval_carrier_replacement.rs"]
mod approval_carrier_replacement;

#[path = "approval_calls/alarm_mark_race.rs"]
mod alarm_mark_race;
