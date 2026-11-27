//! Restore recovery with both the Host delivery route and real supervisor.

use crate::external_provider_supervisor::{RestoreRig, wait_request};
use crate::{
    ExternalProviderSupervisor, LiveSessionOwnership, LiveSessionOwnershipCheck,
    ProviderAcpDeliveryRoute,
};
use collaboration_contracts::{
    DeliveryPrecondition, LoadPolicy, ProviderConversationBackend, SessionDeliveryRoute,
    UnstoredAttemptEvidenceSink,
    layer_zero::{DeliveryRequest, PreparedPush},
};
use collaboration_protocol::{
    AttemptId, ChannelDescription, ConversationOperationSettlement,
    ConversationOperationWaitOutput, DeliveryCorrelationId, DeliveryOutcome, EndpointAvailability,
    EndpointDescription, MessageDelivery, MessageText, NonEmptyText, ObservationTimestamp,
    OperationId, ProviderOperationEffect, PushId, SessionRef,
};
use collaboration_runtime::EndpointDirectory;
use router_clock::ClockInstant;
use std::sync::Arc;
use switchboard_session_store::ProviderOperationStore;
use tokio::sync::Mutex;

struct NoLivePeer;

impl LiveSessionOwnershipCheck for NoLivePeer {
    fn check<'a>(
        &'a self,
        _target: &'a SessionRef,
    ) -> collaboration_contracts::DeliveryFuture<'a, LiveSessionOwnership> {
        Box::pin(std::future::ready(Ok(LiveSessionOwnership::NotLive)))
    }
}

fn delivery_route(
    rig: &RestoreRig,
    host: Arc<ExternalProviderSupervisor>,
    store: Arc<Mutex<ProviderOperationStore>>,
) -> ProviderAcpDeliveryRoute {
    let directory = EndpointDirectory::new(rig.target.endpoint.service_id.clone());
    let binding = host.binding(&rig.target.endpoint).expect("fixture binding");
    directory
        .publish(EndpointDescription {
            endpoint: rig.target.endpoint.clone(),
            label: NonEmptyText::try_from("Cursor fixture".to_owned()).expect("label"),
            availability: EndpointAvailability::Available {
                observed_at: ObservationTimestamp::try_from("2026-11-19T12:00:00Z".to_owned())
                    .expect("timestamp"),
            },
            channels: vec![ChannelDescription::ExternalProvider {
                transport: binding.transport,
                binding_id: binding.binding_id,
                binding_generation: binding.generation.generation,
                runtime: binding.runtime,
                capabilities: binding.capabilities,
                auto_realization: None,
            }],
        })
        .expect("endpoint");
    ProviderAcpDeliveryRoute::new(
        rig.target.endpoint.service_id.clone(),
        std::iter::once(rig.target.endpoint.clone()).collect(),
        directory,
        host,
        store,
        Arc::new(NoLivePeer),
        rig.clock.clone(),
    )
}

fn auto_message(target: SessionRef, text: &str, load_policy: LoadPolicy) -> DeliveryRequest {
    let push_id = PushId::try_from(AttemptId::generate().as_str().to_owned()).expect("push ID");
    let correlation =
        DeliveryCorrelationId::try_from(push_id.as_str().to_owned()).expect("correlation");
    DeliveryRequest {
        payload: PreparedPush {
            line: MessageText::try_from(format!(
                "✉️ sender · \"{text}\" · router://{}/push/{}",
                String::from(target.endpoint.service_id.clone()),
                push_id.as_str()
            ))
            .expect("prepared input"),
            push_id,
            load_policy,
        },
        target,
        mode: MessageDelivery::Auto,
        precondition: DeliveryPrecondition::Unpinned,
        correlation,
        attempt: AttemptId::generate(),
    }
}

#[tokio::test]
async fn a_cold_auto_restore_timeout_finishes_the_input_without_holding_it() {
    let rig = RestoreRig::new();
    rig.seed_session().await;
    let wire = tokio::net::UnixListener::bind(rig.root.path().join("wire.json.restore.sock"))
        .expect("bind held restore observation");
    let (host, store) = rig
        .open_with_store_and_behaviors(&["silent-first-restore", "observe-first-held-restore"])
        .await;
    let host = Arc::new(host);
    let route = delivery_route(&rig, host.clone(), store);
    let not_loaded = route
        .deliver(
            auto_message(rig.target.clone(), "load forbidden", LoadPolicy::LoadedOnly),
            &UnstoredAttemptEvidenceSink,
        )
        .await
        .expect("loaded-only receipt");
    assert!(matches!(
        not_loaded.receipt().outcome,
        DeliveryOutcome::NotSubmitted {
            retryable: true,
            ..
        }
    ));
    assert!(
        !rig.root.path().join("wire.json").exists(),
        "the loaded-only attempt reached ACP"
    );

    let checkpoint = host
        .runtime_for(&rig.target.endpoint)
        .expect("fixture runtime")
        .observe_provider_dispatch_for_fixture(
            acp_client_runtime::owner_wire::FixtureDispatchPoint::Restore,
        );
    let mut cold_delivery = Box::pin(route.deliver(
        auto_message(rig.target.clone(), "retire this input", LoadPolicy::MayLoad),
        &UnstoredAttemptEvidenceSink,
    ));
    let timeout = std::time::Duration::from_secs(30);
    let fenced = tokio::select! {
        completed = checkpoint => matches!(completed, Ok(Ok(()))),
        _receipt = &mut cold_delivery => false,
    };
    if !fenced {
        drop(cold_delivery);
        route.shutdown_queue().await;
        host.shutdown().await.expect("stop Host");
        panic!("the completed restore attempt must put its independent fence on the ordered wire");
    }
    use tokio::io::AsyncReadExt as _;
    let marker = async {
        let (mut fixture, _) = wire.accept().await?;
        fixture.read_u8().await
    };
    let observed = tokio::select! {
        marker = marker => marker.ok(),
        _receipt = &mut cold_delivery => None,
    };
    let deadline_armed = rig
        .clock
        .has_not_due_check_now(ClockInstant::ZERO.after(timeout));
    if observed != Some(b'l') || !deadline_armed {
        drop(cold_delivery);
        route.shutdown_queue().await;
        host.shutdown().await.expect("stop Host");
        assert_eq!(
            observed,
            Some(b'l'),
            "session/load must reach the live provider before its ordered dispatch fence"
        );
        panic!("a held ACP restore must arm the 30-second Host deadline before settling");
    }
    rig.clock.advance(timeout).expect("reach restore deadline");
    let actual = cold_delivery.await.expect("terminal cold delivery receipt");
    assert!(
        matches!(actual.receipt().outcome, DeliveryOutcome::NotSubmitted { ref reason, retryable: false } if reason.contains("did not answer the restore in time, so retry this session")),
        "{actual:?}"
    );

    rig.assert_loads(&host).await;
    let next = auto_message(
        rig.target.clone(),
        "deliver this newer input",
        LoadPolicy::MayLoad,
    );
    let next_id =
        OperationId::try_from(next.attempt.as_str().to_owned()).expect("next operation ID");
    let sent = route
        .deliver(next, &UnstoredAttemptEvidenceSink)
        .await
        .expect("later input delivered");
    assert!(
        matches!(sent.receipt().outcome, DeliveryOutcome::Started),
        "{sent:?}"
    );
    let completed = host
        .wait(wait_request(next_id))
        .await
        .expect("later input settles");
    assert!(matches!(
        completed.output,
        ConversationOperationWaitOutput::Available {
            settlement: ConversationOperationSettlement::PromptCompleted { .. }
        }
    ));
    let observed: serde_json::Value = serde_json::from_slice(
        &std::fs::read(rig.root.path().join("wire.json")).expect("wire receipt"),
    )
    .expect("wire JSON");
    assert_eq!(
        observed["restores"]
            .as_array()
            .expect("restore requests")
            .len(),
        2
    );
    assert_eq!(
        observed["prompts"],
        serde_json::json!([rig.target.session_id])
    );
    route.shutdown_queue().await;
    host.shutdown().await.expect("stop Host");
}

#[tokio::test]
async fn a_cold_auto_load_with_failed_host_preparation_settles_without_holding_input() {
    let rig = RestoreRig::new();
    rig.seed_session().await;
    let (host, store) = rig.open_with_store("restored-in-auto").await;
    let host = Arc::new(host);
    let runtime = host.runtime_for(&rig.target.endpoint).expect("runtime");
    runtime.fault_restore_preparation();
    let route = delivery_route(&rig, host.clone(), store);

    let delivery = route
        .deliver(
            auto_message(rig.target.clone(), "do not hold me", LoadPolicy::MayLoad),
            &UnstoredAttemptEvidenceSink,
        )
        .await
        .expect("terminal cold input receipt");
    assert!(
        !runtime.has_restore_preparation_fault(),
        "the successful ACP reply reached Host preparation"
    );
    let observed: serde_json::Value = serde_json::from_slice(
        &std::fs::read(rig.root.path().join("wire.json")).expect("wire receipt"),
    )
    .expect("wire JSON");
    assert_eq!(
        observed["restores"]
            .as_array()
            .expect("restore requests")
            .len(),
        1
    );
    assert_eq!(
        observed
            .pointer("/restores/0/method")
            .and_then(serde_json::Value::as_str),
        Some("session/load")
    );
    assert_eq!(observed.get("prompts"), Some(&serde_json::json!([])));

    assert!(
        matches!(
            delivery.receipt().outcome,
            DeliveryOutcome::NotSubmitted { ref reason, retryable: false }
                if reason.contains("retry the restore")
        ),
        "a post-reply failure must finish the input with recovery guidance: {delivery:?}"
    );
    route.shutdown_queue().await;
    host.shutdown().await.expect("stop Host");
}

#[tokio::test]
async fn cold_prompt_and_compact_keep_the_post_reply_restore_guidance() {
    for compact in [false, true] {
        let rig = RestoreRig::new();
        rig.seed_session().await;
        let (host, store) = rig.open_with_store("restored-in-auto").await;
        let host = Arc::new(host);
        let runtime = host.runtime_for(&rig.target.endpoint).expect("runtime");
        runtime.fault_restore_preparation();
        let route = delivery_route(&rig, host.clone(), store);
        let actor = rig.target.clone().into();
        let input_id = session_event_model::InputId::generate();
        let result = if compact {
            route.compact(rig.target.clone(), actor, input_id).await
        } else {
            let prompt = session_event_model::ProviderSubmission::plain(vec![
                session_event_model::PromptContent::text("do not submit".to_owned())
                    .expect("prompt content"),
            ]);
            route
                .prompt_contents(rig.target.clone(), actor, input_id, prompt)
                .await
        };
        let failure = result.expect_err("the load's Host preparation failed");
        assert!(!runtime.has_restore_preparation_fault());
        assert!(
            matches!(failure, crate::ProviderPromptContentsError::RestoreFailed { ref reason }
                if reason.contains("retry the restore")),
            "{failure:?}"
        );
        route.shutdown_queue().await;
        host.shutdown().await.expect("stop Host");
    }
}

#[derive(Clone, Copy, Debug)]
enum FailedRestoreActivation {
    AutoLoad,
    QueueLoad,
    Resume,
}

#[tokio::test]
async fn a_failed_host_activation_retires_only_its_restore_and_never_sends_the_old_input() {
    use FailedRestoreActivation::{AutoLoad, QueueLoad, Resume};

    for path in [AutoLoad, QueueLoad, Resume] {
        let rig = RestoreRig::new();
        rig.seed_session().await;
        let (host, store) = rig.open_with_store("restored-in-auto").await;
        let host = Arc::new(host);
        let runtime = host.runtime_for(&rig.target.endpoint).expect("runtime");
        let route = delivery_route(&rig, host.clone(), store);
        let (reached, release) = runtime.pause_restore_preparation();
        let queued_accepted = tokio::sync::Notify::new();
        let restore = async {
            match path {
                AutoLoad | QueueLoad => {
                    let mut input =
                        auto_message(rig.target.clone(), "do not send", LoadPolicy::MayLoad);
                    if matches!(path, QueueLoad) {
                        input.mode = MessageDelivery::Queue;
                    }
                    let id =
                        OperationId::try_from(input.attempt.as_str().to_owned()).expect("input ID");
                    let delivery = route
                        .deliver(input, &UnstoredAttemptEvidenceSink)
                        .await
                        .expect("delivery receipt");
                    if matches!(path, QueueLoad) {
                        assert_eq!(delivery.receipt().outcome, DeliveryOutcome::Queued);
                        queued_accepted.notify_one();
                        let failure = host
                            .wait(wait_request(id))
                            .await
                            .expect_err("queued input ends unsent");
                        assert_eq!(failure.effect, ProviderOperationEffect::None);
                    } else {
                        assert!(
                            matches!(
                                delivery.receipt().outcome,
                                DeliveryOutcome::NotSubmitted {
                                    retryable: false,
                                    ..
                                }
                            ),
                            "{delivery:?}"
                        );
                    }
                }
                Resume => {
                    let id = OperationId::generate();
                    host.resume(rig.resume_request(id.clone()))
                        .await
                        .expect("resume admitted");
                    let failure = host
                        .wait(wait_request(id))
                        .await
                        .expect_err("resume fails at the Host");
                    assert_eq!(failure.effect, ProviderOperationEffect::Applied);
                }
            }
        };
        let fault = async {
            reached.await.expect("ACP prepared the restored actor");
            if matches!(path, QueueLoad) {
                queued_accepted.notified().await;
            }
            assert!(
                matches!(
                    host.owner_session_activity(&rig.target),
                    crate::ProviderSessionActivity::NotLoaded
                ),
                "{path:?}"
            );
            runtime.fault_restore_preparation();
            release.send(()).expect("continue activation");
        };
        let mut restore = Box::pin(restore);
        let mut fault = Box::pin(fault);
        let reached_preparation = tokio::select! {
            biased;
            () = &mut fault => true,
            () = &mut restore => false,
        };
        drop(fault);
        if !reached_preparation {
            drop(restore);
            route.shutdown_queue().await;
            host.shutdown().await.expect("stop Host");
            panic!("{path:?} settled before reaching Host restore preparation");
        }
        restore.await;

        assert!(
            matches!(
                host.owner_session_activity(&rig.target),
                crate::ProviderSessionActivity::NotLoaded
            ),
            "{path:?} left its generation live",
        );
        let wire = rig.wire_receipt();
        let expected_method = match path {
            AutoLoad | QueueLoad => "session/load",
            Resume => "session/resume",
        };
        assert_eq!(wire["restores"].as_array().expect("restores").len(), 1);
        assert_eq!(wire["restores"][0]["method"], expected_method);
        assert_eq!(wire["prompts"], serde_json::json!([]), "{path:?}");

        rig.assert_loads(&host).await;
        let next = auto_message(
            rig.target.clone(),
            "only send this new input",
            LoadPolicy::MayLoad,
        );
        let id = OperationId::try_from(next.attempt.as_str().to_owned()).expect("input ID");
        let sent = route
            .deliver(next, &UnstoredAttemptEvidenceSink)
            .await
            .expect("later input sent");
        assert_eq!(sent.receipt().outcome, DeliveryOutcome::Started);
        host.wait(wait_request(id))
            .await
            .expect("later input settles");
        let wire = rig.wire_receipt();
        assert_eq!(wire["restores"].as_array().expect("restores").len(), 2);
        assert_eq!(
            wire["prompts"],
            serde_json::json!([rig.target.session_id]),
            "{path:?}"
        );
        route.shutdown_queue().await;
        host.shutdown().await.expect("stop Host");
    }
}

mod writer_admission_tests {
    use super::*;
    use collaboration_contracts::{NativeRefusalCode, UnsentCause};

    const RETRY_RESTORE: &str = "the Router couldn't send the restore, so retry this session";

    #[tokio::test]
    async fn a_full_provider_writer_leaves_a_cold_delivery_holdable() {
        let rig = RestoreRig::new();
        rig.seed_session().await;
        let (host, store) = rig.open_with_store("restored-in-auto").await;
        let host = Arc::new(host);
        let owner = host
            .session_owner_for(&rig.target)
            .expect("registered session");
        let held_writer = owner
            .hold_provider_writer_capacity()
            .expect("real provider writer hold");
        let route = delivery_route(&rig, host.clone(), store);

        let result = route
            .deliver(
                auto_message(rig.target.clone(), "cold input", LoadPolicy::MayLoad),
                &UnstoredAttemptEvidenceSink,
            )
            .await
            .expect("cold delivery result");
        assert!(
            matches!(result.receipt().outcome,
                DeliveryOutcome::NotSubmitted { ref reason, retryable: true }
                    if reason == RETRY_RESTORE),
            "{result:?}",
        );
        assert_eq!(
            result.unsent_cause(),
            Some(UnsentCause::Holdable {
                otherwise: NativeRefusalCode::NativeUnavailable,
            })
        );
        assert!(
            !rig.root.path().join("wire.json").exists(),
            "no restore or input reached ACP"
        );

        drop(held_writer);
        rig.assert_loads(&host).await;
        host.shutdown().await.expect("fixture shutdown");
    }
}
