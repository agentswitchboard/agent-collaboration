//! The actual ordered receiver owns every admitted frame until it settles.

use super::*;
use crate::{
    owner_opening_instance::OpeningInstance,
    owner_session_open::{OpenReceiptFailure, OwnerOpenConfiguration},
    owner_session_setting::tests::ConnectionCatalogueBehavior,
};
use agent_client_protocol::{Agent, Client, ConnectionTo, Lines};
use futures_util::{FutureExt as _, SinkExt as _, sink, stream};
use provider_session_machines::{
    ProviderConfigOption, ProviderConfigValue, ProviderSettingChoice, ProviderSettingKind,
    ProviderSettingsCatalog, ProviderSettingsCatalogRecord,
    session_owner::{AuthorizationTarget, DispatchResult},
    session_setting_machine::SettingApplication,
};
use switchboard_harness_contracts::{ReservedReply, WorkReply};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub(crate) struct ReplyRecorder<Fact>(pub(crate) oneshot::Sender<Fact>);

impl<Fact: Send> ReservedReply<Fact> for ReplyRecorder<Fact> {
    fn send(self: Box<Self>, fact: Fact) {
        assert!(
            self.0.send(fact).is_ok(),
            "test still holds this exact reply"
        );
    }
}

pub(crate) async fn inert_connection() -> ConnectionTo<Agent> {
    Client
        .builder()
        .connect_with(
            Lines::new(
                sink::drain().sink_map_err(|impossible| match impossible {}),
                stream::empty::<std::io::Result<String>>(),
            ),
            async |connection| Ok(connection),
        )
        .await
        .expect("open an inert SDK connection for the stopped writer")
}

pub(crate) fn configuration() -> OwnerOpenConfiguration {
    OwnerOpenConfiguration {
        behavior: Arc::new(ConnectionCatalogueBehavior),
        capabilities: Default::default(),
        mcp_servers: Vec::new(),
        reasoning: Default::default(),
        auth_status: Arc::default(),
        observed_tool_calls: Arc::default(),
        #[cfg(feature = "test-observation")]
        reply_test_wire: None,
    }
}

pub(super) fn setting_plan(kind: ProviderSettingKind) -> OwnerSettingWirePlan {
    let catalogue = ProviderSettingsCatalog::from_record(Arc::new(ProviderSettingsCatalogRecord {
        modes: vec![ProviderSettingChoice {
            value: "ask".into(),
            label: "Ask".into(),
        }],
        config_options: vec![ProviderConfigOption {
            id: "fast".into(),
            name: "Fast".into(),
            category: Some(ProviderSettingKind::Fast),
            current_value: ProviderConfigValue::Boolean(true),
            choices: Vec::new(),
        }],
        ..Default::default()
    }));
    let wanted = if kind == ProviderSettingKind::Fast {
        "off"
    } else {
        "ask"
    };
    OwnerSettingWirePlan::from_work_catalog(
        "queued-session".to_owned(),
        &catalogue,
        kind,
        wanted.to_owned(),
        None,
    )
    .expect("fixture offers this exact setting")
}

#[tokio::test]
async fn stopped_writer_settles_each_queued_frame_once_as_unsent() {
    let (wire, output) = OwnerAcpWire::new(
        NonZeroUsize::new(16).expect("fixture capacity"),
        Arc::new(ConnectionCatalogueBehavior),
    );
    let run = TransportRun {
        incarnation: OwnerIncarnation(Uuid::now_v7()),
        generation: SessionGeneration::from_number(1),
    };
    let queue = |frame| {
        wire.transport
            .enqueue_control(run, frame)
            .expect("the real carrier accepts each frame")
    };
    let mut work = Vec::new();
    for index in 0..3 {
        let authorization = DispatchAuthorization {
            incarnation: run.incarnation,
            run: run.generation,
            input_id: InputId::generate(),
            authorization_seq: index,
            target: AuthorizationTarget::Session,
        };
        let (reply, receipt) = oneshot::channel();
        let completion = Box::new(ReplyRecorder(reply));
        let (message, correlation) = match index {
            0 => (
                UntypedMessage::new("session/prompt", serde_json::json!({}))
                    .expect("prompt request"),
                ResponseKind::Prompt {
                    authorization: authorization.clone(),
                    turn: TurnId(Uuid::now_v7()),
                    compact: false,
                    reader: None,
                    completion,
                },
            ),
            1 => (
                UntypedMessage::new("session/steer", serde_json::json!({})).expect("steer request"),
                ResponseKind::Steer {
                    authorization: authorization.clone(),
                    idle: SteerIdlePolicy::Refuse,
                    completion,
                },
            ),
            _ => {
                let (message, setting) = setting_plan(ProviderSettingKind::Mode).correlate(
                    SettingCorrelation::Explicit {
                        authorization: authorization.clone(),
                        completion,
                    },
                );
                (message, ResponseKind::Setting(Box::new(setting)))
            }
        };
        queue(AcpFrame::Request {
            message,
            correlation,
        });
        work.push((authorization, receipt));
    }
    let close = RunAttempt {
        generation: run.generation,
        attempt: CloseAttempt(Uuid::now_v7()),
    };
    let (reply, closing) = oneshot::channel();
    queue(AcpFrame::Request {
        message: UntypedMessage::new("session/close", serde_json::json!({}))
            .expect("close request"),
        correlation: ResponseKind::Close {
            run: close,
            completion: Box::new(ReplyRecorder(reply)),
        },
    });
    let mut settings = Vec::new();
    for kind in [ProviderSettingKind::Mode, ProviderSettingKind::Fast] {
        let restore = RunAttempt {
            generation: run.generation,
            attempt: LoadAttempt(Uuid::now_v7()),
        };
        let setting = OwnerSetting {
            change: RunAttempt {
                generation: run.generation,
                attempt: SettingApplication(Uuid::now_v7()),
            },
            kind,
        };
        let (reply, receipt) = oneshot::channel();
        queue(restore_setting_frame(
            restore,
            setting,
            setting_plan(kind),
            Box::new(ReplyRecorder(reply)),
        ));
        settings.push((restore, setting, receipt));
    }
    let setup = || OwnerSessionOpenSetup {
        cwd: "/".into(),
        work_roots: Default::default(),
        scratch_folder: None,
    };
    let restore = |via| OpenRequest::Restore {
        provider_id: "queued-session".into(),
        attempt: LoadAttempt(Uuid::now_v7()),
        tap: OpeningInstance::default(),
        updates: mpsc::unbounded_channel().1,
        via,
        setup: setup(),
    };
    let mut openings = Vec::new();
    for request in [
        OpenRequest::New(setup()),
        restore(RestoreVia::Load),
        restore(RestoreVia::Resume),
    ] {
        let (frame, receipt) = wire.prepare_open(request, OpenReply::Lifecycle);
        queue(frame);
        openings.push(receipt);
    }
    queue(AcpFrame::Cancel(CancelNotification::new("queued-session")));
    let (_unoffered, mut unoffered) = oneshot::channel::<WorkReply>();

    let retired = CancellationToken::new();
    retired.cancel();
    output
        .run(
            inert_connection().await,
            configuration(),
            retired,
            CancellationToken::new(),
        )
        .await;

    let mut settlements = 0;
    for (authorization, mut receipt) in work {
        assert_eq!(
            receipt.try_recv(),
            Ok(WorkReply::Dispatch(DispatchResult::DefinitelyUnsent(
                authorization
            ))),
            "queued work must settle its own authorization as definitely unsent"
        );
        settlements += 1;
    }
    let mut closing = closing;
    assert_eq!(closing.try_recv(), Ok(CloseFact::NotOffered { run: close }));
    settlements += 1;
    for (expected_restore, expected_setting, mut receipt) in settings {
        assert!(
            matches!(receipt.try_recv(), Ok(SettingFact::NotSent { restore, setting })
            if restore == expected_restore && setting == expected_setting)
        );
        settlements += 1;
    }
    for mut receipt in openings {
        assert!(matches!(
            receipt.try_recv(),
            Ok(Err(OpenReceiptFailure::OpenNotSent))
        ));
        settlements += 1;
    }
    assert_eq!(
        settlements, 9,
        "every queued reply settles once and cancel has no reply"
    );
    assert_eq!(
        unoffered.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    );
}

#[tokio::test]
async fn writer_waits_for_a_taken_permit_to_enqueue_after_close() {
    let (sender, output) = mpsc::channel(1);
    let permit = sender
        .clone()
        .reserve_owned()
        .await
        .expect("writer granted its exact queue permit");
    let claim = crate::owner_opening_instance::OpeningOffer::default();
    assert!(claim.take());
    let run = TransportRun {
        incarnation: OwnerIncarnation(Uuid::now_v7()),
        generation: SessionGeneration::from_number(1),
    };
    let restore = RunAttempt {
        generation: run.generation,
        attempt: LoadAttempt(Uuid::now_v7()),
    };
    let setting = OwnerSetting {
        change: RunAttempt {
            generation: run.generation,
            attempt: SettingApplication(Uuid::now_v7()),
        },
        kind: ProviderSettingKind::Mode,
    };
    let (reply, mut receipt) = oneshot::channel();
    let frame = restore_setting_frame(
        restore,
        setting,
        setting_plan(ProviderSettingKind::Mode),
        Box::new(ReplyRecorder(reply)),
    );
    let stopped = CancellationToken::new();
    stopped.cancel();
    let output = OwnerAcpOutput {
        output,
        restore_updates: Arc::default(),
        #[cfg(feature = "test-observation")]
        drain_pause: None,
    };
    let mut writer = std::pin::pin!(output.run(
        inert_connection().await,
        configuration(),
        stopped,
        CancellationToken::new(),
    ));
    // This poll is the barrier. A pending result is possible only after
    // the post-close receive sees an empty queue with a live permit.
    assert!(
        writer.as_mut().now_or_never().is_none(),
        "post-close drain must wait for the already-granted writer permit"
    );
    assert!(
        sender.is_closed(),
        "the barrier observed the receiver after close"
    );
    assert!(matches!(
        receipt.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    let _sender = permit.send(crate::acp_transport::OrderedOutput { run, frame });
    writer.await;
    assert!(
        matches!(receipt.try_recv(), Ok(SettingFact::NotSent { restore: exact, setting: application })
        if exact == restore && application == setting)
    );
}
