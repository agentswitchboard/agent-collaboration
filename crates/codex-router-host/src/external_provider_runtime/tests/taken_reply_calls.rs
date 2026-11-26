//! Send each writer-owned request through the real ACP binding.

use super::*;
use provider_session_machines::{
    ProviderSettingKind,
    harness_lifecycle::{
        CloseUncertainty, CreationEffect, HarnessContent, LifecycleReply, OpeningFailure,
        SettingAttempt, SettingFact, WorkReply,
    },
    session_owner::{
        AuthorizationTarget, DispatchAuthorization, DispatchResult, ObservationCause,
        ObservedSetting, OwnerSetting, ProviderWireFailureKind, ReplyAdmission, RestoreModeOutcome,
        SettingObservation, SettingOffer, SteerIdlePolicy, TurnMembership, WireOutcome,
    },
    session_setting_machine::{SettingApplication, SettingValueId},
    session_turn_machine::TurnId,
};
use session_event_model::{InputId, PromptContent, StopReason};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum TakenKind {
    Prompt,
    Steer,
    WorkSetting,
    Close,
    Create,
    Load,
    Resume,
    RestoreMode,
    RestoreFastOff,
}

impl TakenKind {
    pub(super) const ALL: [Self; 9] = [
        Self::Prompt,
        Self::Steer,
        Self::WorkSetting,
        Self::Close,
        Self::Create,
        Self::Load,
        Self::Resume,
        Self::RestoreMode,
        Self::RestoreFastOff,
    ];

    pub(super) fn method(self) -> &'static str {
        match self {
            Self::Prompt => "session/prompt",
            Self::Steer => "_session/steering",
            Self::WorkSetting | Self::RestoreMode | Self::RestoreFastOff => {
                "session/set_config_option"
            }
            Self::Close => "session/close",
            Self::Create => "session/new",
            Self::Load => "session/load",
            Self::Resume => "session/resume",
        }
    }

    pub(super) fn needs_saved_session(self) -> bool {
        match self {
            Self::Prompt
            | Self::Steer
            | Self::WorkSetting
            | Self::Close
            | Self::RestoreMode
            | Self::RestoreFastOff => true,
            Self::Create | Self::Load | Self::Resume => false,
        }
    }

    pub(super) fn needs_live_reader(self) -> bool {
        match self {
            Self::Prompt | Self::Steer | Self::WorkSetting => true,
            Self::Create
            | Self::Load
            | Self::Resume
            | Self::Close
            | Self::RestoreMode
            | Self::RestoreFastOff => false,
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum ExpectedReply {
    Lost,
    PeerRefusal,
    BufferedSuccess,
}

pub(super) enum TakenReply {
    Work(DispatchAuthorization, oneshot::Receiver<WorkReply>),
    Close(RunAttempt<CloseAttempt>, oneshot::Receiver<CloseFact>),
    Open(OpenAttempt, oneshot::Receiver<OpeningFact>),
    Setting(
        RunAttempt<LoadAttempt>,
        OwnerSetting,
        oneshot::Receiver<SettingFact>,
    ),
}

impl TakenReply {
    pub(super) async fn assert(self, kind: TakenKind, expected: ExpectedReply) {
        match self {
            Self::Work(auth, receipt) => {
                let reply = receipt.await.expect("the original taken work must settle");
                assert_work_reply(kind, expected, auth, reply);
            }
            Self::Close(run, receipt) => {
                let expected = match expected {
                    ExpectedReply::Lost => CloseFact::Unknown {
                        run,
                        cause: CloseUncertainty::ReplyLost,
                    },
                    ExpectedReply::PeerRefusal => CloseFact::RefusedStillLive { run },
                    ExpectedReply::BufferedSuccess => CloseFact::Closed { run },
                };
                assert_eq!(
                    receipt.await.expect("the exact taken close must settle"),
                    expected
                );
            }
            Self::Open(exact, receipt) => {
                let reply = receipt
                    .await
                    .expect("the original taken opening must settle");
                assert_open_reply(kind, exact, expected, reply);
            }
            Self::Setting(restore, setting, receipt) => {
                let reply = receipt
                    .await
                    .expect("the original taken restore setter must settle");
                assert_setting_reply(expected, restore, setting, reply);
            }
        }
    }
}

fn assert_work_reply(
    kind: TakenKind,
    expected: ExpectedReply,
    auth: DispatchAuthorization,
    reply: WorkReply,
) {
    let expected = match (kind, expected) {
        (TakenKind::Prompt, ExpectedReply::Lost) => {
            assert_eq!(reply, WorkReply::ConnectionLost);
            return;
        }
        (TakenKind::Prompt, ExpectedReply::PeerRefusal) => {
            assert!(
                matches!(reply, WorkReply::Dispatch(DispatchResult::PromptFailed { authorization, failure })
                if authorization == auth && failure.kind == ProviderWireFailureKind::UnsupportedMethod)
            );
            return;
        }
        (TakenKind::Prompt, ExpectedReply::BufferedSuccess) => {
            assert!(
                matches!(reply, WorkReply::Dispatch(DispatchResult::PromptFinished {
                authorization, outcome: WireOutcome::Confirmed(completion)
            }) if authorization == auth && completion.stop_reason == StopReason::EndTurn
                && completion.response.is_empty() && completion.local_cause.is_none())
            );
            return;
        }
        (TakenKind::Steer, ExpectedReply::Lost) => DispatchResult::SteerFinished {
            authorization: auth,
            outcome: WireOutcome::Unknown,
        },
        (TakenKind::Steer, ExpectedReply::PeerRefusal) => DispatchResult::SteerFinished {
            authorization: auth,
            outcome: WireOutcome::Refused,
        },
        (TakenKind::Steer, ExpectedReply::BufferedSuccess) => DispatchResult::SteerFinished {
            authorization: auth,
            outcome: WireOutcome::Confirmed(()),
        },
        (TakenKind::WorkSetting, ExpectedReply::Lost) => DispatchResult::SettingFinished {
            authorization: auth,
            observation: SettingObservation::Unknown,
        },
        (TakenKind::WorkSetting, ExpectedReply::PeerRefusal) => DispatchResult::SettingFinished {
            authorization: auth,
            observation: SettingObservation::Refused {
                current: None,
                catalogue: None,
            },
        },
        (TakenKind::WorkSetting, ExpectedReply::BufferedSuccess) => {
            assert!(
                matches!(reply, WorkReply::Dispatch(DispatchResult::SettingFinished {
                authorization, observation: SettingObservation::AppliedWithReadback(readback)
            }) if authorization == auth && readback.value.0 == "plan")
            );
            return;
        }
        _ => panic!("unexpected work receipt class {kind:?}"),
    };
    assert_eq!(
        reply,
        WorkReply::Dispatch(expected),
        "taken work reply for {kind:?}"
    );
}

fn assert_open_reply(
    kind: TakenKind,
    expected_attempt: OpenAttempt,
    expected: ExpectedReply,
    reply: OpeningFact,
) {
    match (expected_attempt, reply) {
        (OpenAttempt::Create(exact), OpeningFact::Created { attempt, session })
            if matches!(expected, ExpectedReply::BufferedSuccess) =>
        {
            assert_eq!(attempt, exact);
            assert_eq!(session.0, "reactive-session-1");
        }
        (
            OpenAttempt::Create(exact),
            OpeningFact::CreateFailed {
                attempt,
                remote,
                failure,
            },
        ) => {
            assert_eq!(attempt, exact);
            match expected {
                ExpectedReply::Lost => {
                    assert_eq!(remote, CreationEffect::Unknown);
                    assert_eq!(failure, OpeningFailure::OutcomeUnknown);
                }
                ExpectedReply::PeerRefusal => {
                    assert_eq!(remote, CreationEffect::None);
                    assert!(matches!(failure, OpeningFailure::Refused(failure)
                        if failure.kind == ProviderWireFailureKind::UnsupportedMethod));
                }
                ExpectedReply::BufferedSuccess => {
                    panic!("buffered create success must name the exact created session")
                }
            }
        }
        (OpenAttempt::Restore(exact), OpeningFact::RestoreFailed { run, failure }) => {
            assert_eq!(run, exact);
            match expected {
                ExpectedReply::Lost => assert_eq!(failure, OpeningFailure::Disconnected),
                ExpectedReply::PeerRefusal => {
                    assert!(matches!(failure, OpeningFailure::Refused(failure)
                        if failure.kind == ProviderWireFailureKind::UnsupportedMethod));
                }
                ExpectedReply::BufferedSuccess => {
                    let OpeningFailure::Unavailable { diagnostic } = failure else {
                        panic!(
                            "buffered {kind:?} success must retain the provider acknowledgement after local stop, got {failure:?}"
                        );
                    };
                    assert!(diagnostic.contains(kind.method()), "{diagnostic}");
                }
            }
        }
        _ => panic!("the exact taken opening must deliver its typed failure"),
    }
}

fn assert_setting_reply(
    expected: ExpectedReply,
    exact_restore: RunAttempt<LoadAttempt>,
    exact_setting: OwnerSetting,
    reply: SettingFact,
) {
    match reply {
        SettingFact::Mode {
            restore,
            setting,
            outcome,
            catalogue,
        } => {
            assert_eq!((restore, setting), (exact_restore, exact_setting));
            assert_eq!(
                catalogue.is_some(),
                matches!(expected, ExpectedReply::BufferedSuccess)
            );
            match expected {
                ExpectedReply::Lost => assert_eq!(outcome, RestoreModeOutcome::Unconfirmed),
                ExpectedReply::PeerRefusal => {
                    assert!(matches!(outcome, RestoreModeOutcome::Refused { .. }));
                }
                ExpectedReply::BufferedSuccess => assert_eq!(
                    outcome,
                    RestoreModeOutcome::Confirmed(SettingValueId("plan".into()))
                ),
            }
        }
        SettingFact::FastOff {
            restore,
            setting,
            observation,
            catalogue,
        } => {
            assert_eq!((restore, setting), (exact_restore, exact_setting));
            match expected {
                ExpectedReply::BufferedSuccess => {
                    assert!(catalogue.is_some());
                    assert_eq!(
                        observation.value,
                        ObservedSetting::Confirmed(SettingValueId("off".into()))
                    );
                    assert_eq!(observation.cause, None);
                }
                ExpectedReply::Lost | ExpectedReply::PeerRefusal => {
                    assert!(catalogue.is_none());
                    assert_eq!(observation.value, ObservedSetting::Unknown);
                    assert_eq!(
                        observation.cause,
                        Some(if matches!(expected, ExpectedReply::Lost) {
                            ObservationCause::Unconfirmed
                        } else {
                            ObservationCause::Refused
                        })
                    );
                }
            }
        }
        SettingFact::NotSent { .. } => panic!("the peer already received the exact setter"),
    }
}

pub(super) fn take_call(
    fixture: &HubRuntime,
    binding: &mut SessionHarnessBinding,
    owner: OwnerAddress,
    run: RunAttempt<LoadAttempt>,
    kind: TakenKind,
    catalogue: Option<Arc<provider_session_machines::ProviderSettingsCatalogRecord>>,
    turn: TurnId,
) -> TakenReply {
    match kind {
        TakenKind::Create | TakenKind::Load | TakenKind::Resume => {
            take_open(fixture, binding, owner, run, kind)
        }
        TakenKind::Close => {
            let close = RunAttempt {
                generation: run.generation,
                attempt: CloseAttempt(Uuid::now_v7()),
            };
            let (reply, receipt) = oneshot::channel();
            binding
                .close(close, Box::new(Reply(reply)))
                .expect("take the real close");
            TakenReply::Close(close, receipt)
        }
        TakenKind::RestoreMode | TakenKind::RestoreFastOff => {
            let setting_kind = if kind == TakenKind::RestoreMode {
                ProviderSettingKind::Mode
            } else {
                ProviderSettingKind::Fast
            };
            let setting = OwnerSetting {
                change: RunAttempt {
                    generation: run.generation,
                    attempt: SettingApplication(Uuid::now_v7()),
                },
                kind: setting_kind,
            };
            let (reply, receipt) = oneshot::channel();
            let mut reply = Some(reply);
            binding
                .put_setting_back(
                    SettingAttempt {
                        restore: run,
                        setting,
                        value: SettingValueId(
                            if setting_kind == ProviderSettingKind::Mode {
                                "plan"
                            } else {
                                "off"
                            }
                            .into(),
                        ),
                        catalogue: catalogue.expect("real held restore setting catalogue"),
                    },
                    &mut || {
                        ReplyAdmission::Reserved(Box::new(Reply(
                            reply.take().expect("one exact reservation"),
                        )))
                    },
                )
                .expect("take a real restore setting");
            TakenReply::Setting(run, setting, receipt)
        }
        TakenKind::Prompt | TakenKind::Steer | TakenKind::WorkSetting => {
            take_work(binding, owner, run.generation, kind, turn)
        }
    }
}

fn take_open(
    fixture: &HubRuntime,
    binding: &mut SessionHarnessBinding,
    owner: OwnerAddress,
    run: RunAttempt<LoadAttempt>,
    kind: TakenKind,
) -> TakenReply {
    let (reply, receipt) = oneshot::channel();
    let (attempt, request) = match kind {
        TakenKind::Create => {
            let attempt = CreateAttempt(Uuid::now_v7());
            (
                OpenAttempt::Create(attempt),
                OpenRequest::Create {
                    owner,
                    attempt,
                    requested: Default::default(),
                    setup: OpenSetup {
                        cwd: fixture._root.path().to_owned(),
                        work_roots: Default::default(),
                        scratch_folder: None,
                    },
                },
            )
        }
        TakenKind::Load | TakenKind::Resume => {
            let OpenRequest::Restore { session, setup, .. } = restore_request(fixture, owner, run)
            else {
                panic!("expected actual restore setup");
            };
            (
                OpenAttempt::Restore(run),
                OpenRequest::Restore {
                    owner,
                    run,
                    session,
                    setup,
                    via: if kind == TakenKind::Load {
                        RestoreVia::Load
                    } else {
                        RestoreVia::Resume
                    },
                    requested_mode: None,
                },
            )
        }
        _ => panic!("request {kind:?} does not open a session"),
    };
    binding
        .open(
            request,
            Box::new(Reply(reply)),
            Arc::new(AcceptEvents),
            Box::pin(std::future::pending()),
        )
        .expect("take the real ordered opening");
    TakenReply::Open(attempt, receipt)
}

pub(super) fn take_work(
    binding: &SessionHarnessBinding,
    owner: OwnerAddress,
    generation: SessionGeneration,
    kind: TakenKind,
    turn: TurnId,
) -> TakenReply {
    let (reply, receipt) = oneshot::channel();
    let authorization = take_work_with_reply(
        binding,
        owner,
        generation,
        kind,
        turn,
        Box::new(Reply(reply)),
    );
    TakenReply::Work(authorization, receipt)
}

pub(super) fn take_work_with_reply(
    binding: &SessionHarnessBinding,
    owner: OwnerAddress,
    generation: SessionGeneration,
    kind: TakenKind,
    turn: TurnId,
    reply: LifecycleReply<WorkReply>,
) -> DispatchAuthorization {
    let authorization = DispatchAuthorization {
        incarnation: owner.incarnation,
        run: generation,
        input_id: InputId::generate(),
        authorization_seq: if kind == TakenKind::Steer { 2 } else { 1 },
        target: match kind {
            TakenKind::Prompt => AuthorizationTarget::NewTurn,
            TakenKind::Steer => AuthorizationTarget::ExistingTurn { turn },
            TakenKind::WorkSetting => AuthorizationTarget::Session,
            _ => panic!("request {kind:?} does not offer work"),
        },
    };
    let content = [PromptContent::text("receipt proof".into()).expect("valid prompt")];
    match kind {
        TakenKind::Prompt => {
            binding.set_scope(TurnMembership::Provisional {
                run: generation,
                turn,
            });
            binding
                .offer_prompt(
                    authorization.clone(),
                    turn,
                    HarnessContent::Input(&content),
                    reply,
                )
                .expect("offer actual prompt");
        }
        TakenKind::Steer => binding
            .offer_steer(
                authorization.clone(),
                HarnessContent::Input(&content),
                SteerIdlePolicy::Refuse,
                reply,
            )
            .expect("offer actual steer to the live turn"),
        TakenKind::WorkSetting => assert_eq!(
            binding
                .offer_setting(
                    authorization.clone(),
                    ProviderSettingKind::Mode,
                    SettingValueId("plan".into()),
                    reply
                )
                .expect("offer actual work setting"),
            SettingOffer::Offered,
        ),
        _ => panic!("request {kind:?} does not offer work"),
    }
    assert_eq!(
        binding.hand_over(&authorization, &mut || true),
        Ok(DispatchResult::Taken(authorization.clone()))
    );
    authorization
}
