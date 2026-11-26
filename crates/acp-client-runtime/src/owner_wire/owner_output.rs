//! Drain the ordered ACP writer and settle each original response.

#[cfg(feature = "test-observation")]
use super::FixtureDispatchPoint;
use super::writer_reply_signals::{WriterReplySignals, error_reply_lost};
use super::{AcpFrame, ResponseKind};
use crate::{
    acp_transport::OrderedOutput, owner_session_open::OwnerOpenConfiguration,
    provider_prompt_result_codec::decode_prompt_result,
};
#[cfg(feature = "test-observation")]
use agent_client_protocol::UntypedMessage;
use agent_client_protocol::{Agent, ConnectionTo, Error, schema::v1::ErrorCode};
use provider_session_machines::{
    harness_lifecycle::CloseFact,
    session_owner::{DispatchAuthorization, DispatchResult, PromptCompletion, WireOutcome},
    session_turn_machine::TurnId,
};
use std::sync::Arc;
use switchboard_harness_contracts::{
    CloseUncertainty, ReservedWorkReply, SteerIdlePolicy, WorkReply,
};
use tokio::sync::mpsc;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use uuid::Uuid;

pub(crate) struct PromptTerminal {
    pub(crate) turn: TurnId,
    pub(crate) compact: bool,
    pub(super) authorization: DispatchAuthorization,
    pub(super) reply: Option<Result<serde_json::Value, Error>>,
    pub(super) completion: ReservedWorkReply,
}

impl PromptTerminal {
    pub(super) fn route(self, reader: Option<mpsc::Sender<Self>>, connection_lost: bool) {
        if connection_lost {
            self.completion.send(WorkReply::ConnectionLost);
        } else if let Some(reader) = reader {
            if let Err(undelivered) = reader.try_send(self) {
                undelivered.into_inner().finish(String::new(), None);
            }
        } else {
            self.finish(String::new(), None);
        }
    }

    pub(crate) fn stop_reason(&self) -> Option<session_event_model::StopReason> {
        decode_prompt_result(self.reply.as_ref()?.as_ref().ok()?).ok()
    }

    pub(crate) fn finish(
        self,
        response: String,
        local_cause: Option<session_event_model::LocalCause>,
    ) {
        if let Some(Err(error)) = &self.reply
            && let Some(failure) =
                crate::provider_acp_error_mapping::acp_operation_error(error.clone())
                    .coded_wire_failure()
        {
            self.completion
                .send(WorkReply::Dispatch(DispatchResult::PromptFailed {
                    authorization: self.authorization,
                    failure,
                }));
            return;
        }
        let outcome = match self.reply {
            Some(Ok(reply)) => {
                decode_prompt_result(&reply).map_or(WireOutcome::Unknown, |stop_reason| {
                    WireOutcome::Confirmed(PromptCompletion {
                        stop_reason,
                        local_cause,
                        response,
                    })
                })
            }
            _ => WireOutcome::Unknown,
        };
        self.completion
            .send(WorkReply::Dispatch(DispatchResult::PromptFinished {
                authorization: self.authorization,
                outcome,
            }));
    }
}

pub(crate) struct OwnerAcpOutput {
    pub(super) output: mpsc::Receiver<OrderedOutput<AcpFrame>>,
    pub(super) restore_updates: Arc<crate::owner_restore_updates::RestoreUpdates>,
    #[cfg(feature = "test-observation")]
    pub(super) drain_pause: Option<super::owner_opening_test_control::AsyncBarrier>,
}

impl OwnerAcpOutput {
    #[cfg(test)]
    pub(crate) fn buffer_next_close_reply_for_test(
        &mut self,
        behavior: &dyn crate::ProviderBehavior,
    ) {
        self.buffer_close_outcome_for_test(Ok(serde_json::json!({})), behavior);
    }

    #[cfg(test)]
    pub(crate) fn buffer_close_outcome_for_test(
        &mut self,
        reply: Result<serde_json::Value, Error>,
        behavior: &dyn crate::ProviderBehavior,
    ) {
        let next = self
            .output
            .try_recv()
            .expect("the real writer accepted a close frame");
        let AcpFrame::Request {
            correlation: correlation @ ResponseKind::Close { .. },
            ..
        } = next.frame
        else {
            panic!("expected an exact close request");
        };
        correlation.complete(Some(reply), false, behavior);
    }

    #[cfg(test)]
    pub(crate) fn hold_next_frame_for_test(&mut self) -> impl Send + use<> {
        self.output
            .try_recv()
            .expect("the exact offered frame remains in custody")
    }

    pub(crate) async fn run(
        mut self,
        connection: ConnectionTo<Agent>,
        configuration: OwnerOpenConfiguration,
        shutdown: CancellationToken,
        transport_ending: CancellationToken,
    ) {
        let signals = WriterReplySignals::new(shutdown.child_token(), transport_ending);
        let replies = TaskTracker::new();
        loop {
            let next = tokio::select! {
                biased;
                () = signals.shutdown.cancelled() => break,
                next = self.output.recv() => next,
            };
            let Some(OrderedOutput { frame, run }) = next else {
                break;
            };
            match frame {
                AcpFrame::Open(open) => open.start(
                    run,
                    &connection,
                    &configuration,
                    &replies,
                    signals.clone(),
                    self.restore_updates.clone(),
                ),
                AcpFrame::Cancel(notification) => {
                    if connection.send_notification(notification).is_err() {
                        tracing::debug!("ACP cancel could not leave the ordered writer");
                    }
                }
                #[cfg(feature = "test-observation")]
                AcpFrame::FixtureStopWriter => {
                    self.output.close();
                    break;
                }
                #[cfg(feature = "test-observation")]
                AcpFrame::FixturePauseWriter {
                    before_dequeue,
                    during_drain,
                } => {
                    self.drain_pause = Some(during_drain);
                    before_dequeue.pause().await;
                }
                #[cfg(feature = "test-observation")]
                AcpFrame::FixtureDispatchFence(point, complete) => {
                    let after = match point {
                        FixtureDispatchPoint::Cancel => "session/cancel",
                        FixtureDispatchPoint::Restore => "session/load",
                    };
                    let result = UntypedMessage::new(
                        "fixture/orderedDispatchFence",
                        serde_json::json!({"after":after}),
                    )
                    .and_then(|fence| connection.send_notification(fence));
                    let _fixture_gone = complete.send(result);
                }
                AcpFrame::Request {
                    message,
                    correlation,
                } => {
                    let behavior = configuration.behavior.clone();
                    #[cfg(all(debug_assertions, unix, feature = "harness-work-observation"))]
                    let witness = match &correlation {
                        ResponseKind::Prompt { authorization, .. }
                        | ResponseKind::Steer { authorization, .. } => {
                            Some((authorization, message.method.clone()))
                        }
                        ResponseKind::Close { .. } | ResponseKind::Setting(_) => None,
                    };
                    let request = connection.send_request(message);
                    #[cfg(all(debug_assertions, unix, feature = "harness-work-observation"))]
                    if let Some((authorization, method)) = witness {
                        crate::work_call_observation::record_frame_id(
                            authorization,
                            &method,
                            request.id(),
                        );
                    }
                    let request = request.block_task();
                    let signals = signals.clone();
                    let connection = connection.clone();
                    #[cfg(feature = "test-observation")]
                    let reply_test_wire = configuration.reply_test_wire.clone();
                    replies.spawn(async move {
                        #[cfg(feature = "test-observation")]
                        if let Some(wire) = reply_test_wire {
                            wire.before_taken_reply_for_test().await;
                        }
                        let (response, connection_lost) =
                            ResponseKind::read_reply(request, &signals.shutdown, |error| {
                                signals.transport_ended(&connection, error)
                            })
                            .await;
                        correlation.complete(response, connection_lost, behavior.as_ref());
                    });
                }
            }
        }
        self.drain_unsent(&signals.shutdown).await;
        replies.close();
        replies.wait().await;
    }

    pub(crate) async fn drain_unsent(&mut self, shutdown: &CancellationToken) {
        self.output.close();
        shutdown.cancel();
        #[cfg(feature = "test-observation")]
        if let Some(pause) = self.drain_pause.take() {
            pause.pause().await;
        }
        while let Some(OrderedOutput { run, frame }) = self.output.recv().await {
            match frame {
                AcpFrame::Open(open) => {
                    if let super::OpenRequest::Restore { tap, .. } = &open.request {
                        self.restore_updates.reply_settled(run, tap);
                    }
                    open.respond.unsent();
                }
                AcpFrame::Request { correlation, .. } => correlation.complete_unsent(),
                AcpFrame::Cancel(_) => {}
                #[cfg(feature = "test-observation")]
                AcpFrame::FixtureDispatchFence(_, receipt) => {
                    let _fixture_gone = receipt.send(Err(Error::internal_error()));
                }
                #[cfg(feature = "test-observation")]
                AcpFrame::FixtureStopWriter | AcpFrame::FixturePauseWriter { .. } => {}
            }
        }
    }
}

impl ResponseKind {
    fn complete_unsent(self) {
        match self {
            Self::Prompt {
                authorization,
                completion,
                ..
            }
            | Self::Steer {
                authorization,
                completion,
                ..
            } => {
                completion.send(WorkReply::Dispatch(DispatchResult::DefinitelyUnsent(
                    authorization,
                )));
            }
            Self::Close { run, completion } => completion.send(CloseFact::NotOffered { run }),
            Self::Setting(setting) => setting.settle_unsent(),
        }
    }

    pub(super) async fn read_reply(
        request: impl std::future::Future<Output = Result<serde_json::Value, Error>>,
        shutdown: &CancellationToken,
        incoming_lost: impl FnOnce(&Error) -> bool,
    ) -> (Option<Result<serde_json::Value, Error>>, bool) {
        let response = tokio::select! {
            biased;
            reply = request => Some(reply),
            () = shutdown.cancelled() => None,
        };
        let connection_lost = response.as_ref().is_none_or(|reply| {
            reply
                .as_ref()
                .is_err_and(|error| error_reply_lost(shutdown, error, incoming_lost))
        });
        (response, connection_lost)
    }

    fn complete(
        self,
        reply: Option<Result<serde_json::Value, Error>>,
        connection_lost: bool,
        behavior: &dyn crate::ProviderBehavior,
    ) {
        // An error from a connection already known to be ending is
        // missing peer evidence. Completed results stay intact.
        let reply = if connection_lost { None } else { reply };
        match self {
            Self::Prompt {
                authorization,
                turn,
                compact,
                reader,
                completion,
            } => PromptTerminal {
                turn,
                compact,
                authorization,
                reply,
                completion,
            }
            .route(reader, connection_lost),
            Self::Setting(setting) => {
                setting.settle(reply, behavior);
            }
            Self::Steer {
                authorization,
                idle,
                completion,
            } => {
                completion.send(WorkReply::Dispatch(settle_steer_reply(
                    authorization,
                    idle,
                    reply,
                )));
            }
            Self::Close { run, completion } => {
                let fact = match reply {
                    Some(Ok(_)) => CloseFact::Closed { run },
                    Some(Err(error)) if definite_refusal(&error) => {
                        CloseFact::RefusedStillLive { run }
                    }
                    Some(Err(_)) => CloseFact::Unknown {
                        run,
                        cause: CloseUncertainty::EffectUnconfirmed,
                    },
                    None => CloseFact::Unknown {
                        run,
                        cause: CloseUncertainty::ReplyLost,
                    },
                };
                completion.send(fact);
            }
        }
    }
}

fn settle_steer_reply(
    authorization: DispatchAuthorization,
    idle: SteerIdlePolicy,
    reply: Option<Result<serde_json::Value, Error>>,
) -> DispatchResult {
    match reply {
        Some(Ok(reply)) => match reply.get("outcome").and_then(serde_json::Value::as_str) {
            Some("startedNewTurn") => DispatchResult::SteerStartedNewTurn {
                authorization,
                turn: TurnId(Uuid::now_v7()),
            },
            Some("promptRequired") => match idle {
                SteerIdlePolicy::AllowOwnerPrompt => DispatchResult::PromptRequired {
                    authorization,
                    replacement_turn: TurnId(Uuid::now_v7()),
                },
                SteerIdlePolicy::Refuse => DispatchResult::SteerNeedsPrompt(authorization),
            },
            Some("injected") => DispatchResult::SteerFinished {
                authorization,
                outcome: WireOutcome::Confirmed(()),
            },
            Some("failed") => DispatchResult::SteerFinished {
                authorization,
                outcome: WireOutcome::Refused,
            },
            _ => DispatchResult::SteerInvalidReply(authorization),
        },
        Some(Err(error)) if definite_refusal(&error) => DispatchResult::SteerFinished {
            authorization,
            outcome: WireOutcome::Refused,
        },
        _ => DispatchResult::SteerFinished {
            authorization,
            outcome: WireOutcome::Unknown,
        },
    }
}

fn definite_refusal(error: &Error) -> bool {
    !agent_client_protocol::is_incoming_transport_closed(error)
        && matches!(
            error.code,
            ErrorCode::MethodNotFound
                | ErrorCode::InvalidParams
                | ErrorCode::AuthRequired
                | ErrorCode::ResourceNotFound
        )
}
