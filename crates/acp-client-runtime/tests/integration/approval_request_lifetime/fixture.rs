//! A controlled peer on the production ACP connection and an injected policy.
#![expect(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "An invalid fixture message or closed channel fails the scenario at the wire step."
)]

use acp_client_runtime::{
    ApprovalPortOutcome, EventSinkClosed, ExternalProviderPromptOutcome,
    ExternalProviderRuntimeError, HistoryReplayFuture, ProviderTurnInput, SessionEventSink,
    acp_owner_fixture::PolicyFixtureClient,
};
use agent_client_protocol::schema::v1::RequestId;
use serde_json::{Value, json};
use session_event_model::{
    ApprovalRequest, InputId, PromptContent, ProviderSubmission, QuestionRequest, QuestionResponse,
    SessionEvent, SkillView, managed_interaction_refusal::ManagedInteractionRefusal,
};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex, MutexGuard, OnceLock, Weak,
        atomic::{AtomicUsize, Ordering},
    },
};
use switchboard_harness_contracts::{
    ApprovalContext, Decision, ExactScope, ExactTurn, RequestCancellation,
    RequestCancellationSignal, RequestDecisionRefusal, RequestFuture, RequestPolicy,
    StopRequestsCause,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream, ReadHalf, WriteHalf},
    sync::{Notify, oneshot},
};

type Payloads = (
    u64,
    u64,
    ApprovalRequest,
    QuestionRequest,
    QuestionResponse,
    ManagedInteractionRefusal,
);
pub(super) type Client = PolicyFixtureClient<Payloads>;
type PromptResult = Result<ExternalProviderPromptOutcome, ExternalProviderRuntimeError>;
pub(super) const SESSION: &str = "fixture-session";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum PolicyEvent {
    Started {
        request: u64,
        operation: u64,
        scope: ExactScope,
    },
    ScopeCancelled(u64),
    RequestCancelled(u64),
    Finished(u64),
    Refused(u64, RequestDecisionRefusal),
    Stopped {
        operation: u64,
        turn: ExactTurn,
        cause: StopRequestsCause,
    },
    ManagedCancel {
        operation: u64,
        queued: Option<RequestId>,
    },
}

pub(super) enum DecisionPlan {
    Refuse(RequestDecisionRefusal),
    Hold(oneshot::Receiver<()>),
    WaitForScopeEnd,
}

#[derive(Default)]
pub(super) struct Policy {
    plans: Mutex<BTreeMap<u64, DecisionPlan>>,
    held_stops: Mutex<BTreeMap<u64, oneshot::Receiver<()>>>,
    events: Mutex<Vec<PolicyEvent>>,
    changed: Notify,
    // The managed batch's first query belongs to its ordinary sibling.
    managed_query: AtomicUsize,
    client: OnceLock<Weak<Client>>,
}

impl Policy {
    pub(super) fn managed_batch() -> Self {
        Self {
            managed_query: AtomicUsize::new(1),
            ..Self::default()
        }
    }

    pub(super) fn plan(&self, id: u64, plan: DecisionPlan) {
        assert!(locked(&self.plans).insert(id, plan).is_none());
    }

    pub(super) fn hold_answer(&self, id: u64) -> oneshot::Sender<()> {
        let (release, wait) = oneshot::channel();
        self.plan(id, DecisionPlan::Hold(wait));
        release
    }

    pub(super) fn hold_stop(&self, operation: u64) -> oneshot::Sender<()> {
        let (release, wait) = oneshot::channel();
        assert!(locked(&self.held_stops).insert(operation, wait).is_none());
        release
    }

    pub(super) fn events(&self) -> Vec<PolicyEvent> {
        locked(&self.events).clone()
    }

    pub(super) async fn observe(&self, find: impl Fn(&PolicyEvent) -> bool) -> PolicyEvent {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(event) = locked(&self.events)
                .iter()
                .find(|event| find(event))
                .cloned()
            {
                return event;
            }
            notified.await;
        }
    }

    fn record(&self, event: PolicyEvent) {
        locked(&self.events).push(event);
        self.changed.notify_waiters();
    }

    async fn decide<Reply>(
        &self,
        context: ApprovalContext<u64>,
        request_id: &str,
        cancellation: RequestCancellation,
        answer: Reply,
    ) -> Decision<Reply> {
        let (_, request): (String, u64) =
            serde_json::from_str(request_id).expect("canonical session and peer request ID");
        let plan = locked(&self.plans).remove(&request);
        self.record(PolicyEvent::Started {
            request,
            operation: context.context,
            scope: context.scope,
        });
        let result = match plan {
            Some(DecisionPlan::Refuse(refusal)) => {
                self.record(PolicyEvent::Refused(request, refusal));
                Decision::Refused(refusal)
            }
            Some(DecisionPlan::Hold(mut release)) => {
                tokio::select! {
                    () = cancellation.turn.cancelled() => {
                        self.record(PolicyEvent::ScopeCancelled(request));
                        release.await.expect("release the old decision");
                    }
                    () = cancellation.request.cancelled() => {
                        assert!(!cancellation.turn.is_cancelled(), "cancellation is local to the request");
                        self.record(PolicyEvent::RequestCancelled(request));
                    }
                    result = &mut release => result.expect("release the live decision"),
                }
                Decision::Answer(answer)
            }
            Some(DecisionPlan::WaitForScopeEnd) => {
                cancellation.turn.cancelled().await;
                self.record(PolicyEvent::ScopeCancelled(request));
                Decision::Answer(answer)
            }
            None => Decision::Answer(answer),
        };
        self.record(PolicyEvent::Finished(request));
        result
    }
}

impl RequestPolicy<Payloads> for Policy {
    fn decide_approval(
        &self,
        context: ApprovalContext<u64>,
        request: ApprovalRequest,
        cancellation: RequestCancellation,
    ) -> RequestFuture<'_, Decision<ApprovalPortOutcome>> {
        Box::pin(async move {
            self.decide(
                context,
                &request.request_id,
                cancellation,
                ApprovalPortOutcome::Selected {
                    option_id: "allow".into(),
                    note: None,
                },
            )
            .await
        })
    }

    fn decide_question(
        &self,
        context: ApprovalContext<u64>,
        request: QuestionRequest,
        cancellation: RequestCancellation,
    ) -> RequestFuture<'_, Decision<QuestionResponse>> {
        Box::pin(async move {
            self.decide(
                context,
                &request.request_id,
                cancellation,
                QuestionResponse::Declined,
            )
            .await
        })
    }

    fn operation_id(&self, context: &u64) -> u64 {
        *context
    }

    fn binding_retirement(&self, _context: &u64) -> Arc<dyn RequestCancellationSignal> {
        Arc::new(LiveBinding)
    }

    fn bind_owner_scope(&self, _context: &mut u64, _turn: ExactTurn) {}

    fn managed_refusal(&self) -> Option<ManagedInteractionRefusal> {
        self.managed_query
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |query| {
                (query != 0).then(|| query + 1)
            })
            .ok()
            .filter(|query| *query == 2)
            .map(|_| ManagedInteractionRefusal::ApprovalsUnavailableOnBuild)
    }

    fn cancel_after_managed_refusal(&self, context: ApprovalContext<u64>) -> RequestFuture<'_, ()> {
        Box::pin(async move {
            let client = self
                .client
                .get()
                .and_then(Weak::upgrade)
                .expect("active policy client");
            self.record(PolicyEvent::ManagedCancel {
                operation: context.context,
                queued: client.managed_permission_enqueued_for_test(),
            });
            client
                .cancel_active_prompt(SESSION.into())
                .await
                .expect("managed cancel on the real owner");
        })
    }

    fn stop_all(
        &self,
        operation: u64,
        turn: ExactTurn,
        cause: StopRequestsCause,
    ) -> RequestFuture<'_, ()> {
        Box::pin(async move {
            self.record(PolicyEvent::Stopped {
                operation,
                turn,
                cause,
            });
            let wait = locked(&self.held_stops).remove(&operation);
            if let Some(wait) = wait {
                wait.await.expect("release policy stop");
            }
        })
    }
}

struct LiveBinding;

impl RequestCancellationSignal for LiveBinding {
    fn is_cancelled(&self) -> bool {
        false
    }

    fn cancelled(&self) -> RequestFuture<'_, ()> {
        Box::pin(std::future::pending())
    }
}

fn locked<T>(value: &Mutex<T>) -> MutexGuard<'_, T> {
    value.lock().expect("test policy state")
}

pub(super) struct Peer {
    lines: tokio::io::Lines<BufReader<ReadHalf<DuplexStream>>>,
    writer: WriteHalf<DuplexStream>,
}

impl Peer {
    pub(super) async fn read(&mut self) -> Value {
        let line = self
            .lines
            .next_line()
            .await
            .expect("read raw provider input")
            .expect("ACP pipe open");
        serde_json::from_str(&line).expect("valid wire frame")
    }

    pub(super) async fn send(&mut self, message: Value) {
        self.writer
            .write_all(format!("{message}\n").as_bytes())
            .await
            .expect("write raw frame");
        self.writer.flush().await.expect("flush raw frame");
    }

    pub(super) async fn respond(&mut self, request: &Value, result: Value) {
        self.send(json!({"jsonrpc":"2.0","id":request["id"],"result":result}))
            .await;
    }

    pub(super) async fn sync(&mut self) {
        self.send(json!({"jsonrpc":"2.0","id":"fixture-sync","method":"fixture/sync","params":{}}))
            .await;
        let reply = self.read().await;
        assert_eq!(
            reply["id"], "fixture-sync",
            "unexpected frame before sentinel: {reply}"
        );
        assert_eq!(reply["error"]["code"], -32601);
    }

    pub(super) async fn create_session(&mut self, client: &Client, session: &str) {
        let (created, ()) = tokio::join!(client.create_session(std::env::temp_dir()), async {
            let request = self.read().await;
            assert_eq!(request["method"], "session/new");
            self.respond(&request, json!({"sessionId":session})).await;
        },);
        assert!(created.is_ok(), "create the real owner: {created:?}");
    }

    pub(super) async fn start_prompt(
        &mut self,
        client: &Arc<Client>,
        session: &str,
        operation: u64,
    ) -> (Value, tokio::task::JoinHandle<PromptResult>) {
        let session = session.to_owned();
        let client = Arc::clone(client);
        let prompt = tokio::spawn(async move {
            client
                .submit_with_approval_dispatch_for_input(
                    session,
                    InputId::generate(),
                    ProviderTurnInput::Prompt(ProviderSubmission::plain(vec![
                        PromptContent::text("test prompt".into()).expect("fixture content"),
                    ])),
                    operation,
                    None,
                )
                .await
        });
        let frame = self.read().await;
        assert_eq!(frame["method"], "session/prompt");
        (frame, prompt)
    }
}

pub(super) async fn connect(policy: Arc<Policy>) -> (Arc<Client>, Peer) {
    let (io, peer) = tokio::io::duplex(65_536);
    let (reader, writer) = tokio::io::split(peer);
    let (deliver_peer, receive_peer) = oneshot::channel();
    let agent = tokio::spawn(async move {
        let mut peer = Peer {
            lines: BufReader::new(reader).lines(),
            writer,
        };
        let request = peer.read().await;
        assert_eq!(request["method"], "initialize");
        peer.respond(
            &request,
            json!({"protocolVersion":1,"agentCapabilities":{}}),
        )
        .await;
        assert!(deliver_peer.send(peer).is_ok());
        std::future::pending::<()>().await;
    });
    let client = Arc::new(
        Client::initialize_in_process(
            io,
            agent,
            policy.clone(),
            Arc::new(DiscardingSink),
            Arc::new(crate::test_behavior::TestProviderBehavior),
            Arc::new(router_clock::ManualClock::new(chrono::DateTime::UNIX_EPOCH)),
        )
        .await
        .expect("initialize real ACP connection"),
    );
    assert!(policy.client.set(Arc::downgrade(&client)).is_ok());
    let peer = receive_peer.await.expect("the initialized raw peer");
    (client, peer)
}

struct DiscardingSink;

impl SessionEventSink for DiscardingSink {
    fn publish_skill_view(&self, _session: &str, _view: SkillView) -> Result<(), EventSinkClosed> {
        Ok(())
    }
    fn begin_history_replay(&self, _session: &str) -> HistoryReplayFuture<'_> {
        Box::pin(async { Ok(()) })
    }
    fn publish(&self, _session: &str, _event: SessionEvent) -> Result<(), EventSinkClosed> {
        Ok(())
    }
}
