use std::{
    collections::HashMap,
    future::ready,
    sync::{Arc, Mutex},
};

use collaboration_central_tokio::test_support::{MachineLinkFixture, start_machine_link_fixture};
use collaboration_mcp::{
    CollaborationApiConfig, FixedRouterExecutableRelation, ManagedToolBindings,
    managed_collaboration_api_router, serve_collaboration_api,
};
use collaboration_protocol::{
    ManagedSessionToolBinding, ManagedStartResolver, ManagedToolAdmissionLease,
    ManagedToolDescriptor, ManagedToolResolution, PrepareManagedToolDescriptor,
    ResolveManagedStartFuture, RouterExecutableRelation,
    message_tools::{
        MessageCallerRefusal, MessageInboxPage, MessageSendOutcome, MessageToolRefusal,
        MessageToolResult, MessageWriteStatus,
    },
};
use collaboration_service::{
    ApplicationFixture,
    collaboration_application::message_operations::{
        MessageOperations, MessageSessionDirectory, MessageSessionPresence, V0MessageAdmission,
    },
};
use rmcp::{
    RoleClient, ServiceExt as _,
    model::{CallToolRequestParams, CallToolResult},
    service::{ClientInitializeError, RunningService, ServiceError},
    transport::{
        StreamableHttpClientTransport, streamable_http_client::StreamableHttpClientTransportConfig,
    },
};
use router_clock::{ManualClock, RuntimeClock};
use serde_json::{Value, json};
use switchboard_machine_core_store::{
    InboxDecision, InboxOfferExpiry, InboxStore, MACHINE_LOCAL_DATABASE, MachineLocalStore,
};
use switchboard_message_system::CloseReport;
use tokio::{net::TcpListener, task::JoinHandle};
use workspace_ids::{ManagedToolBindingId, SessionId, SessionRef};
use workspace_model::central_mail::{MessageBody, MessageDelivery, MessageRecord};

#[cfg(target_os = "macos")]
#[path = "managed_claude_catalog.rs"]
mod managed_claude_catalog;

use super::{
    first_message_local::{EnrolledFixtureMac, RunningFixtureMac},
    message_unit_fixture::{EPOCH, KnownRecipient},
};

const FIRST_BODY: &str = "only the first managed owner can read this";
const SECOND_BODY: &str = "only the second managed owner can read this";
const FIRST_MESSAGE: &str = "019f0000-0000-7000-8000-000000000091";
const SECOND_MESSAGE: &str = "019f0000-0000-7000-8000-000000000092";
const SEND_REQUEST: &str = "019f0000-0000-7000-8000-0000000000ad";

#[derive(Clone)]
enum TestPhase {
    Pending,
    Open(SessionRef),
    Retired,
}

#[derive(Default)]
struct TestStartResolver(Mutex<HashMap<ManagedToolBindingId, TestPhase>>);

struct TestLease;
impl ManagedToolAdmissionLease for TestLease {}

impl ManagedStartResolver for TestStartResolver {
    fn resolve_managed_start<'lookup>(
        &'lookup self,
        binding: &'lookup ManagedSessionToolBinding,
    ) -> ResolveManagedStartFuture<'lookup> {
        let resolved = match self.0.lock().unwrap().get(&binding.binding).cloned() {
            Some(TestPhase::Pending) => ManagedToolResolution::Pending,
            Some(TestPhase::Open(address)) => ManagedToolResolution::Open {
                address,
                lease: Box::new(TestLease),
            },
            Some(TestPhase::Retired) | None => ManagedToolResolution::Retired,
        };
        Box::pin(ready(Ok(resolved)))
    }
}

struct ExternalDirectory;

impl MessageSessionDirectory for ExternalDirectory {
    fn lookup<'lookup>(
        &'lookup self,
        session_id: &'lookup SessionId,
    ) -> std::pin::Pin<Box<dyn Future<Output = MessageSessionPresence> + Send + 'lookup>> {
        Box::pin(ready(if session_id.as_str() == "terminal" {
            MessageSessionPresence::KnownOnce
        } else {
            MessageSessionPresence::UnknownSession
        }))
    }
}

struct ManagedMcpJourney {
    config: CollaborationApiConfig,
    root_mac: RunningFixtureMac,
    central: MachineLinkFixture,
    listener: JoinHandle<std::io::Result<()>>,
    bindings: Arc<ManagedToolBindings>,
    resolver: Arc<TestStartResolver>,
    shared_url: String,
    first: SessionRef,
    second: SessionRef,
}

impl ManagedMcpJourney {
    async fn start() -> Self {
        let clock = Arc::new(ManualClock::new(EPOCH.parse().unwrap()));
        let central = start_machine_link_fixture(clock.clone()).await.unwrap();
        let mac = EnrolledFixtureMac::enroll(
            &central,
            "managed-first",
            "019f0000-0000-7000-8000-00000000cd11".parse().unwrap(),
        )
        .await;
        let first = mac.address.clone();
        let second = SessionRef {
            machine_id: first.machine_id.clone(),
            session_id: "managed-second".parse().unwrap(),
        };
        let running = mac
            .start(
                &central,
                Arc::new(KnownRecipient(first.session_id.clone())),
                None,
            )
            .await;
        let mut inbox = InboxStore::new(
            MachineLocalStore::open(&mac.root.join(MACHINE_LOCAL_DATABASE))
                .await
                .unwrap(),
        );
        let now = clock.utc_now().timestamp_millis();
        let expiry = "2030-03-17T17:47:40Z".parse().unwrap();
        for (recipient, message_id, body) in [
            (&first, FIRST_MESSAGE, FIRST_BODY),
            (&second, SECOND_MESSAGE, SECOND_BODY),
        ] {
            let message = MessageRecord::queued_central(
                message_id.parse().unwrap(),
                recipient.clone(),
                MessageBody::Direct {
                    body: body.to_owned().try_into().unwrap(),
                    delivery: MessageDelivery::Auto,
                    reply_to: None,
                },
                clock.utc_now(),
            );
            inbox
                .record_delivery(
                    &message,
                    InboxDecision::Accepted,
                    now,
                    InboxOfferExpiry::DirectWithExpiry(expiry),
                )
                .await
                .unwrap();
            assert_eq!(
                inbox
                    .message_for(&message.message_id)
                    .await
                    .unwrap()
                    .healthy,
                Some(message)
            );
        }
        drop(inbox);
        let operations = MessageOperations::new(
            Arc::new(V0MessageAdmission::new(
                first.machine_id.clone(),
                Arc::new(ExternalDirectory),
            )),
            Arc::new(running.front.clone()),
            clock,
        );
        let application = ApplicationFixture::new(
            "019f0000-0000-7000-8000-000000000081",
            "019f0000-0000-7000-8000-000000000082",
        )
        .unwrap()
        .build()
        .with_messages(operations);
        let config = CollaborationApiConfig {
            application,
            service_directory: mac.root,
            native_definitions: None,
            router_executable_check: Arc::new(FixedRouterExecutableRelation(
                RouterExecutableRelation::Match,
            )),
            concurrent_requests: 8,
            shutdown: Default::default(),
        };
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = socket.local_addr().unwrap();
        let resolver = Arc::new(TestStartResolver::default());
        let (api, bindings) =
            managed_collaboration_api_router(&config, address, None, resolver.clone()).unwrap();
        let listener = tokio::spawn(serve_collaboration_api(
            socket,
            api,
            config.shutdown.clone(),
        ));
        Self {
            config,
            root_mac: running,
            central,
            listener,
            bindings,
            resolver,
            shared_url: format!("http://{address}/mcp"),
            first,
            second,
        }
    }

    fn prepare(
        &self,
        start: u8,
        revision: u8,
        phase: TestPhase,
    ) -> (ManagedSessionToolBinding, ManagedToolDescriptor) {
        let binding = ManagedSessionToolBinding {
            start: format!("019f0000-0000-7000-8000-0000000000{start:02x}")
                .parse()
                .unwrap(),
            router_run: "019f0000-0000-7000-8000-0000000000ff".parse().unwrap(),
            binding: format!("019f0000-0000-7000-8000-00000000{start:02x}{revision:02x}")
                .parse()
                .unwrap(),
        };
        self.set_phase(&binding, phase);
        let descriptor = self.bindings.prepare_managed_descriptor(&binding).unwrap();
        (binding, descriptor)
    }

    fn set_phase(&self, binding: &ManagedSessionToolBinding, phase: TestPhase) {
        self.resolver
            .0
            .lock()
            .unwrap()
            .insert(binding.binding.clone(), phase);
    }

    async fn close(self) {
        self.config.shutdown.cancel();
        self.listener.await.unwrap().unwrap();
        assert_eq!(self.root_mac.system.close().await, CloseReport::Closed);
        self.central.close().await.unwrap();
    }
}

type McpClient = RunningService<RoleClient, ()>;

async fn connect(url: &str, bearer: Option<&str>) -> Result<McpClient, Box<ClientInitializeError>> {
    let mut config = StreamableHttpClientTransportConfig::with_uri(url.to_owned());
    config.auth_header = bearer.map(str::to_owned);
    ().serve(StreamableHttpClientTransport::from_config(config))
        .await
        .map_err(Box::new)
}

async fn managed_client(descriptor: &ManagedToolDescriptor) -> McpClient {
    connect(
        &descriptor.url,
        Some(descriptor.bearer.expose_header_value()),
    )
    .await
    .unwrap()
}

async fn call(client: &McpClient, name: &str, args: Value) -> Result<CallToolResult, ServiceError> {
    client
        .call_tool(
            CallToolRequestParams::new(name.to_owned())
                .with_arguments(args.as_object().unwrap().clone()),
        )
        .await
}

async fn read(client: &McpClient) -> MessageInboxPage {
    let result = call(client, "message_inbox", json!({})).await.unwrap();
    let result: MessageToolResult<MessageInboxPage> =
        serde_json::from_value(result.structured_content.unwrap()).unwrap();
    match result {
        MessageToolResult::Returned(page) => Ok(page),
        MessageToolResult::Refused(refusal) => Err(refusal),
    }
    .unwrap()
}

#[tokio::test]
async fn pending_lists_caller_free_catalog_and_refuses_calls_fails_on_early_admission() {
    let fixture = ManagedMcpJourney::start().await;
    let (_, descriptor) = fixture.prepare(1, 1, TestPhase::Pending);
    let client = managed_client(&descriptor).await;
    let mut catalog = client.list_all_tools().await.unwrap();
    catalog.sort_unstable_by(|left, right| left.name.cmp(&right.name));
    assert_eq!(fixture.bindings.completed_handshakes(&descriptor), 1);
    assert_eq!(
        catalog
            .iter()
            .map(|tool| tool.name.as_ref())
            .collect::<Vec<_>>(),
        ["message_inbox", "message_send", "message_status"]
    );
    for tool in catalog {
        assert!(
            !tool.input_schema["properties"]
                .as_object()
                .unwrap()
                .contains_key("callerSessionId")
        );
        assert_eq!(tool.input_schema["additionalProperties"], false);
    }
    let error = call(&client, "message_inbox", json!({})).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("The managed session is still starting"),
        "{error}"
    );
    client.cancel().await.unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn open_reads_exact_inbox_and_owns_writes_fails_on_shared_only_or_unscoped_identity() {
    let fixture = ManagedMcpJourney::start().await;
    let (binding, first) = fixture.prepare(1, 1, TestPhase::Pending);
    let (_, second) = fixture.prepare(2, 1, TestPhase::Open(fixture.second.clone()));
    let first = managed_client(&first).await;
    let second = managed_client(&second).await;
    fixture.set_phase(&binding, TestPhase::Open(fixture.first.clone()));
    let first_page = read(&first).await;
    let second_page = read(&second).await;
    for (page, expected_id, expected_body) in [
        (&first_page, FIRST_MESSAGE, FIRST_BODY),
        (&second_page, SECOND_MESSAGE, SECOND_BODY),
    ] {
        assert_eq!(page.messages.len(), 1);
        assert_eq!(page.messages[0].message_id.as_str(), expected_id);
        assert!(
            matches!(&page.messages[0].body, MessageBody::Direct { body, .. } if body.as_str() == expected_body)
        );
    }
    let crossed = call(
        &first,
        "message_inbox",
        json!({"acknowledge":second_page.read_token}),
    )
    .await
    .unwrap();
    let crossed: MessageToolResult<MessageInboxPage> =
        serde_json::from_value(crossed.structured_content.unwrap()).unwrap();
    assert_eq!(
        crossed,
        MessageToolResult::Refused(MessageToolRefusal::InboxAcknowledgmentNotKept)
    );
    let own = call(
        &first,
        "message_inbox",
        json!({"acknowledge":first_page.read_token}),
    )
    .await
    .unwrap();
    let own: MessageToolResult<MessageInboxPage> =
        serde_json::from_value(own.structured_content.unwrap()).unwrap();
    assert!(
        matches!(own, MessageToolResult::Returned(page) if page.acknowledgment.is_some_and(|ack| ack.changed == 1))
    );
    let sent = call(
        &first,
        "message_send",
        json!({"to":fixture.second,"body":"scoped test from first","requestId":SEND_REQUEST}),
    )
    .await
    .unwrap();
    let sent: MessageToolResult<MessageSendOutcome> =
        serde_json::from_value(sent.structured_content.unwrap()).unwrap();
    assert!(
        matches!(sent, MessageToolResult::Returned(MessageSendOutcome::InOutbox { request_id, .. }) if request_id.as_str() == SEND_REQUEST)
    );
    let first_status = call(&first, "message_status", json!({"requestId":SEND_REQUEST}))
        .await
        .unwrap();
    let second_status = call(&second, "message_status", json!({"requestId":SEND_REQUEST}))
        .await
        .unwrap();
    let first_status: MessageToolResult<MessageWriteStatus> =
        serde_json::from_value(first_status.structured_content.unwrap()).unwrap();
    let second_status: MessageToolResult<MessageWriteStatus> =
        serde_json::from_value(second_status.structured_content.unwrap()).unwrap();
    assert!(matches!(
        first_status,
        MessageToolResult::Returned(MessageWriteStatus::InOutbox { .. })
    ));
    assert_eq!(
        second_status,
        MessageToolResult::Returned(MessageWriteStatus::NotFound)
    );
    first.cancel().await.unwrap();
    second.cancel().await.unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn open_rejects_caller_argument_fails_on_caller_spoofable_managed_handler() {
    let fixture = ManagedMcpJourney::start().await;
    let (_, descriptor) = fixture.prepare(1, 1, TestPhase::Open(fixture.first.clone()));
    let client = managed_client(&descriptor).await;
    for (name, arguments) in [
        ("message_inbox", json!({"callerSessionId":"managed-second"})),
        (
            "message_send",
            json!({"callerSessionId":"managed-second","to":fixture.first,"body":"forged","requestId":SEND_REQUEST}),
        ),
        (
            "message_status",
            json!({"callerSessionId":"managed-second","requestId":SEND_REQUEST}),
        ),
    ] {
        let result = call(&client, name, arguments).await.unwrap();
        assert_eq!(result.is_error, Some(true));
        assert!(result.content.iter().any(|item| {
            item.as_text()
                .is_some_and(|text| text.text.contains("unknown field `callerSessionId`"))
        }));
    }
    client.cancel().await.unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn retired_credential_stays_refused_after_reprepare_fails_on_rebind() {
    let fixture = ManagedMcpJourney::start().await;
    let (binding, prior) = fixture.prepare(1, 1, TestPhase::Open(fixture.first.clone()));
    let prior_client = managed_client(&prior).await;
    fixture.set_phase(&binding, TestPhase::Retired);
    let (_, replacement) = fixture.prepare(1, 2, TestPhase::Open(fixture.second.clone()));
    assert_eq!(prior.url, replacement.url);
    assert!(
        call(&prior_client, "message_inbox", json!({}))
            .await
            .is_err()
    );
    let prepared_again = fixture
        .bindings
        .prepare_managed_descriptor(&binding)
        .unwrap();
    assert!(
        connect(
            &prepared_again.url,
            Some(prepared_again.bearer.expose_header_value())
        )
        .await
        .is_err()
    );
    let replacement_client = managed_client(&replacement).await;
    assert_eq!(
        read(&replacement_client).await.messages[0]
            .message_id
            .as_str(),
        SECOND_MESSAGE
    );
    let _expired = prior_client.cancel().await;
    replacement_client.cancel().await.unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn other_descriptor_bearer_fails_on_address_only_admission() {
    let fixture = ManagedMcpJourney::start().await;
    let (_, address) = fixture.prepare(1, 1, TestPhase::Open(fixture.first.clone()));
    let (_, different) = fixture.prepare(2, 1, TestPhase::Open(fixture.second.clone()));
    assert!(
        connect(&address.url, Some(different.bearer.expose_header_value()))
            .await
            .is_err()
    );
    assert!(connect(&address.url, None).await.is_err());
    let valid = managed_client(&address).await;
    assert_eq!(
        read(&valid).await.messages[0].message_id.as_str(),
        FIRST_MESSAGE
    );
    valid.cancel().await.unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn shared_door_checks_reported_external_id_fails_on_global_managed_admission() {
    let fixture = ManagedMcpJourney::start().await;
    let client = connect(&fixture.shared_url, None).await.unwrap();
    for (args, refusal) in [
        (json!({}), MessageCallerRefusal::CallerRequired),
        (
            json!({"callerSessionId":"unknown"}),
            MessageCallerRefusal::UnknownSession,
        ),
    ] {
        let result = call(&client, "message_inbox", args).await.unwrap();
        let result: MessageToolResult<MessageInboxPage> =
            serde_json::from_value(result.structured_content.unwrap()).unwrap();
        assert_eq!(
            result,
            MessageToolResult::Refused(MessageToolRefusal::Caller(refusal))
        );
    }
    let external = call(
        &client,
        "message_inbox",
        json!({"callerSessionId":"terminal"}),
    )
    .await
    .unwrap();
    let external: MessageToolResult<MessageInboxPage> =
        serde_json::from_value(external.structured_content.unwrap()).unwrap();
    assert!(matches!(external, MessageToolResult::Returned(page) if page.messages.is_empty()));
    client.cancel().await.unwrap();
    fixture.close().await;
}
