use std::sync::Arc;

use collaboration_contracts::{
    ProviderConversationBackend, SessionTokenAtCreation, SessionTokenIssuance,
    SessionTokenIssuanceFuture, SessionTokenNotIssued, V0SessionTokenIssuer,
};
use collaboration_protocol::{
    ConversationAdmissionState, ConversationCreateRequest, ConversationOperationSettlement,
    ConversationOperationWaitOutput, ConversationOperationWaitRequest, EndpointId, NonEmptyText,
    OperationId, PositiveSeconds, ProviderBindingId, ProviderKind, ProviderRequestedPolicy,
    ProviderRequestedSettings, ProviderWorkingDirectory, RouterAccess, SessionRef,
};
use tokio::sync::{Mutex, mpsc, oneshot};

use crate::{
    ExternalProviderRuntime,
    external_provider_runtime::acp_scripted_fixture::{launch_fixture_agent, manual_clock},
    external_provider_supervisor::{ExternalProviderBinding, ExternalProviderSupervisor},
};

struct IssuanceCall {
    session: SessionRef,
    outcome: SessionTokenIssuance,
    release: oneshot::Sender<()>,
}

struct ObservedIssuer {
    result: Option<SessionTokenNotIssued>,
    calls: mpsc::UnboundedSender<IssuanceCall>,
}

impl SessionTokenAtCreation for ObservedIssuer {
    fn request<'session>(
        &'session self,
        session: &'session SessionRef,
    ) -> SessionTokenIssuanceFuture<'session> {
        Box::pin(async move {
            let outcome = match self.result {
                Some(reason) => SessionTokenIssuance::NotIssued(reason),
                None => V0SessionTokenIssuer.request(session).await,
            };
            let (release, held) = oneshot::channel();
            self.calls
                .send(IssuanceCall {
                    session: session.clone(),
                    outcome,
                    release,
                })
                .unwrap();
            let _released = held.await;
            outcome
        })
    }
}

struct CreatedSessionFixture {
    backend: ExternalProviderSupervisor,
    request: ConversationCreateRequest,
    calls: mpsc::UnboundedReceiver<IssuanceCall>,
    _directory: tempfile::TempDir,
}

impl CreatedSessionFixture {
    async fn claude(not_issued: Option<SessionTokenNotIssued>) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let working = root.join("project");
        std::fs::create_dir(&working).unwrap();
        let clock = manual_clock();
        let runtime = ExternalProviderRuntime::initialize(
            launch_fixture_agent(
                "permission-mode-agent.ts",
                vec![
                    root.join("receipt.json").display().to_string(),
                    "claude".to_owned(),
                    "grants-directories".to_owned(),
                ],
            ),
            Arc::new(acp_provider_adapters::ClaudeAcpBehavior::new(
                acp_provider_adapters::ClaudeSkillSource::Annotated,
            )),
            clock.clone(),
        )
        .await
        .unwrap();
        let store = switchboard_session_store::ProviderOperationStore::open(
            &root.join("operations.sqlite"),
        )
        .await
        .unwrap();
        let mut identity = crate::external_provider_supervisor::tests::binding();
        identity.endpoint.endpoint_id = EndpointId::try_from("claude-local".to_owned()).unwrap();
        identity.binding_id = ProviderBindingId::try_from("claude-binding".to_owned()).unwrap();
        identity.runtime.provider = ProviderKind::ClaudeCode;
        identity.runtime.runtime_name = NonEmptyText::try_from("claude".to_owned()).unwrap();
        let endpoint = identity.endpoint.clone();
        let requester = crate::external_provider_supervisor::tests::requester();
        let (called, calls) = mpsc::unbounded_channel();
        let backend = ExternalProviderSupervisor::new_with_session_tokens(
            vec![ExternalProviderBinding { identity, runtime }],
            Arc::new(Mutex::new(store)),
            None,
            None,
            clock,
            Arc::new(ObservedIssuer {
                result: not_issued,
                calls: called,
            }),
        )
        .unwrap();
        let request = ConversationCreateRequest {
            operation_id: OperationId::generate(),
            endpoint,
            generation: None,
            working_directory: ProviderWorkingDirectory::try_from(working.display().to_string())
                .unwrap(),
            created_by: requester.clone().into(),
            approver: requester.into(),
            requested_policy: ProviderRequestedPolicy {
                access: RouterAccess::WorkspaceWrite,
            },
            settings: Some(ProviderRequestedSettings {
                mode: Some("plan".to_owned()),
                ..ProviderRequestedSettings::default()
            }),
            work_roots: Vec::new(),
        };
        Self {
            backend,
            request,
            calls,
            _directory: directory,
        }
    }

    fn wait_request(&self) -> ConversationOperationWaitRequest {
        ConversationOperationWaitRequest {
            operation_id: self.request.operation_id.clone(),
            timeout_seconds: PositiveSeconds::try_from(30).unwrap(),
        }
    }

    async fn complete_create(&mut self, expected: SessionTokenNotIssued) -> SessionRef {
        let submitted = self.backend.create(self.request.clone()).await.unwrap();
        assert_eq!(submitted.admission, ConversationAdmissionState::Admitted);

        let mut settlement = self.backend.wait(self.wait_request());
        let called = tokio::select! {
            biased;
            called = self.calls.recv() => called.unwrap(),
            result = &mut settlement => panic!("create settled before its token request: {result:?}"),
        };
        assert_eq!(called.session.endpoint, self.request.endpoint);
        assert_eq!(called.outcome, SessionTokenIssuance::NotIssued(expected));
        let before_handoff = self
            .backend
            .lookup_existing(self.request.operation_id.clone())
            .await
            .unwrap()
            .unwrap();
        assert!(
            before_handoff.terminal_at.is_none(),
            "the token request must precede the create's handoff"
        );
        called.release.send(()).unwrap();

        let settled = tokio::select! {
            biased;
            duplicate = self.calls.recv() => panic!("create called its token port again for {:?}", duplicate.unwrap().session),
            settled = &mut settlement => settled.unwrap(),
        };
        let ConversationOperationWaitOutput::Available {
            settlement: ConversationOperationSettlement::Created { target, .. },
        } = settled.output
        else {
            panic!(
                "token not being issued must not change the create settlement: {:?}",
                settled.output
            );
        };
        assert_eq!(
            called.session, target,
            "the port must receive the actual new session identity"
        );
        target
    }
}

#[tokio::test]
async fn managed_claude_creation_calls_v0_once_before_handoff_and_replay_does_not_call_it() {
    let mut fixture = CreatedSessionFixture::claude(None).await;
    fixture
        .complete_create(SessionTokenNotIssued::DeferredInV0)
        .await;
    let replay = fixture
        .backend
        .create(fixture.request.clone())
        .await
        .unwrap();
    assert_eq!(replay.admission, ConversationAdmissionState::Existing);
    assert!(
        fixture.calls.try_recv().is_err(),
        "one issued create calls the port only once"
    );
    fixture.backend.shutdown().await.unwrap();
}

#[tokio::test]
async fn managed_claude_creation_ignores_a_token_refusal_and_unavailability() {
    for reason in [
        SessionTokenNotIssued::Refused,
        SessionTokenNotIssued::Unavailable,
    ] {
        let mut fixture = CreatedSessionFixture::claude(Some(reason)).await;
        let target = fixture.complete_create(reason).await;
        let inspected = fixture
            .backend
            .inspect_session(collaboration_protocol::ProviderSessionInspectRequest {
                target: target.clone(),
            })
            .await
            .unwrap();
        assert_eq!(inspected.target, target);
        assert!(fixture.calls.try_recv().is_err());
        fixture.backend.shutdown().await.unwrap();
    }
}
