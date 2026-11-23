use std::{
    collections::{BTreeMap, VecDeque},
    future::{Future, ready},
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    Router,
    body::{Body, Bytes, to_bytes},
    extract::{Request, State},
    http::{HeaderMap, Method, StatusCode, header},
    response::Response,
};
use router_clock::{ClockInstant, ManualClock, RuntimeClock, WaitRegistration};

use super::attempt_checkpoint::{AttemptCheckpoints, RequestTrace};
use switchboard_machine_enrollment::{
    BindingGeneration, ConditionalWrite, CredentialStore, CredentialStoreError, EnrollmentBinding,
    EnrollmentEnd, EnrollmentFailure, EnrollmentReceipt, EnrollmentRequest, OrganizationId,
    SignInClient, StoreDurability, StoredEnrollment,
};
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use url::Url;
use workspace_ids::MachineId;

pub const ROUTER_CLIENT: &str = "019a0000-0000-7000-8000-000000000001";
pub const ENROLLMENT_ID: &str = "019a0000-0000-7000-8000-000000000002";
const MACHINE: &str = "019a0000-0000-7000-8000-000000000005";
const ORGANIZATION: &str = "019a0000-0000-7000-8000-000000000006";
pub const DEVICE_SECRET: &str = "test-secret-device-1154718";
pub const INITIAL_ACCESS: &str = "test-initial-access-7303-not-a-jwt";
pub const INITIAL_REFRESH: &str = "test-initial-refresh-118409";
pub const ROTATED_ACCESS: &str = "test-rotated-access-2495-not-a-jwt";
pub const ROTATED_REFRESH: &str = "test-rotated-refresh-5084643";
pub const USER_CODE: &str = "SHOP-LION";

pub struct Reply {
    status: StatusCode,
    body: Body,
    retry_after: Option<&'static str>,
    headers: HeaderMap,
    held: Option<oneshot::Receiver<()>>,
}

impl Reply {
    pub fn failure(status: StatusCode, reason: &str) -> Self {
        Self::new(status, serde_json::json!({"error": reason}).to_string())
    }

    pub fn new(status: StatusCode, body: String) -> Self {
        Self {
            status,
            body: Body::from(body),
            retry_after: None,
            headers: HeaderMap::new(),
            held: None,
        }
    }

    pub fn chunked(status: StatusCode, chunks: Vec<String>) -> Self {
        let body = Body::from_stream(futures_util::stream::iter(
            chunks
                .into_iter()
                .map(|chunk| Ok::<_, std::convert::Infallible>(Bytes::from(chunk))),
        ));
        Self {
            status,
            body,
            retry_after: None,
            headers: HeaderMap::new(),
            held: None,
        }
    }

    pub fn hold(mut self) -> (Self, oneshot::Sender<()>) {
        let (release, held) = oneshot::channel();
        self.held = Some(held);
        (self, release)
    }

    pub fn rate_limit(retry_after: &'static str) -> Self {
        Self::new(StatusCode::TOO_MANY_REQUESTS, String::new()).after(retry_after)
    }

    pub fn after(mut self, value: &'static str) -> Self {
        self.retry_after = Some(value);
        self
    }

    pub fn with_headers(mut self, headers: HeaderMap) -> Self {
        self.headers = headers;
        self
    }
}

pub fn token_reply(access: &str, refresh: &str) -> Reply {
    Reply::new(
        StatusCode::OK,
        serde_json::json!({
            "access_token": access,
            "refresh_token": refresh,
            "expires_in": 60,
            "expires_at": 60,
            "token_type": "Bearer",
            "scope": "offline_access",
            "machine_client_id": ENROLLMENT_ID
        })
        .to_string(),
    )
}

pub struct Observation {
    pub method: Method,
    pub target: String,
    pub at: ClockInstant,
    wait_checkpoint: usize,
    active_waits: Vec<WaitRegistration>,
    pub content_type: Option<String>,
    pub authorization: Option<String>,
    pub form: BTreeMap<String, String>,
}

impl Observation {
    pub fn assert_at(&self, seconds: u64) {
        assert_eq!(self.at, at(seconds), "time of {}", self.target);
    }

    fn assert_request(&self, method: Method, target: &str, fields: &[(&str, &str)]) {
        assert_eq!(self.method, method);
        assert_eq!(self.target, target);
        assert!(
            self.authorization.is_none(),
            "the token route takes no authorization header"
        );
        let expected_type = (!fields.is_empty()).then_some("application/x-www-form-urlencoded");
        assert_eq!(self.content_type.as_deref(), expected_type);
        assert_eq!(self.form.len(), fields.len(), "wrong set of form keys");
        for &(key, value) in fields {
            assert!(
                self.form.get(key).is_some_and(|field| field == value),
                "wrong form field {key}"
            );
        }
    }

    pub fn assert_discovery(&self) {
        self.assert_request(Method::GET, "/.well-known/fleet-login", &[]);
    }

    pub fn assert_start(&self) {
        self.assert_request(
            Method::POST,
            "/api/auth/device/code",
            &[
                ("client_id", ROUTER_CLIENT),
                ("machine_id", MACHINE),
                ("organization_id", ORGANIZATION),
                ("machine_label", "Test Mac"),
                ("scope", "offline_access"),
            ],
        );
    }

    pub fn assert_poll(&self) {
        self.assert_request(
            Method::POST,
            "/api/auth/oauth2/token",
            &[
                ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
                ("client_id", ROUTER_CLIENT),
                ("device_code", DEVICE_SECRET),
            ],
        );
    }

    pub fn assert_refresh(&self, token: &str) {
        self.assert_request(
            Method::POST,
            "/api/auth/oauth2/token",
            &[
                ("grant_type", "refresh_token"),
                ("client_id", ENROLLMENT_ID),
                ("refresh_token", token),
            ],
        );
    }

    pub fn visible_wire_fields(&self) -> String {
        format!(
            "{} {} {:?} {:?}",
            self.method, self.target, self.content_type, self.authorization
        )
    }
}

struct ScriptState {
    replies: Mutex<VecDeque<Reply>>,
    observed: mpsc::UnboundedSender<Observation>,
    clock: Arc<ManualClock>,
}

impl ScriptState {
    fn next_reply(&self) -> Reply {
        self.replies
            .lock()
            .expect("script replies")
            .pop_front()
            .unwrap_or_else(|| Reply::failure(StatusCode::BAD_REQUEST, "unexpected_request"))
    }
}

async fn serve_request(State(state): State<Arc<ScriptState>>, request: Request) -> Response {
    let observed_at = state.clock.monotonic_now();
    let (request, body) = request.into_parts();
    let body = to_bytes(body, 64 * 1024).await.expect("request body");
    let mut fields = BTreeMap::<String, String>::new();
    for (name, value) in url::form_urlencoded::parse(&body) {
        assert!(
            fields
                .insert(name.into_owned(), value.into_owned())
                .is_none(),
            "duplicate form key"
        );
    }
    let header = |name| {
        request
            .headers
            .get(name)
            .map(|value| value.to_str().expect("header is text").to_owned())
    };
    let reply = state.next_reply();
    state
        .observed
        .send(Observation {
            method: request.method,
            target: request.uri.to_string(),
            at: observed_at,
            wait_checkpoint: state.clock.wait_checkpoint(),
            active_waits: active_waits_since(&state.clock, 0),
            content_type: header(header::CONTENT_TYPE),
            authorization: header(header::AUTHORIZATION),
            form: fields,
        })
        .expect("test observes requests");
    if let Some(held) = reply.held {
        let _ = held.await;
    }
    let mut response = Response::builder().status(reply.status);
    response
        .headers_mut()
        .expect("script response headers")
        .extend(reply.headers);
    if let Some(wait) = reply.retry_after {
        response = response.header(header::RETRY_AFTER, wait);
    }
    response.body(reply.body).expect("script response")
}

pub fn at(seconds: u64) -> ClockInstant {
    ClockInstant::ZERO.after(Duration::from_secs(seconds))
}

pub fn machine_id() -> MachineId {
    MACHINE.parse().expect("valid machine id")
}

pub fn enrollment_request(machine_id: &MachineId) -> EnrollmentRequest<'_> {
    EnrollmentRequest {
        machine_id,
        machine_label: "  Test Mac  ",
        organization_id: organization_id(),
    }
}

pub fn organization_id() -> OrganizationId {
    ORGANIZATION.parse().expect("valid organization id")
}

#[derive(Clone)]
pub struct ScriptStore {
    saved: Arc<Mutex<StoredEnrollment>>,
    submitted: Arc<Mutex<Vec<EnrollmentBinding>>>,
    durability: StoreDurability,
}

impl Default for ScriptStore {
    fn default() -> Self {
        Self::new(StoreDurability::Durable)
    }
}

impl ScriptStore {
    pub fn new(durability: StoreDurability) -> Self {
        Self {
            saved: Arc::new(Mutex::new(StoredEnrollment::Empty)),
            submitted: Arc::new(Mutex::new(Vec::new())),
            durability,
        }
    }

    pub fn snapshot(&self) -> StoredEnrollment {
        self.saved.lock().expect("fake store").clone()
    }

    pub fn current(&self) -> Option<EnrollmentBinding> {
        match self.snapshot() {
            StoredEnrollment::Active(binding) => Some(binding),
            _ => None,
        }
    }

    pub fn submissions(&self) -> Vec<EnrollmentBinding> {
        self.submitted
            .lock()
            .expect("fake store submissions")
            .clone()
    }

    fn write(
        &self,
        expected: BindingGeneration,
        replacement: StoredEnrollment,
    ) -> ConditionalWrite {
        let mut saved = self.saved.lock().expect("fake store");
        if saved.generation() != expected {
            return ConditionalWrite::Superseded;
        }
        assert_eq!(expected.successor(), Some(replacement.generation()));
        *saved = replacement;
        ConditionalWrite::Applied
    }
}

impl CredentialStore for ScriptStore {
    fn durability(&self) -> StoreDurability {
        self.durability
    }

    fn read(&self) -> impl Future<Output = Result<StoredEnrollment, CredentialStoreError>> + Send {
        ready(Ok(self.snapshot()))
    }

    fn replace(
        &self,
        expected: BindingGeneration,
        binding: &EnrollmentBinding,
    ) -> impl Future<Output = Result<ConditionalWrite, CredentialStoreError>> + Send {
        self.submitted
            .lock()
            .expect("fake store submissions")
            .push(binding.clone());
        ready(Ok(
            self.write(expected, StoredEnrollment::Active(binding.clone()))
        ))
    }

    fn remove(
        &self,
        expected: BindingGeneration,
        why: EnrollmentEnd,
    ) -> impl Future<Output = Result<ConditionalWrite, CredentialStoreError>> + Send {
        let snapshot = self.snapshot();
        let Some((key, generation)) = snapshot.key().zip(expected.successor()) else {
            return ready(Ok(ConditionalWrite::Superseded));
        };
        ready(Ok(self.write(
            expected,
            StoredEnrollment::Ended {
                generation,
                key,
                why,
            },
        )))
    }

    fn mark_unusable(
        &self,
        expected: BindingGeneration,
        why: EnrollmentEnd,
    ) -> impl Future<Output = Result<ConditionalWrite, CredentialStoreError>> + Send {
        self.remove(expected, why)
    }
}

pub struct SignInScript {
    pub clock: Arc<ManualClock>,
    pub client: SignInClient,
    pub observations: mpsc::UnboundedReceiver<Observation>,
    state: Arc<ScriptState>,
    server: JoinHandle<()>,
    transport: Arc<RequestTrace>,
    attempt: Option<AttemptCheckpoints>,
    observed_requests: usize,
    cadence_observed: bool,
    wait_checkpoint: usize,
}

impl SignInScript {
    pub async fn start(expires_in: u64, polls: impl IntoIterator<Item = Reply>) -> Self {
        Self::from_replies(|issuer| {
            Self::enrollment_prelude(issuer, expires_in, None)
                .into_iter()
                .chain(polls)
                .collect()
        })
        .await
    }

    pub async fn token_responses(replies: impl IntoIterator<Item = Reply>) -> Self {
        Self::from_replies(|_| replies.into_iter().collect()).await
    }

    pub async fn hold_start(
        expires_in: u64,
        polls: impl IntoIterator<Item = Reply>,
    ) -> (Self, oneshot::Sender<()>) {
        let (release, held) = oneshot::channel();
        (
            Self::from_replies(|issuer| {
                Self::enrollment_prelude(issuer, expires_in, Some(held))
                    .into_iter()
                    .chain(polls)
                    .collect()
            })
            .await,
            release,
        )
    }

    fn enrollment_prelude(
        issuer: &str,
        expires_in: u64,
        held_start: Option<oneshot::Receiver<()>>,
    ) -> [Reply; 2] {
        let discovery = Reply::new(
            StatusCode::OK,
            serde_json::json!({"client_id": ROUTER_CLIENT, "link_url": "https://central.example/v1/organizations/{organization_id}/link"}).to_string(),
        );
        let mut device = Reply::new(
            StatusCode::OK,
            serde_json::json!({
                "device_code": DEVICE_SECRET,
                "user_code": USER_CODE,
                "verification_uri": format!("{issuer}/device"),
                "verification_uri_complete": format!("{issuer}/device?user_code={USER_CODE}"),
                "expires_in": expires_in,
                "interval": 5,
            })
            .to_string(),
        );
        device.held = held_start;
        [discovery, device]
    }

    pub async fn from_replies(replies: impl FnOnce(&str) -> VecDeque<Reply>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("loopback listener");
        let issuer = format!("http://{}", listener.local_addr().expect("bound address"));
        let clock = Arc::new(ManualClock::new(chrono::DateTime::UNIX_EPOCH));
        let (observed, observations) = mpsc::unbounded_channel();
        let state = Arc::new(ScriptState {
            replies: Mutex::new(replies(&issuer)),
            observed,
            clock: Arc::clone(&clock),
        });
        let app = Router::new()
            .fallback(serve_request)
            .with_state(Arc::clone(&state));
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("scripted sign-in");
        });
        let transport = Arc::new(RequestTrace::default());
        let observation = Arc::clone(&transport);
        let client = SignInClient::new_loopback_for_test(
            Url::parse(&issuer).expect("sign-in origin"),
            Arc::clone(&clock) as Arc<dyn RuntimeClock + Send + Sync>,
            move |event| observation.record(event),
        )
        .expect("client");
        Self {
            clock,
            client,
            observations,
            state,
            server,
            transport,
            attempt: None,
            observed_requests: 0,
            cadence_observed: false,
            wait_checkpoint: 0,
        }
    }

    pub fn spawn_attempt(
        &mut self,
        attempt: impl Future<Output = Result<EnrollmentReceipt, EnrollmentFailure>> + Send + 'static,
    ) -> JoinHandle<Result<EnrollmentReceipt, EnrollmentFailure>> {
        let (checkpoints, task) = AttemptCheckpoints::spawn(attempt, Arc::clone(&self.transport));
        self.attempt = Some(checkpoints);
        task
    }

    pub async fn next_request(&mut self) -> Observation {
        let request = if let Ok(request) = self.observations.try_recv() {
            request
        } else if let Some(attempt) = &mut self.attempt {
            let progress = if self.observed_requests == 0 || self.cadence_observed {
                attempt.drive().await
            } else {
                attempt.after_response(self.observed_requests).await
            };
            assert!(
                progress.dispatched > self.observed_requests,
                "enrollment must dispatch the next expected request before parking or finishing"
            );
            tokio::select! {
                biased;
                request = self.observations.recv() => request,
                () = attempt.finished() => None,
            }
            .expect("started HTTP request must reach the scripted server")
        } else {
            self.observations
                .recv()
                .await
                .expect("script sees next request")
        };
        assert!(
            request
                .active_waits
                .iter()
                .any(|wait| wait.deadline == request.at.after(Duration::from_secs(30))),
            "wire request must have its injected HTTP timeout"
        );
        self.cadence_observed = false;
        self.observed_requests += 1;
        self.wait_checkpoint = request.wait_checkpoint;
        request
    }

    pub async fn wait_for_poll(&mut self, delay: u64, due: u64) {
        self.observe_poll_wait(due).await;
        self.clock
            .advance(Duration::from_secs(delay))
            .expect("advance manual clock");
    }

    pub async fn observe_poll_wait(&mut self, due: u64) {
        let attempt = self.attempt.as_mut().expect("driven enrollment attempt");
        let progress = attempt.after_response(self.observed_requests).await;
        assert_eq!(
            progress.dispatched, self.observed_requests,
            "client polled before its deadline"
        );
        let waits = active_waits_since(&self.clock, self.wait_checkpoint);
        let deadlines: Vec<_> = waits.iter().map(|wait| wait.deadline).collect();
        assert_eq!(deadlines, [at(due)], "expected poll deadline was not armed");
        self.cadence_observed = true;
    }

    pub async fn finish_before_more_polling(
        &mut self,
        attempt: JoinHandle<Result<EnrollmentReceipt, EnrollmentFailure>>,
    ) -> Option<Result<EnrollmentReceipt, EnrollmentFailure>> {
        let checkpoints = self.attempt.as_mut().expect("driven enrollment attempt");
        let progress = if self.cadence_observed {
            checkpoints.drive().await
        } else {
            checkpoints.after_response(self.observed_requests).await
        };
        if !progress.finished || progress.dispatched != self.observed_requests {
            attempt.abort();
            return None;
        }
        Some(attempt.await.expect("enrollment task"))
    }

    pub fn assert_last_poll_bound(&self, due: u64) {
        assert!(
            active_waits_since(&self.clock, 0)
                .iter()
                .any(|wait| wait.deadline == at(due)),
            "last in-flight response bound"
        );
    }

    pub fn assert_exhausted(&mut self) {
        assert!(
            self.state
                .replies
                .lock()
                .expect("script replies")
                .is_empty()
        );
        assert!(
            self.observations.try_recv().is_err(),
            "unexpected extra request"
        );
    }
}

impl Drop for SignInScript {
    fn drop(&mut self) {
        self.server.abort();
    }
}

fn active_waits_since(clock: &ManualClock, checkpoint: usize) -> Vec<WaitRegistration> {
    clock
        .wait_registrations_since(checkpoint)
        .into_iter()
        .filter(|wait| wait.first_polled_at.is_some() && !wait.dropped)
        .collect()
}
