use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use reqwest::{
    Client, RequestBuilder,
    header::{CONTENT_LENGTH, RETRY_AFTER},
};
use router_clock::{ClockInstant, RuntimeClock, timeout};
use serde::Serialize;
use url::Url;
use workspace_ids::MachineId;
use zeroize::Zeroizing;

use crate::{
    CredentialStore, DeviceAuthorization, DiscoveryAnswer, EnrollmentContext, OrganizationId,
    PollAnswer, RefreshAnswer, RefreshCredential, RefreshOwner, RefreshSource, RouterClientId,
    SecretValue, SignInDeployment, SignInFailure, decode_device_authorization, decode_discovery,
    decode_poll, decode_refresh, decode_start_failure,
};

const DEVICE_CODE_PATH: &str = "/api/auth/device/code";
const TOKEN_PATH: &str = "/api/auth/oauth2/token";
const RESPONSE_BODY_LIMIT: usize = 64 * 1024;
const SIGN_IN_WAIT: Duration = Duration::from_secs(30);

/// Composition creates this adapter for a single configured sign-in issuer and clock.
#[derive(Clone)]
pub struct SignInClient {
    deployment: SignInDeployment,
    date_anchor: (SystemTime, ClockInstant),
    http: Client,
    pub(crate) clock: Arc<dyn RuntimeClock + Send + Sync>,
    #[cfg(feature = "test-support")]
    loopback_observer: Option<Arc<LoopbackObserver>>,
}

struct HttpRefreshSource(SignInClient);

/// The scripted loopback client reports request progress without exposing wire values.
#[cfg(feature = "test-support")]
pub enum LoopbackRequestEvent {
    Dispatched,
    Settled,
}

#[cfg(feature = "test-support")]
type LoopbackObserver = dyn Fn(LoopbackRequestEvent) + Send + Sync;

#[cfg(feature = "test-support")]
struct ObserveLoopbackSettlement<'observer>(&'observer LoopbackObserver);

#[cfg(feature = "test-support")]
impl Drop for ObserveLoopbackSettlement<'_> {
    fn drop(&mut self) {
        (self.0)(LoopbackRequestEvent::Settled);
    }
}

/// Construction of the sign-in adapter can fail before any request is sent.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SignInClientError {
    #[error("sign-in requires an HTTPS origin")]
    InvalidIssuer,
    #[error("the sign-in HTTP client could not be configured")]
    ClientSetup,
}

/// Composition supplies the installed Mac's identity and the person's organization.
pub struct EnrollmentRequest<'request> {
    pub machine_id: &'request MachineId,
    pub machine_label: &'request str,
    pub organization_id: OrganizationId,
}

#[derive(Serialize)]
struct StartForm<'request> {
    client_id: String,
    machine_id: &'request str,
    machine_label: &'request str,
    organization_id: String,
}

#[derive(Serialize)]
struct DeviceTokenForm<'request> {
    grant_type: &'static str,
    client_id: String,
    device_code: &'request str,
}

#[derive(Serialize)]
struct RefreshForm<'request> {
    grant_type: &'static str,
    client_id: String,
    refresh_token: &'request str,
}

struct HttpAnswer {
    status: reqwest::StatusCode,
    retry_after: Option<Duration>,
    body: Zeroizing<Vec<u8>>,
}

impl SignInClient {
    pub fn new(
        issuer: Url,
        clock: Arc<dyn RuntimeClock + Send + Sync>,
    ) -> Result<Self, SignInClientError> {
        let deployment =
            SignInDeployment::new(issuer).map_err(|_| SignInClientError::InvalidIssuer)?;
        Self::with_deployment(deployment, clock)
    }

    /// Builds the scripted transport for a numeric HTTP loopback origin.
    #[cfg(feature = "test-support")]
    pub fn new_loopback_for_test(
        issuer: Url,
        clock: Arc<dyn RuntimeClock + Send + Sync>,
        observe: impl Fn(LoopbackRequestEvent) + Send + Sync + 'static,
    ) -> Result<Self, SignInClientError> {
        let deployment = SignInDeployment::loopback_for_test(issuer)
            .map_err(|_| SignInClientError::InvalidIssuer)?;
        let mut client = Self::with_deployment(deployment, clock)?;
        client.loopback_observer = Some(Arc::new(observe));
        Ok(client)
    }

    fn with_deployment(
        deployment: SignInDeployment,
        clock: Arc<dyn RuntimeClock + Send + Sync>,
    ) -> Result<Self, SignInClientError> {
        let date_anchor = (clock.utc_now().into(), clock.monotonic_now());
        let http = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .no_proxy()
            .build()
            .map_err(|_| SignInClientError::ClientSetup)?;
        Ok(Self {
            deployment,
            date_anchor,
            http,
            clock,
            #[cfg(feature = "test-support")]
            loopback_observer: None,
        })
    }

    pub fn refresh_owner<Store: CredentialStore + 'static>(
        &self,
        store: Store,
        machine_id: MachineId,
        organization_id: OrganizationId,
    ) -> RefreshOwner<Store, impl RefreshSource + use<Store>> {
        RefreshOwner::new(
            store,
            HttpRefreshSource(self.clone()),
            Arc::clone(&self.clock),
            EnrollmentContext {
                machine_id,
                organization_id,
                deployment: self.deployment.clone(),
            },
        )
    }

    #[must_use]
    pub fn deployment(&self) -> &SignInDeployment {
        &self.deployment
    }

    pub async fn discover(&self) -> Result<DiscoveryAnswer, SignInFailure> {
        let answer = self
            .request(self.http.get(self.endpoint("/.well-known/fleet-login")))
            .await?;
        if answer.status != reqwest::StatusCode::OK {
            return Err(decode_start_failure(
                answer.status.as_u16(),
                answer.retry_after,
                &answer.body,
            ));
        }
        decode_discovery(&answer.body).map_err(|_| malformed_answer())
    }

    pub(crate) async fn start(
        &self,
        client_id: RouterClientId,
        request: EnrollmentRequest<'_>,
    ) -> Result<DeviceAuthorization, SignInFailure> {
        let label = request.machine_label.trim();
        if label.is_empty()
            || label.len() > 256
            || label
                .chars()
                .any(|ch| ch.is_control() && !matches!(ch, '\t' | '\n'))
        {
            return Err(SignInFailure::Invalid {
                reason: crate::InvalidReason::InvalidRequest,
            });
        }
        let form = StartForm {
            client_id: client_id.to_string(),
            machine_id: request.machine_id.as_str(),
            machine_label: label,
            organization_id: request.organization_id.to_string(),
        };
        let answer = self
            .request(self.http.post(self.endpoint(DEVICE_CODE_PATH)).form(&form))
            .await?;
        if answer.status != reqwest::StatusCode::OK {
            return Err(decode_start_failure(
                answer.status.as_u16(),
                answer.retry_after,
                &answer.body,
            ));
        }
        let authorization =
            decode_device_authorization(&answer.body).map_err(|_| malformed_answer())?;
        if authorization.instructions.verification_link.origin()
            != self.deployment.origin().origin()
        {
            return Err(malformed_answer());
        }
        Ok(authorization)
    }

    pub(crate) async fn poll(
        &self,
        client_id: RouterClientId,
        device_code: &SecretValue,
    ) -> PollAnswer {
        let form = DeviceTokenForm {
            grant_type: "urn:ietf:params:oauth:grant-type:device_code",
            client_id: client_id.to_string(),
            device_code: device_code.expose_secret(),
        };
        match self
            .request(self.http.post(self.endpoint(TOKEN_PATH)).form(&form))
            .await
        {
            Ok(answer) => decode_poll(answer.status.as_u16(), answer.retry_after, &answer.body),
            Err(SignInFailure::Unavailable { retry_after }) => {
                PollAnswer::Unavailable { retry_after }
            }
            Err(SignInFailure::Invalid { reason }) => PollAnswer::Invalid { reason },
        }
    }

    fn endpoint(&self, path: &str) -> Url {
        let mut url = self.deployment.origin().clone();
        url.set_path(path);
        url
    }

    async fn request(&self, request: RequestBuilder) -> Result<HttpAnswer, SignInFailure> {
        #[cfg(feature = "test-support")]
        let _observe_settlement = self.loopback_observer.as_deref().map(|observe| {
            observe(LoopbackRequestEvent::Dispatched);
            ObserveLoopbackSettlement(observe)
        });
        // Only classifications cross this edge. reqwest errors and bodies can contain input bytes.
        let unavailable = || SignInFailure::Unavailable { retry_after: None };
        timeout(self.clock.as_ref(), SIGN_IN_WAIT, async move {
            let mut answer = request.send().await.map_err(|_| unavailable())?;
            let status = answer.status();
            let retry_after = answer
                .headers()
                .get(RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| retry_delay(value, self.date_anchor, self.clock.monotonic_now()));
            let oversized = || SignInFailure::Invalid {
                reason: crate::InvalidReason::OversizedAnswer,
            };
            if answer
                .headers()
                .get(CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok())
                .is_some_and(|length| length > RESPONSE_BODY_LIMIT as u64)
            {
                return Err(oversized());
            }
            let mut body = Zeroizing::new(Vec::with_capacity(RESPONSE_BODY_LIMIT));
            while let Some(chunk) = answer.chunk().await.map_err(|_| unavailable())? {
                if chunk.len() > RESPONSE_BODY_LIMIT - body.len() {
                    return Err(oversized());
                }
                body.extend_from_slice(&chunk);
            }
            Ok(HttpAnswer {
                status,
                retry_after,
                body,
            })
        })
        .await
        .map_err(|_| unavailable())?
    }
}

impl RefreshSource for HttpRefreshSource {
    async fn refresh(&self, credential: &RefreshCredential) -> RefreshAnswer {
        let form = RefreshForm {
            grant_type: "refresh_token",
            client_id: credential.enrollment_id.to_string(),
            refresh_token: credential.refresh_token.expose_secret(),
        };
        match self
            .0
            .request(self.0.http.post(self.0.endpoint(TOKEN_PATH)).form(&form))
            .await
        {
            Ok(answer) => decode_refresh(answer.status.as_u16(), answer.retry_after, &answer.body),
            Err(SignInFailure::Unavailable { retry_after }) => {
                RefreshAnswer::Unavailable { retry_after }
            }
            Err(SignInFailure::Invalid {
                reason: crate::InvalidReason::OversizedAnswer,
            }) => RefreshAnswer::OversizedReply,
            Err(SignInFailure::Invalid { reason }) => RefreshAnswer::Refused { reason },
        }
    }
}

fn malformed_answer() -> SignInFailure {
    SignInFailure::Invalid {
        reason: crate::InvalidReason::MalformedAnswer,
    }
}

fn retry_delay(
    value: &str,
    anchor: (SystemTime, ClockInstant),
    now: ClockInstant,
) -> Option<Duration> {
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    let target = httpdate::parse_http_date(value).ok()?;
    let (utc, elapsed) = anchor;
    Some(
        target
            .duration_since(utc)
            .unwrap_or_default()
            .saturating_sub(now.saturating_duration_since(elapsed)),
    )
}
