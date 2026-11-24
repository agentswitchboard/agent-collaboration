//! Worker sign-in trust configured explicitly for org-scoped central runtimes.

use crate::sign_in_crypto::{WorkerEd25519, WorkerEs256};
use collaboration_central_core::{
    SignInTrustSettings,
    organization_authority::{SignInVerification, SignInVerifiers},
    person_request::{PersonRequestPolicy, PersonRequestVerifier},
};
use fleet_identity_protocol::OrganizationId;
use fleet_runtime_ports::Clock;
use fleet_runtime_ports::{IssuerKeySource, KeyFetchFailure, RawJwks};
use fleet_signin::{
    IssuerKeyCache, IssuerSignatureVerifier,
    machine_token::{MachineTokenPolicy, MachineTokenVerifier},
    organization_authority::{
        FleetOrganizationStatementTrust, FleetOrganizationStatementVerifier, InvalidStampTrust,
    },
};
use std::{future::Future, sync::Arc};
use worker::{
    Env, Fetcher, Method, RequestInit, RequestRedirect,
    send::{SendFuture, SendWrapper},
};

const SIGN_IN_KEYS_BINDING: &str = "SIGN_IN_KEYS";
// Only the service binding routes the request. This URL supplies its fixed request path.
const SIGN_IN_KEYS_REQUEST: &str = "https://sign-in-keys.invalid/api/auth/jwks";

struct WorkerSignInTrustSettings {
    issuer: String,
    audience: String,
    admin_audience: String,
    person_link_audience: Option<String>,
    machine_audience: String,
    public_origin: String,
    service_id: Option<message_board::ServiceId>,
}

/// Retained by the host so fresh verifiers share the issuer's bounded key cache.
pub(crate) struct CloudflareSignInTrust {
    settings: WorkerSignInTrustSettings,
    keys: WorkerSignInKeys,
    cache: Arc<IssuerKeyCache>,
}

impl CloudflareSignInTrust {
    /// A deployment with no sign-in configuration keeps ordinary central operation.
    /// Once any field is supplied, all required settings must be present and valid.
    pub(crate) fn from_env(env: &Env) -> worker::Result<Option<Self>> {
        let variable = |name: &str| {
            let present = js_sys::Reflect::has(env, &worker::wasm_bindgen::JsValue::from_str(name))
                .map_err(|_| invalid_configuration())?;
            if !present {
                return Ok(None);
            }
            env.var(name)
                .map(|value| Some(value.to_string()))
                .map_err(|_| invalid_configuration())
        };
        let issuer = variable("CENTRAL_SIGN_IN_ISSUER")?;
        let audience = variable("CENTRAL_SIGN_IN_AUDIENCE")?;
        let admin_audience = variable("CENTRAL_ADMIN_AUDIENCE")?;
        let person_link_audience = variable("CENTRAL_PERSON_LINK_AUDIENCE")?;
        let machine_audience = variable("CENTRAL_MACHINE_AUDIENCE")?;
        let public_origin = variable("CENTRAL_PUBLIC_ORIGIN")?;
        let service_id = variable("CENTRAL_SIGN_IN_SERVICE_ID")?;
        if [
            &issuer,
            &audience,
            &admin_audience,
            &person_link_audience,
            &machine_audience,
            &public_origin,
            &service_id,
        ]
        .iter()
        .all(|value| value.is_none())
        {
            return Ok(None);
        }
        let required = |value: Option<String>| {
            value
                .filter(|value| !value.is_empty() && !value.contains('\0'))
                .ok_or_else(invalid_configuration)
        };
        let settings = WorkerSignInTrustSettings {
            issuer: required(issuer)?,
            audience: required(audience)?,
            admin_audience: required(admin_audience)?,
            person_link_audience: person_link_audience
                .map(|value| required(Some(value)))
                .transpose()?,
            machine_audience: required(machine_audience)?,
            public_origin: required(public_origin)?,
            service_id: service_id
                .map(message_board::ServiceId::try_from)
                .transpose()
                .map_err(|_| invalid_configuration())?,
        };
        if !SignInTrustSettings::is_valid_machine_audience(&settings.machine_audience) {
            return Err(invalid_configuration());
        }
        // Central’s configured origin is used for DPoP, independently of the keys source.
        let origin =
            worker::Url::parse(&settings.public_origin).map_err(|_| invalid_configuration())?;
        if origin.scheme() != "https"
            || origin.host_str().is_none()
            || !origin.username().is_empty()
            || origin.password().is_some()
            || origin.query().is_some()
            || origin.fragment().is_some()
            || origin.path() != "/"
        {
            return Err(invalid_configuration());
        }
        let binding = env
            .service(SIGN_IN_KEYS_BINDING)
            .map_err(|_| invalid_configuration())?;
        Ok(Some(Self {
            settings,
            keys: WorkerSignInKeys(SendWrapper::new(binding)),
            cache: Arc::default(),
        }))
    }

    /// Bind trust to this org and the same clock its runtime uses for admission.
    pub(crate) fn for_organization(
        &self,
        organization: OrganizationId,
        clock: Arc<dyn Clock>,
    ) -> Result<Arc<dyn SignInVerification>, InvalidStampTrust> {
        let trust = FleetOrganizationStatementTrust::new(
            self.settings.issuer.clone(),
            self.settings.audience.clone(),
            organization,
        )?;
        let statement_verifier = FleetOrganizationStatementVerifier::new(
            WorkerEd25519,
            self.keys.clone(),
            self.cache.clone(),
            clock.clone(),
            trust,
        );
        let person_request_verifier = PersonRequestVerifier::new(
            IssuerSignatureVerifier::new(
                WorkerEd25519,
                self.keys.clone(),
                self.cache.clone(),
                clock.clone(),
            ),
            WorkerEs256,
            clock.clone(),
            PersonRequestPolicy {
                issuer: self.settings.issuer.clone(),
                audience: self.settings.admin_audience.clone(),
                public_origin: self.settings.public_origin.clone(),
            },
        );
        let machine_token_verifier = MachineTokenVerifier::new(
            IssuerSignatureVerifier::new(
                WorkerEd25519,
                self.keys.clone(),
                self.cache.clone(),
                clock.clone(),
            ),
            clock,
            MachineTokenPolicy {
                issuer: self.settings.issuer.clone(),
                audience: self.settings.machine_audience.clone(),
            },
        );
        Ok(Arc::new(SignInVerifiers::from_host_verifiers(
            statement_verifier,
            person_request_verifier,
            machine_token_verifier,
            self.settings.service_id.clone(),
            &self.cache,
            self.settings.person_link_audience.clone(),
        )))
    }
}

fn invalid_configuration() -> worker::Error {
    worker::Error::RustError("invalid sign-in trust configuration".into())
}

#[derive(Clone)]
struct WorkerSignInKeys(SendWrapper<Fetcher>);

impl IssuerKeySource for WorkerSignInKeys {
    fn configured_source(&self) -> &str {
        SIGN_IN_KEYS_BINDING
    }

    fn fetch_issuer_keys(&self) -> impl Future<Output = Result<RawJwks, KeyFetchFailure>> + Send {
        SendFuture::new(async {
            let mut init = RequestInit::new();
            init.with_method(Method::Get)
                .with_redirect(RequestRedirect::Manual);
            let response = self
                .0
                .fetch(SIGN_IN_KEYS_REQUEST, Some(init))
                .await
                .map_err(|_| KeyFetchFailure::Unreachable)?;
            let status = response.status().as_u16();
            if !(200..300).contains(&status) {
                return Err(KeyFetchFailure::BadResponse { status });
            }
            let cache_control = response
                .headers()
                .get(http::header::CACHE_CONTROL)
                .map(|value| std::str::from_utf8(value.as_bytes()).map(str::to_owned))
                .transpose()
                .map_err(|_| KeyFetchFailure::Unreachable)?;
            let body =
                axum::body::to_bytes(axum::body::Body::new(response.into_body()), usize::MAX)
                    .await
                    .map_err(|_| KeyFetchFailure::Unreachable)?
                    .to_vec();
            Ok(RawJwks {
                body,
                cache_control,
            })
        })
    }
}
