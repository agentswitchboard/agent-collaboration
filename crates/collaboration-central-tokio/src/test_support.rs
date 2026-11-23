//! Local Tokio machine-link integration fixtures.
use crate::{OrganizationSignInLink, TokioWorkspaceHost, WorkspaceLink};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use collaboration_central_conformance::{ConformanceResult as Result, signin_fixtures as signed};
use collaboration_central_core::SignInTrustSettings;
use fleet_contracts::ProvisionOrganization;
use fleet_contracts::{DeviceLoginDecision, MachineTokenDecision};
use fleet_identity_protocol::{EnrollmentId, OrganizationId};
use reqwest_websocket::{RequestBuilderExt, WebSocket};
use router_clock::{ManualClock, RuntimeClock};
use serde_json::json;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// The enrolled machine on the fixture organization link.
pub const MACHINE_LINK_FIXTURE_MACHINE: &str = "019f0000-0000-7000-8000-00000000ce01";

/// Owns the real loopback link, its stores, and fixture sign-in authority.
pub struct MachineLinkFixture {
    pub host: Arc<TokioWorkspaceHost>,
    pub clock: Arc<ManualClock>,
    pub token: String,
    pub enrollment_id: EnrollmentId,
    pub organization_id: OrganizationId,
    pub signin_socket: std::path::PathBuf,
    signin_link: OrganizationSignInLink,
    address: std::net::SocketAddr,
    link: WorkspaceLink,
    stop: CancellationToken,
    serving: JoinHandle<std::io::Result<()>>,
    keys: signed::SignInKeys,
    _root: tempfile::TempDir,
}

/// Start a real Tokio Central using the caller's clock and isolated fixture sign-in.
pub async fn start_machine_link_fixture(clock: Arc<ManualClock>) -> Result<MachineLinkFixture> {
    let root = tempfile::Builder::new()
        .prefix("workspace-link-")
        .tempdir()?;
    let now_seconds = clock.utc_now().timestamp();
    let keys = signed::SignInKeys::start().await?;
    let host = Arc::new(
        TokioWorkspaceHost::open_with_signin(
            &root.path().join("modern"),
            clock.clone(),
            SignInTrustSettings {
                issuer: signed::ISSUER.into(),
                audience: signed::AUDIENCE.into(),
                admin_audience: signed::ADMIN_AUDIENCE.into(),
                machine_audience: signed::MACHINE_AUDIENCE.into(),
                public_origin: signed::PUBLIC_ORIGIN.into(),
                jwks_url: keys.url.clone(),
                service_id: Some(
                    "019f0000-0000-7000-8000-00000000ce05"
                        .to_owned()
                        .try_into()?,
                ),
            },
        )
        .await?,
    );
    let organization: OrganizationId = signed::ORGANIZATION.to_owned().try_into()?;
    let runtime = host.central(&organization).await?;
    runtime
        .provision_organization(&ProvisionOrganization {
            organization_id: organization.clone(),
            organization_statement: signed::stamp(signed::ORGANIZATION, now_seconds)?.try_into()?,
        })
        .await?;
    let approval = serde_json::from_value(json!({
        "organizationId": signed::ORGANIZATION, "approver": signed::OWNER,
        "permissionChecked":"machine:enroll", "requestId":"019f0000-0000-7000-8000-00000000ce04",
        "machineId":MACHINE_LINK_FIXTURE_MACHINE, "label":"Link test machine",
        "allowedWorkspaces":[], "approverSecondFactorAt":now_seconds-60,
        "codeExpiresAt":now_seconds+600,
    }))?;
    let DeviceLoginDecision::Approved { enrollment_id, .. } =
        runtime.decide_device_login(&approval).await?
    else {
        return Err("real machine approval refused".into());
    };
    let token =
        mint_enrolled_machine_token(&host, &organization, &enrollment_id, now_seconds).await?;
    let signin_socket = root.path().join("signin.sock");
    let listener =
        tokio::net::TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0))).await?;
    let address = listener.local_addr()?;
    let signin_link = OrganizationSignInLink::start(host.clone(), &signin_socket)?;
    let link = WorkspaceLink::new(host.clone());
    let router = link.router();
    let stop = CancellationToken::new();
    let stopped = stop.clone();
    let serving = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(stopped.cancelled_owned())
            .await
    });
    Ok(MachineLinkFixture {
        host,
        clock,
        token,
        enrollment_id,
        organization_id: organization,
        signin_socket,
        signin_link,
        address,
        link,
        stop,
        serving,
        keys,
        _root: root,
    })
}

impl MachineLinkFixture {
    pub fn keys(&self) -> &signed::SignInKeys {
        &self.keys
    }

    pub fn root_path(&self) -> &std::path::Path {
        self._root.path()
    }

    /// HTTP origin of the loopback listener.
    pub fn origin(&self) -> String {
        format!("http://{}", self.address)
    }

    pub fn url(&self, organization: &str) -> String {
        format!("{}/v1/organizations/{organization}/link", self.origin())
    }

    /// Ask the live organization for a grant and sign a fresh fixture bearer.
    pub async fn mint_machine_token(&self) -> Result<String> {
        mint_enrolled_machine_token(
            &self.host,
            &self.organization_id,
            &self.enrollment_id,
            self.clock.utc_now().timestamp(),
        )
        .await
    }

    pub fn client(&self) -> Result<reqwest::Client> {
        Ok(reqwest::Client::builder().no_proxy().build()?)
    }

    pub fn token_for_installation(&self, machine_id: &str) -> Result<String> {
        let encoded = self.token.split('.').nth(1).ok_or("fixture token claims")?;
        let mut claims: serde_json::Value =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(encoded)?)?;
        signed::set_claim(
            claims
                .get_mut("principal")
                .ok_or("fixture machine principal")?,
            "machine_id",
            json!(machine_id),
        )?;
        signed::sign(
            &json!({"alg":"EdDSA","typ":"at+jwt","kid":"stamp-a"}),
            &claims,
            7,
        )
    }

    pub async fn connect(&self) -> Result<WebSocket> {
        Ok(self
            .client()?
            .get(self.url(signed::ORGANIZATION))
            .bearer_auth(&self.token)
            .upgrade()
            .send()
            .await?
            .into_websocket()
            .await?)
    }

    pub async fn close(self) -> Result<()> {
        self.link.begin_shutdown();
        self.stop.cancel();
        let serving = self.serving.await;
        self.link.shutdown().await;
        let signin = self.signin_link.shutdown().await;
        self.host.close().await;
        let keys = self.keys.shutdown().await;
        serving??;
        signin?;
        keys
    }
}

async fn mint_enrolled_machine_token(
    host: &TokioWorkspaceHost,
    organization_id: &OrganizationId,
    enrollment_id: &EnrollmentId,
    now_seconds: i64,
) -> Result<String> {
    let runtime = host.central(organization_id).await?;
    let grant = runtime.decide_machine_token(&serde_json::from_value(json!({
        "organizationId": organization_id, "enrollmentId": enrollment_id, "clientId": enrollment_id,
    }))?).await?;
    let MachineTokenDecision::Grant {
        grant_expires_at, ..
    } = grant
    else {
        return Err("real machine token grant refused".into());
    };
    signed::machine_token(
        organization_id.as_str(),
        MACHINE_LINK_FIXTURE_MACHINE,
        enrollment_id.as_str(),
        now_seconds,
        grant_expires_at.0,
    )
}
