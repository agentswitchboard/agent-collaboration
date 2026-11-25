use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::Path,
};

use fleet_identity_protocol::OrganizationId;
use serde::Deserialize;
use url::{Host, Url};

use crate::FleetTargetError;

/// The fields the workspace command uses from the Node owner CLI's existing target.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FleetTargetDocument {
    issuer: String,
    organization_id: OrganizationId,
    central_admin_audience: Option<String>,
    owner_fleet_cli_client_id: Option<String>,
    owner_cli_redirect_uri: Option<String>,
}

pub struct FleetTarget {
    issuer_name: String,
    issuer_url: Url,
    organization: OrganizationId,
    admin_audience: String,
    client_id: String,
    redirect_uri: Option<String>,
}

impl FleetTarget {
    pub fn read(path: &Path) -> Result<Self, FleetTargetError> {
        let bytes = std::fs::read(path).map_err(|_| FleetTargetError::Read)?;
        Self::from_json(&bytes)
    }

    pub fn from_json(bytes: &[u8]) -> Result<Self, FleetTargetError> {
        let document: FleetTargetDocument =
            serde_json::from_slice(bytes).map_err(|_| FleetTargetError::InvalidDocument)?;
        let issuer_url =
            Url::parse(&document.issuer).map_err(|_| FleetTargetError::InvalidIssuer)?;
        if issuer_url.scheme() != "https"
            || issuer_url.host().is_none()
            || !issuer_url.username().is_empty()
            || issuer_url.password().is_some()
            || issuer_url.query().is_some()
            || issuer_url.fragment().is_some()
        {
            return Err(FleetTargetError::InvalidIssuer);
        }
        let admin_audience = document
            .central_admin_audience
            .filter(|audience| Url::parse(audience).is_ok())
            .ok_or(FleetTargetError::MissingFleetSettings)?;
        let client_id = document
            .owner_fleet_cli_client_id
            .filter(|client_id| !client_id.trim().is_empty())
            .ok_or(FleetTargetError::MissingFleetSettings)?;
        Ok(Self {
            issuer_name: document.issuer,
            issuer_url,
            organization: document.organization_id,
            admin_audience,
            client_id,
            redirect_uri: document.owner_cli_redirect_uri,
        })
    }

    pub(crate) fn issuer(&self) -> &str {
        &self.issuer_name
    }

    pub(crate) fn organization(&self) -> &OrganizationId {
        &self.organization
    }

    pub(crate) fn admin_audience(&self) -> &str {
        &self.admin_audience
    }

    pub(crate) fn client_id(&self) -> &str {
        &self.client_id
    }

    pub(crate) fn token_url(&self) -> Url {
        self.issuer_route("/api/auth/oauth2/token")
    }

    pub(crate) fn person_link_url(&self) -> Url {
        self.issuer_route(&format!(
            "/api/fleet/person-link/{}",
            self.organization.as_str()
        ))
    }

    pub(crate) fn issuer_route(&self, path: &str) -> Url {
        let mut url = self.issuer_url.clone();
        url.set_path(path);
        url
    }

    pub(crate) fn loopback_redirect(&self) -> Result<(Url, SocketAddr), FleetTargetError> {
        let invalid = FleetTargetError::InvalidRedirect;
        let redirect = self
            .redirect_uri
            .as_deref()
            .and_then(|redirect| Url::parse(redirect).ok())
            .ok_or(invalid)?;
        let address = match redirect.host() {
            Some(Host::Domain("localhost")) => IpAddr::V4(Ipv4Addr::LOCALHOST),
            Some(Host::Ipv4(Ipv4Addr::LOCALHOST)) => IpAddr::V4(Ipv4Addr::LOCALHOST),
            Some(Host::Ipv6(address)) if address.is_loopback() => IpAddr::V6(address),
            _ => return Err(invalid),
        };
        if redirect.scheme() != "http"
            || !redirect.username().is_empty()
            || redirect.password().is_some()
            || redirect.fragment().is_some()
        {
            return Err(invalid);
        }
        let socket = SocketAddr::new(address, redirect.port().unwrap_or(0));
        Ok((redirect, socket))
    }
}
