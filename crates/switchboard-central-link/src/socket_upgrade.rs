//! The authenticated upgrade to the configured Central address.

use central_link_protocol::MachineTokenText;
use http_body_util::Empty;
use hyper::{Request, StatusCode, Uri, body::Bytes, upgrade::Upgraded};
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::{
    client::legacy::Client,
    rt::{TokioExecutor, TokioIo},
};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{
        handshake::{client::generate_key, derive_accept_key},
        http::{
            HeaderMap, HeaderValue,
            header::{
                AUTHORIZATION, CONNECTION, SEC_WEBSOCKET_ACCEPT, SEC_WEBSOCKET_EXTENSIONS,
                SEC_WEBSOCKET_KEY, SEC_WEBSOCKET_PROTOCOL, SEC_WEBSOCKET_VERSION, UPGRADE,
            },
            uri::Scheme,
        },
        protocol::Role,
    },
};

use crate::link_policy::DialFailure;

pub(super) type Socket = WebSocketStream<TokioIo<Upgraded>>;

const UPGRADE_RESPONSE_MAX_HEADERS: usize = 124;
const UPGRADE_RESPONSE_MAX_BYTES: usize = 64 * 1024;

/// Invalid address supplied by the trusted link configuration owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AddressError {
    InvalidLocation,
}

/// A credential-free WebSocket address built by the link configuration owner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DialAddress(Uri);

impl DialAddress {
    /// Validate an operator-supplied secure WebSocket dial address.
    pub fn secure(address: &str) -> Result<Self, AddressError> {
        Self::validate(address, |scheme, _host| scheme == "wss")
    }

    /// Allow exactly the literal loopback hosts for the scripted and Tokio fixtures.
    #[cfg(feature = "test-support")]
    pub fn test_loopback(address: &str) -> Result<Self, AddressError> {
        Self::validate(address, |scheme, host| {
            scheme == "ws" && matches!(host, "127.0.0.1" | "[::1]")
        })
    }

    fn validate(
        address: &str,
        permitted: impl FnOnce(&str, &str) -> bool,
    ) -> Result<Self, AddressError> {
        let uri: Uri = address.parse().map_err(|_| AddressError::InvalidLocation)?;
        let (Some(scheme), Some(authority)) = (uri.scheme_str(), uri.authority()) else {
            return Err(AddressError::InvalidLocation);
        };
        if !permitted(scheme, authority.host())
            || authority.as_str().contains('@')
            || uri.query().is_some()
            || address.contains('#')
            || uri.path() == "/"
        {
            return Err(AddressError::InvalidLocation);
        }
        Ok(Self(uri))
    }

    pub(super) async fn dial(&self, token: &MachineTokenText) -> Result<Socket, DialFailure> {
        let mut address_parts = self.0.clone().into_parts();
        address_parts.scheme = Some(match self.0.scheme_str() {
            #[cfg(feature = "test-support")]
            Some("ws") => Scheme::HTTP,
            Some("wss") => Scheme::HTTPS,
            _ => return Err(DialFailure::InvalidAddress),
        });
        let address = Uri::from_parts(address_parts).map_err(|_| DialFailure::InvalidAddress)?;
        let websocket_key = generate_key();
        let auth = format!("Bearer {}", token.expose());
        let mut value = HeaderValue::from_str(&auth).map_err(|_| DialFailure::Unauthorized)?;
        value.set_sensitive(true);
        let request = Request::get(address)
            .header(AUTHORIZATION, value)
            .header(CONNECTION, "Upgrade")
            .header(UPGRADE, "websocket")
            .header(SEC_WEBSOCKET_KEY, websocket_key.as_str())
            .header(SEC_WEBSOCKET_VERSION, "13")
            .body(Empty::<Bytes>::new())
            .map_err(|_| DialFailure::InvalidAddress)?;
        let connector = HttpsConnectorBuilder::new().with_webpki_roots();
        #[cfg(feature = "test-support")]
        let connector = connector.https_or_http();
        #[cfg(not(feature = "test-support"))]
        let connector = connector.https_only();
        let client = Client::builder(TokioExecutor::new())
            .pool_max_idle_per_host(0)
            .http1_max_headers(UPGRADE_RESPONSE_MAX_HEADERS)
            .http1_max_buf_size(UPGRADE_RESPONSE_MAX_BYTES)
            .build(connector.enable_http1().build());

        let mut response = client
            .request(request)
            .await
            .map_err(|_| DialFailure::Transport)?;
        match response.status() {
            StatusCode::SWITCHING_PROTOCOLS => {
                check_upgrade_headers(response.headers(), &websocket_key)?
            }
            StatusCode::UNAUTHORIZED => return Err(DialFailure::Unauthorized),
            StatusCode::SERVICE_UNAVAILABLE => return Err(DialFailure::Busy),
            StatusCode::BAD_REQUEST => return Err(DialFailure::InvalidAddress),
            _ => return Err(DialFailure::Transport),
        };
        // Keep Hyper's upgraded wrapper so bytes read alongside the 101 reach the socket.
        let upgraded = hyper::upgrade::on(&mut response)
            .await
            .map_err(|_| DialFailure::Transport)?;
        Ok(WebSocketStream::from_raw_socket(TokioIo::new(upgraded), Role::Client, None).await)
    }
}

fn check_upgrade_headers(headers: &HeaderMap, websocket_key: &str) -> Result<(), DialFailure> {
    let upgrades_connection = headers.get_all(CONNECTION).iter().any(|value| {
        value.to_str().is_ok_and(|value| {
            value
                .split(',')
                .any(|token| token.trim().eq_ignore_ascii_case("upgrade"))
        })
    });
    let upgrades_websocket = headers
        .get(UPGRADE)
        .is_some_and(|value| value.as_bytes().eq_ignore_ascii_case(b"websocket"));
    let accepts_key = headers
        .get(SEC_WEBSOCKET_ACCEPT)
        .is_some_and(|value| value == derive_accept_key(websocket_key.as_bytes()).as_str());
    if upgrades_connection
        && upgrades_websocket
        && accepts_key
        && !headers.contains_key(SEC_WEBSOCKET_PROTOCOL)
        && !headers.contains_key(SEC_WEBSOCKET_EXTENSIONS)
    {
        Ok(())
    } else {
        Err(DialFailure::Transport)
    }
}
