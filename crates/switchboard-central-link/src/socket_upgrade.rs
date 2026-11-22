//! The authenticated upgrade to the configured Central address.

use central_link_protocol::MachineTokenText;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{
        Error,
        client::IntoClientRequest,
        http::{
            StatusCode,
            header::{AUTHORIZATION, HeaderValue},
        },
    },
};

use crate::link_policy::DialFailure;

pub(super) type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Invalid address supplied by the trusted link configuration owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AddressError {
    InvalidLocation,
}

/// A WebSocket URL accepted by the link driver without credentials or redirects.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DialAddress(String);

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
        let request = address
            .into_client_request()
            .map_err(|_| AddressError::InvalidLocation)?;
        let uri = request.uri();
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
        Ok(Self(address.to_owned()))
    }

    pub(super) async fn dial(&self, token: &MachineTokenText) -> Result<Socket, DialFailure> {
        let mut request = self
            .0
            .as_str()
            .into_client_request()
            .map_err(|_| DialFailure::InvalidAddress)?;
        let auth = format!("Bearer {}", token.expose());
        let mut value = HeaderValue::from_str(&auth).map_err(|_| DialFailure::Unauthorized)?;
        value.set_sensitive(true);
        request.headers_mut().insert(AUTHORIZATION, value);
        match connect_async(request).await {
            Ok((socket, response)) if response.status() == StatusCode::SWITCHING_PROTOCOLS => {
                Ok(socket)
            }
            Err(Error::Http(response)) => Err(match response.status() {
                StatusCode::UNAUTHORIZED => DialFailure::Unauthorized,
                StatusCode::SERVICE_UNAVAILABLE => DialFailure::Busy,
                StatusCode::BAD_REQUEST => DialFailure::InvalidAddress,
                _ => DialFailure::Transport,
            }),
            Ok(_) | Err(_) => Err(DialFailure::Transport),
        }
    }
}
