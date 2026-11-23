//! The authenticated upgrade to the configured Central address.

use std::{
    future::poll_fn,
    io::{self, IoSlice},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, ready},
};

use central_link_protocol::MachineTokenText;
use futures_util::future::try_join;
use http_body_util::Empty;
use hyper::{Request, StatusCode, Uri, body::Bytes, client::conn::http1, upgrade::Upgraded};
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{
        handshake::{client::generate_key, derive_accept_key},
        http::{
            HeaderMap, HeaderValue,
            header::{
                AUTHORIZATION, CONNECTION, HOST, SEC_WEBSOCKET_ACCEPT, SEC_WEBSOCKET_EXTENSIONS,
                SEC_WEBSOCKET_KEY, SEC_WEBSOCKET_PROTOCOL, SEC_WEBSOCKET_VERSION, UPGRADE,
            },
            uri::Scheme,
        },
        protocol::{Role, WebSocketConfig},
    },
};
use tower_service::Service;

use crate::link_policy::DialFailure;

pub(super) type Socket = WebSocketStream<TokioIo<Upgraded>>;

const UPGRADE_RESPONSE_MAX_HEADERS: usize = 124;
const UPGRADE_RESPONSE_MAX_BYTES: usize = 64 * 1024;
const UPGRADE_RESPONSE_MAX_READS: usize = 512;
const SOCKET_BYTE_LIMIT: usize = 1024 * 1024;
// The longest client frame header uses a two-byte prefix, eight-byte length and four-byte mask.
pub(super) const MAX_OUTBOUND_TEXT_BYTES: usize = SOCKET_BYTE_LIMIT - 14;
// Apply the same 64 KiB envelope to the outbound bearer header, before formatting its value.
pub(super) const MAX_CREDENTIAL_BYTES: usize =
    UPGRADE_RESPONSE_MAX_BYTES - "Authorization: Bearer \r\n".len();

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
        if token.expose().len() > MAX_CREDENTIAL_BYTES {
            return Err(DialFailure::Unauthorized);
        }
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
        let request_target = address
            .path_and_query()
            .ok_or(DialFailure::InvalidAddress)?;
        let authority = address.authority().ok_or(DialFailure::InvalidAddress)?;
        let request = Request::get(request_target.as_str())
            .header(HOST, authority.as_str())
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
        let mut connector = connector.enable_http1().build();
        poll_fn(|context| connector.poll_ready(context))
            .await
            .map_err(|_| DialFailure::Transport)?;
        let stream = connector
            .call(address)
            .await
            .map_err(|_| DialFailure::Transport)?;
        upgrade_over_stream(TokioIo::new(stream), request, &websocket_key).await
    }
}

async fn upgrade_over_stream(
    stream: impl AsyncRead + AsyncWrite + Unpin + Send + 'static,
    request: Request<Empty<Bytes>>,
    websocket_key: &str,
) -> Result<Socket, DialFailure> {
    let counting_upgrade = Arc::new(AtomicBool::new(true));
    let budgeted = UpgradeReadStream {
        stream,
        counting_upgrade: counting_upgrade.clone(),
        bytes_read: 0,
        completed_reads: 0,
    };
    let (mut sender, connection) = http1::Builder::new()
        .max_headers(UPGRADE_RESPONSE_MAX_HEADERS)
        .max_buf_size(UPGRADE_RESPONSE_MAX_BYTES)
        .handshake(TokioIo::new(budgeted))
        .await
        .map_err(|_| DialFailure::Transport)?;
    let exchange = async {
        let mut response = sender
            .send_request(request)
            .await
            .map_err(|_| DialFailure::Transport)?;
        match response.status() {
            StatusCode::SWITCHING_PROTOCOLS => {
                check_upgrade_headers(response.headers(), websocket_key)?
            }
            StatusCode::UNAUTHORIZED => return Err(DialFailure::Unauthorized),
            StatusCode::SERVICE_UNAVAILABLE => return Err(DialFailure::Busy),
            StatusCode::BAD_REQUEST => return Err(DialFailure::InvalidAddress),
            _ => return Err(DialFailure::Transport),
        }
        hyper::upgrade::on(&mut response)
            .await
            .map_err(|_| DialFailure::Transport)
    };
    let connection = async {
        connection
            .with_upgrades()
            .await
            .map_err(|_| DialFailure::Transport)
    };
    let (upgraded, ()) = try_join(exchange, connection).await?;
    counting_upgrade.store(false, Ordering::Relaxed);
    // Hyper replays any bytes read alongside the 101 before reading the stream again.
    Ok(WebSocketStream::from_raw_socket(
        TokioIo::new(upgraded),
        Role::Client,
        Some(socket_config()),
    )
    .await)
}

struct UpgradeReadStream<Stream> {
    stream: Stream,
    counting_upgrade: Arc<AtomicBool>,
    bytes_read: usize,
    completed_reads: usize,
}

impl<Stream: AsyncRead + Unpin> AsyncRead for UpgradeReadStream<Stream> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if !self.counting_upgrade.load(Ordering::Relaxed) {
            return Pin::new(&mut self.stream).poll_read(context, buffer);
        }
        if buffer.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let remaining_budget = UPGRADE_RESPONSE_MAX_BYTES - self.bytes_read;
        if remaining_budget == 0 || self.completed_reads == UPGRADE_RESPONSE_MAX_READS {
            return Poll::Ready(Err(io::Error::other("HTTP upgrade read budget exhausted")));
        }
        // Keep any bytes beyond the budget in the stream. If a valid 101 ends at
        // the limit, the next read belongs to WebSocket and uses the original stream.
        let allowed_bytes = remaining_budget.min(buffer.remaining());
        let mut bounded = ReadBuf::new(buffer.initialize_unfilled_to(allowed_bytes));
        ready!(Pin::new(&mut self.stream).poll_read(context, &mut bounded))?;
        let read_size = bounded.filled().len();
        buffer.advance(read_size);
        if read_size > 0 {
            self.bytes_read += read_size;
            self.completed_reads += 1;
        }
        Poll::Ready(Ok(()))
    }
}

impl<Stream: AsyncWrite + Unpin> AsyncWrite for UpgradeReadStream<Stream> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(context, buffer)
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(context)
    }

    fn is_write_vectored(&self) -> bool {
        self.stream.is_write_vectored()
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffers: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write_vectored(context, buffers)
    }
}

fn socket_config() -> WebSocketConfig {
    WebSocketConfig::default()
        .max_frame_size(Some(SOCKET_BYTE_LIMIT))
        .max_message_size(Some(SOCKET_BYTE_LIMIT))
        .max_write_buffer_size(SOCKET_BYTE_LIMIT)
}

#[cfg(test)]
#[path = "socket_upgrade_test.rs"]
mod tests;

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
