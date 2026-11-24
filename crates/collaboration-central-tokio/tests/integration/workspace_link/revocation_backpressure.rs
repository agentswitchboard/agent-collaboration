//! Force the actual Axum socket's write to return Pending without a peer read.
use super::fixture::{Fixture, receive, send, start_fixture};
use super::idle_expiry::{
    assert_idle_commit, connect_replacement, require_inactive_close, seed_idle_grant,
};
use super::link_support::{caller, create_workspace};
use axum::serve::Listener;
use central_link_protocol::{
    CorrelationId, DataFrame, LinkFrame, LinkRequest, MachineLinkCloseCode, TokenRenewalRefusal,
    TokenRenewalResult,
};
use collaboration_central_conformance::fixture_link_reader::read_fixture_frame;
use collaboration_central_conformance::{ConformanceResult as Result, signin_fixtures as signed};
use collaboration_central_tokio::WorkspaceLink;
use fleet_runtime::PausePoint;
use futures_util::{FutureExt, StreamExt};
use reqwest_websocket::{RequestBuilderExt, WebSocket};
use router_clock::RuntimeClock;
use serde_json::json;
use std::{
    io,
    net::{Ipv4Addr, SocketAddr},
    pin::Pin,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
    sync::Notify,
};
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn signed_revocation_drops_the_transport_with_a_reply_blocked_in_io() -> Result<()> {
    run_revocation(WriteSettlement::Unwritable).await
}

#[tokio::test]
async fn signed_revocation_reports_its_typed_close_when_a_pending_reply_can_flush() -> Result<()> {
    run_revocation(WriteSettlement::Writable).await
}

async fn run_revocation(settlement: WriteSettlement) -> Result<()> {
    let fixture = start_fixture().await?;
    let gated = GatedLink::open(&fixture).await?;
    let result = revoke_while_reply_is_blocked(&fixture, &gated, settlement).await;
    gated.close().await?;
    fixture.close().await?;
    result
}

#[tokio::test]
async fn idle_renewal_writes_the_refusal_then_typed_close_when_io_becomes_writable() -> Result<()> {
    run_idle_renewal(WriteSettlement::Writable).await
}

#[tokio::test]
async fn idle_renewal_cleans_up_and_drops_the_transport_after_the_io_budget() -> Result<()> {
    run_idle_renewal(WriteSettlement::Unwritable).await
}

async fn run_idle_renewal(settlement: WriteSettlement) -> Result<()> {
    let fixture = start_fixture().await?;
    let gated = GatedLink::open(&fixture).await?;
    let result = settle_idle_renewal(&fixture, &gated, settlement).await;
    gated.close().await?;
    fixture.close().await?;
    result
}

enum WriteSettlement {
    Writable,
    Unwritable,
}

struct GatedLink {
    link: WorkspaceLink,
    gate: Arc<WriteGate>,
    address: SocketAddr,
    stop: CancellationToken,
    task: tokio::task::JoinHandle<io::Result<()>>,
}

impl GatedLink {
    async fn open(fixture: &Fixture) -> Result<Self> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let address = listener.local_addr()?;
        let gate = Arc::new(WriteGate::default());
        let link = WorkspaceLink::new(fixture.host.clone());
        let router = link.router();
        let stop = CancellationToken::new();
        let listener_stop = stop.clone();
        let gated = GatedListener {
            listener,
            gate: gate.clone(),
        };
        let task = tokio::spawn(async move {
            axum::serve(gated, router)
                .with_graceful_shutdown(listener_stop.cancelled_owned())
                .await
        });
        Ok(Self {
            link,
            gate,
            address,
            stop,
            task,
        })
    }

    async fn connect(&self, fixture: &Fixture) -> Result<WebSocket> {
        let mut socket = fixture
            .client()?
            .get(format!(
                "http://{}/v1/organizations/{}/link",
                self.address,
                signed::ORGANIZATION
            ))
            .bearer_auth(&fixture.token)
            .upgrade()
            .send()
            .await?
            .into_websocket()
            .await?;
        require_welcome(&mut socket).await?;
        Ok(socket)
    }

    async fn close(self) -> Result<()> {
        self.link.begin_shutdown();
        self.stop.cancel();
        self.task.await??;
        self.link.shutdown().await;
        Ok(())
    }
}

async fn require_welcome(socket: &mut WebSocket) -> Result<()> {
    if matches!(
        receive(socket).await?,
        LinkFrame::Data(DataFrame::Welcome { .. })
    ) {
        Ok(())
    } else {
        Err("machine did not receive Welcome".into())
    }
}

async fn settle_idle_renewal(
    fixture: &Fixture,
    gated: &GatedLink,
    settlement: WriteSettlement,
) -> Result<()> {
    let mut socket = gated.connect(fixture).await?;
    seed_idle_grant(fixture, signed::MACHINE_IDLE_LIMIT_SECONDS).await?;
    let runtime = fixture
        .host
        .central(&signed::ORGANIZATION.to_owned().try_into()?)
        .await?;
    let pauses = runtime.runtime().pause_points();
    let mut committed = pauses.hold_central(PausePoint::AfterIdleExpiryCommit);
    let mut before_cleanup = pauses.hold_central(PausePoint::BeforeIdleEnrollmentCleanup);
    let mut after_cleanup = pauses.hold_central(PausePoint::AfterIdleEnrollmentCleanup);
    let correlation_id: CorrelationId = "019f0000-0000-7000-8000-00000000e303".parse()?;
    gated.gate.blocked.store(true, Ordering::SeqCst);
    send(
        &mut socket,
        &LinkFrame::Data(DataFrame::RenewMachineToken {
            correlation_id: correlation_id.clone(),
            token: fixture.token.clone().try_into()?,
        }),
    )
    .await?;
    tokio::select! {
        () = committed.until_held() => {},
        () = gated.gate.pending.notified() => return Err("renewal attempted IO before observing the durable idle commit".into()),
    }
    assert_idle_commit(fixture).await?;
    if socket.next().now_or_never().is_some()
        || gated.gate.pending.notified().now_or_never().is_some()
    {
        return Err("a renewal frame or write attempt escaped the held commit".into());
    }
    let deadline = fixture
        .clock
        .monotonic_now()
        .after(Duration::from_millis(250));
    committed.release();
    tokio::select! {
        () = gated.gate.pending.notified() => {},
        () = before_cleanup.until_held() => return Err("idle cleanup bypassed the terminal refusal IO attempt".into()),
    }
    tokio::select! {
        () = fixture.clock.not_due_check_now(deadline) => {},
        () = before_cleanup.until_held() => return Err("idle cleanup ran without arming the terminal refusal budget".into()),
    }
    fixture.clock.advance(Duration::from_millis(249))?;
    tokio::select! {
        () = fixture.clock.not_due_check_now(deadline) => {},
        () = before_cleanup.until_held() => return Err("idle cleanup began before the full refusal budget elapsed".into()),
    }
    if socket.next().now_or_never().is_some() {
        return Err("the peer received a frame while its server write was pending".into());
    }
    match settlement {
        WriteSettlement::Writable => {
            gated.gate.blocked.store(false, Ordering::SeqCst);
            gated.gate.wake_writer();
            let expected = LinkFrame::Data(DataFrame::MachineTokenRenewal {
                correlation_id,
                result: TokenRenewalResult::Refused {
                    reason: TokenRenewalRefusal::ConnectionEnrollmentEnded,
                },
            });
            let response = receive(&mut socket).await?;
            if response != expected {
                return Err(format!(
                    "expected the correlated inactive refusal after IO resumed: {response:?}"
                )
                .into());
            }
            require_inactive_close(&mut socket).await?;
        }
        WriteSettlement::Unwritable => fixture.clock.advance(Duration::from_millis(1))?,
    }
    before_cleanup.until_held().await;
    let mut replacement = connect_replacement(fixture).await?;
    require_welcome(&mut replacement).await?;
    before_cleanup.release();
    after_cleanup.until_held().await;
    after_cleanup.release();
    gated.gate.dropped.notified().await;
    if matches!(settlement, WriteSettlement::Unwritable)
        && let Some(Ok(frame)) =
            read_fixture_frame(&mut socket, "revocation backpressure link frame").await?
    {
        return Err(
            format!("unwritable transport unexpectedly received a frame: {frame:?}").into(),
        );
    }
    send(&mut replacement, &LinkFrame::Ping).await?;
    if receive(&mut replacement).await? != LinkFrame::Pong {
        return Err("idle cleanup disturbed the replacement enrollment's socket".into());
    }
    Ok(())
}

async fn revoke_while_reply_is_blocked(
    fixture: &Fixture,
    gated: &GatedLink,
    settlement: WriteSettlement,
) -> Result<()> {
    let mut socket = gated.connect(fixture).await?;
    let gate = &gated.gate;
    let private = reqwest::Client::builder()
        .no_proxy()
        .unix_socket(fixture.signin_socket.clone())
        .build()?;
    let revocation = signed::RevocationJourney {
        client: &private,
        origin: "http://localhost",
        organization: signed::ORGANIZATION,
        now: signed::NOW_SECONDS,
    };
    let owner = revocation.prepare().await?;
    gate.blocked.store(true, Ordering::SeqCst);
    let request = LinkRequest::new(
        "019f0000-0000-7000-8000-00000000e300".parse()?,
        caller()?,
        create_workspace("019f0000-0000-7000-8000-00000000e301", "Blocked reply")?,
    )?;
    send(
        &mut socket,
        &LinkFrame::Data(DataFrame::Request {
            request: Box::new(request),
        }),
    )
    .await?;
    // No peer reads from here. Wait for Pending at the Axum socket's actual write boundary.
    gate.pending.notified().await;
    if matches!(settlement, WriteSettlement::Writable) {
        // Lift the IO block without waking the writer. Committed revocation's
        // cancellation wakes the pending send and the typed close may flush.
        gate.blocked.store(false, Ordering::SeqCst);
    }
    let close_deadline = fixture
        .clock
        .monotonic_now()
        .after(Duration::from_millis(250));
    revocation
        .expect(
            &owner,
            fixture.enrollment_id.as_str(),
            json!({"result":"revoked"}),
        )
        .await?;
    gate.wake_writer();
    if matches!(settlement, WriteSettlement::Unwritable) {
        tokio::select! {
            () = fixture.clock.not_due_check_now(close_deadline) => {}
            () = gate.dropped.notified() => return Err("the transport dropped before arming the authority close budget".into()),
        }
        fixture.clock.advance(Duration::from_millis(249))?;
        tokio::select! {
            () = fixture.clock.not_due_check_now(close_deadline) => {}
            () = gate.dropped.notified() => return Err("the transport dropped before the authority close budget elapsed".into()),
        }
        fixture.clock.advance(Duration::from_millis(1))?;
    }
    gate.dropped.notified().await;
    if matches!(settlement, WriteSettlement::Writable) {
        let first = read_fixture_frame(&mut socket, "revocation backpressure link frame").await?;
        let close = if matches!(first, Some(Ok(reqwest_websocket::Message::Text(_)))) {
            // Tungstenite may already have queued the ordinary reply in its write buffer.
            read_fixture_frame(&mut socket, "revocation backpressure link frame").await?
        } else {
            first
        };
        if !matches!(close, Some(Ok(reqwest_websocket::Message::Close { code, .. }))
            if MachineLinkCloseCode::from_code(code.into()) == Some(MachineLinkCloseCode::Revoked))
        {
            return Err(format!(
                "cancelling the pending ordinary send must publish Revoked: {close:?}"
            )
            .into());
        }
    }
    Ok(())
}

#[derive(Default)]
struct WriteGate {
    blocked: AtomicBool,
    pending: Notify,
    dropped: Notify,
    waker: Mutex<Option<Waker>>,
}

impl WriteGate {
    fn wake_writer(&self) {
        if let Some(waker) = self
            .waker
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        {
            waker.wake();
        }
    }
}

struct GatedListener {
    listener: TcpListener,
    gate: Arc<WriteGate>,
}

impl Listener for GatedListener {
    type Io = GatedSocket;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        let (stream, address) = Listener::accept(&mut self.listener).await;
        (
            GatedSocket {
                stream,
                gate: self.gate.clone(),
            },
            address,
        )
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        self.listener.local_addr()
    }
}

struct GatedSocket {
    stream: TcpStream,
    gate: Arc<WriteGate>,
}

impl Drop for GatedSocket {
    fn drop(&mut self) {
        self.gate.dropped.notify_one();
    }
}

impl AsyncRead for GatedSocket {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(context, buffer)
    }
}

impl AsyncWrite for GatedSocket {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        if !self.gate.blocked.load(Ordering::SeqCst) {
            return Pin::new(&mut self.stream).poll_write(context, buffer);
        }
        *self
            .gate
            .waker
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(context.waker().clone());
        self.gate.pending.notify_one();
        Poll::Pending
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(context)
    }
}
