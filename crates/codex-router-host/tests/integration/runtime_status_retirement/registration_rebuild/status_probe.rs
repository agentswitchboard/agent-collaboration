use std::{
    future::Future as _,
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};

use central_link_protocol::{CorrelationId, DataFrame, LinkFrame};
use codex_router_host::{
    HostCoordinationPaths, MessageSystemObservation, MessageSystemReason, MessageSystemStatus,
};
use futures_util::SinkExt;
use router_clock::ManualClock;
use switchboard_central_link::link_policy::LinkStatus;
use tokio::{
    io::AsyncWrite,
    net::TcpStream,
    sync::{oneshot, watch},
    task::JoinHandle,
};
use tokio_tungstenite::{WebSocketStream, tungstenite::Message};

use super::{Registration, TestResult, assert_operator_observation, read_frame};

#[derive(Clone)]
pub(super) struct CapabilityHold(Arc<Mutex<Option<HeldCapability>>>);

pub(super) struct HeldCapability {
    first_write: Option<oneshot::Sender<()>>,
    release: oneshot::Receiver<()>,
}

impl CapabilityHold {
    pub(super) fn new() -> (Self, oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (first_write, observed) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let hold = HeldCapability {
            first_write: Some(first_write),
            release: released,
        };
        (Self(Arc::new(Mutex::new(Some(hold)))), observed, release)
    }

    pub(super) fn wrap<Writer>(&self, writer: Writer) -> CapabilityWriter<Writer> {
        CapabilityWriter {
            hold: self.0.lock().expect("capability hold").take(),
            writer,
        }
    }
}

pub(super) struct CapabilityWriter<Writer> {
    hold: Option<HeldCapability>,
    writer: Writer,
}

impl<Writer: AsyncWrite + Unpin> AsyncWrite for CapabilityWriter<Writer> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        // The real HTTP upgrade precedes this first masked WebSocket Text frame.
        // Blocking it holds the driver after Welcome and before admission.
        if bytes.first() == Some(&0x81)
            && let Some(hold) = &mut self.hold
        {
            if let Some(first) = hold.first_write.take() {
                let _ = first.send(());
            }
            match Pin::new(&mut hold.release).poll(context) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(())) => self.hold = None,
                Poll::Ready(Err(_)) => {
                    return Poll::Ready(Err(io::Error::other("capability hold dropped")));
                }
            }
        }
        Pin::new(&mut self.writer).poll_write(context, bytes)
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.writer).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.writer).poll_shutdown(context)
    }
}

#[derive(Default)]
pub(super) struct StatusHistory {
    pub(super) driver: Vec<LinkStatus>,
    pub(super) message_system: Vec<MessageSystemObservation>,
}

pub(super) fn collect_status(
    mut driver: watch::Receiver<LinkStatus>,
    mut message_system: watch::Receiver<MessageSystemObservation>,
) -> (
    oneshot::Sender<()>,
    oneshot::Receiver<()>,
    JoinHandle<StatusHistory>,
) {
    let (stop, mut stopping) = oneshot::channel();
    let (ready, started) = oneshot::channel();
    let collector = tokio::spawn(async move {
        let mut history = StatusHistory {
            driver: vec![driver.borrow_and_update().clone()],
            message_system: vec![*message_system.borrow_and_update()],
        };
        let _ = ready.send(());
        loop {
            tokio::select! {
                biased;
                changed = driver.changed() => {
                    if changed.is_err() { break; }
                    history.driver.push(driver.borrow_and_update().clone());
                }
                changed = message_system.changed() => {
                    if changed.is_err() { break; }
                    history.message_system.push(*message_system.borrow_and_update());
                }
                _ = &mut stopping => break,
            }
        }
        history
    });
    (stop, started, collector)
}

impl StatusHistory {
    pub(super) fn saw_connected(&self) -> bool {
        self.driver
            .iter()
            .any(|status| matches!(status, LinkStatus::Connected { .. }))
            || self
                .message_system
                .iter()
                .any(|observation| observation.status == MessageSystemStatus::Connected)
    }
}

pub(super) async fn exercise_refusal_through_renewal(
    socket: &mut WebSocketStream<TcpStream>,
    registration: &Registration,
    paths: &HostCoordinationPaths,
    clock: &ManualClock,
    status: &mut watch::Receiver<MessageSystemObservation>,
) -> TestResult {
    use central_link_protocol::{LinkCallRefusal, MachineLinkCloseCode};
    clock.advance(Duration::from_secs(5))?;
    let LinkFrame::Data(DataFrame::RenewMachineToken { correlation_id, .. }) =
        read_frame(socket).await?
    else {
        return Err("renewal must be in flight while the registration answer is held".into());
    };
    send_json(socket, serde_json::json!({
        "kind": "centralCallReply",
        "correlationId": registration.correlation,
        "result": { "kind": "refused", "reason": LinkCallRefusal::AuthorityLapsed { cause: MachineLinkCloseCode::Expired } }
    })).await?;
    status
        .wait_for(|status| status.reason == MessageSystemReason::AuthorityExpired)
        .await?;
    assert_operator_observation(
        paths,
        MessageSystemStatus::RegistrationFailed,
        MessageSystemReason::AuthorityExpired,
    )
    .await?;
    send_json(
        socket,
        serde_json::json!({
            "kind":"machineTokenRenewal",
            "correlationId":correlation_id,
            "result": {
                "kind":"renewed",
                "centralTime":"2030-01-01T00:00:05Z",
                "effectiveExpiresAt":"2030-01-01T00:10:05Z"
            }
        }),
    )
    .await?;
    // The typed refusal returns after the queued renewal is processed, while
    // the injected clock is still below the registration retry deadline.
    let barrier = "01890f2e-7b4c-7cc0-98c4-00000000009e";
    send_json(
        socket,
        serde_json::json!({
            "kind":"callMac",
            "correlationId":barrier,
            "request":{"call":"fixtureObservationBarrier","envelope":{"target":"x","body":{}}}
        }),
    )
    .await?;
    assert!(matches!(read_frame(socket).await?,
        LinkFrame::Data(DataFrame::MacCallReply { correlation_id, .. }) if correlation_id == barrier.parse::<CorrelationId>()?
    ));
    assert_operator_observation(
        paths,
        MessageSystemStatus::RegistrationFailed,
        MessageSystemReason::AuthorityExpired,
    )
    .await?;
    Ok(())
}

async fn send_json(
    socket: &mut WebSocketStream<TcpStream>,
    value: serde_json::Value,
) -> TestResult {
    socket.send(Message::Text(value.to_string().into())).await?;
    Ok(())
}
