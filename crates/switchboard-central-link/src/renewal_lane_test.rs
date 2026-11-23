use std::{
    future::poll_fn,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use central_link_client::EnrollmentEnd;
use central_link_protocol::{
    DataFrame, FrameDirection, LinkFrame, MachineLinkCloseCode, decode_text,
};
use futures_util::{Sink, Stream};
use router_clock::{ClockInstant, ManualClock, RuntimeClock};
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::{Error, Message, protocol::CloseFrame};

use super::{Control, HeartbeatFixture, elapsed_tick, test_settings};
use crate::{
    lease_source::{LeaseFuture, LeaseSource, LinkLease},
    link_policy::{LeaseVersion, LeaseWindow, LinkInput},
};

struct PolledLeaseSource;

impl LeaseSource<u8> for PolledLeaseSource {
    fn acquire<'source>(
        &'source self,
        binding: &'source u8,
        newer_than: Option<LeaseVersion>,
    ) -> LeaseFuture<'source, u8> {
        assert_eq!(newer_than, Some(LeaseVersion(1)));
        let mut polls_left = 2;
        Box::pin(poll_fn(move |_| {
            if polls_left > 0 {
                polls_left -= 1;
                return Poll::Pending;
            }
            Poll::Ready(Ok(LinkLease {
                window: LeaseWindow {
                    binding: *binding,
                    version: LeaseVersion(2),
                    usable_until: elapsed_tick(600),
                },
                token: "renewed.synthetic.credential"
                    .to_owned()
                    .try_into()
                    .unwrap(),
            }))
        }))
    }

    fn end(&self, _binding: &u8, _cause: EnrollmentEnd) {}
}

struct BusySocket<'clock> {
    clock: &'clock ManualClock,
    reads: usize,
    renewal_sent_at: Option<ClockInstant>,
}

impl Stream for BusySocket<'_> {
    type Item = Result<Message, Error>;

    fn poll_next(mut self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.reads += 1;
        if self.renewal_sent_at.is_some() {
            return Poll::Ready(Some(Ok(Message::Close(Some(CloseFrame {
                code: MachineLinkCloseCode::Inactive.code().into(),
                reason: "".into(),
            })))));
        }
        if self.reads == 6 {
            // Even a read-first loop reaches an output assertion under the injected clock.
            self.clock.advance(Duration::from_secs(60)).unwrap();
        }
        Poll::Ready(Some(Ok(Message::Text("pong".into()))))
    }
}

impl Sink<Message> for BusySocket<'_> {
    type Error = Error;

    fn poll_ready(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(mut self: Pin<&mut Self>, message: Message) -> Result<(), Self::Error> {
        let Message::Text(text) = message else {
            return Err(Error::Io(std::io::ErrorKind::InvalidData.into()));
        };
        if matches!(
            decode_text(text.as_str(), FrameDirection::ToCentral),
            Ok(LinkFrame::Data(DataFrame::RenewMachineToken { .. }))
        ) {
            if self.renewal_sent_at.is_some() {
                return Err(Error::Io(std::io::ErrorKind::InvalidData.into()));
            }
            self.renewal_sent_at = Some(self.clock.monotonic_now());
        }
        Ok(())
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn a_continuously_ready_socket_renews_before_expiry_and_reads_the_final_close() {
    let fixture = HeartbeatFixture::new();
    let (mut driver, attempt) = fixture.welcomed_driver(&PolledLeaseSource);
    assert!(driver.apply(LinkInput::BindingAvailable(1)).is_some());
    assert!(
        driver
            .apply(LinkInput::DialStarted {
                attempt: attempt.clone(),
                lease: LeaseWindow {
                    binding: 1,
                    version: LeaseVersion(1),
                    usable_until: elapsed_tick(600)
                },
                now: elapsed_tick(0),
            })
            .is_some()
    );
    assert!(
        driver
            .apply(LinkInput::Welcome {
                attempt: attempt.clone(),
                machine_id: fixture.machine.clone(),
                deadline: elapsed_tick(120),
                now: elapsed_tick(0),
            })
            .is_some()
    );
    let mut socket = BusySocket {
        clock: &fixture.clock,
        reads: 0,
        renewal_sent_at: None,
    };
    let (_settings_sender, mut settings) = watch::channel(test_settings());
    let (_shutdown_sender, mut shutdown) = watch::channel(false);
    let mut control = Control {
        settings: &mut settings,
        shutdown: &mut shutdown,
        observed: test_settings(),
    };

    fixture.clock.advance(Duration::from_secs(60)).unwrap();
    assert!(
        driver
            .open_socket(&mut socket, &attempt, &1, elapsed_tick(0), &mut control)
            .await
            .is_ok()
    );

    assert_eq!(
        socket.renewal_sent_at,
        Some(elapsed_tick(60)),
        "ready reads must allow a renewal on the current socket before the old deadline"
    );
    assert!(socket.reads >= 3);
    assert_eq!(driver.state.ending(), Some(EnrollmentEnd::Inactive));
}
