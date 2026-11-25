#![expect(
    clippy::panic,
    reason = "The test reports a fault applied without an in-flight packet."
)]

use std::{
    collections::BTreeSet,
    io::Cursor,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use central_link_protocol::{DataFrame, FrameDirection, LinkFrame, decode_text};
use router_clock::{ClockInstant, ManualClock, RuntimeClock};
use tokio::sync::watch;
use tungstenite::protocol::frame::{FrameHeader, coding};
use turmoil::{Protocol, Segment, Sim};

use super::{CENTRAL, LINK_PORT, MAC};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FrameKind {
    Welcome,
    Batch,
    WriteAnswer,
    Offer,
    Receipt,
    ReceiptResult,
    LifecycleCall,
    LifecycleReply,
    Renewal,
    RenewalReply,
    Ping,
    Pong,
    Other,
}

impl FrameKind {
    fn classify(frame: &LinkFrame) -> Self {
        match frame {
            LinkFrame::Data(DataFrame::Welcome { .. }) => Self::Welcome,
            LinkFrame::Data(DataFrame::Batch { .. }) => Self::Batch,
            LinkFrame::Data(DataFrame::Reply { result, .. })
                if matches!(
                    result.as_ref(),
                    central_link_protocol::ReplyPayload::Mail(
                        workspace_model::central_mail::CentralMailResult::Done(
                            workspace_model::central_mail::CentralMailAnswer::Queued { .. }
                        )
                    )
                ) =>
            {
                Self::WriteAnswer
            }
            LinkFrame::Data(DataFrame::Deliver { .. }) => Self::Offer,
            LinkFrame::Data(DataFrame::Receipts { .. }) => Self::Receipt,
            LinkFrame::Data(DataFrame::ReceiptResults { .. }) => Self::ReceiptResult,
            LinkFrame::Data(DataFrame::CallCentral { .. }) => Self::LifecycleCall,
            LinkFrame::Data(DataFrame::CentralCallReply { .. }) => Self::LifecycleReply,
            LinkFrame::Data(DataFrame::RenewMachineToken { .. }) => Self::Renewal,
            LinkFrame::Data(DataFrame::MachineTokenRenewal { .. }) => Self::RenewalReply,
            LinkFrame::Ping => Self::Ping,
            LinkFrame::Pong => Self::Pong,
            LinkFrame::Data(_) => Self::Other,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Packet {
    from: SocketAddr,
    to: SocketAddr,
    sequence: u64,
}

#[derive(Clone, Debug)]
pub(super) struct InFlightFrame {
    pub(super) kind: FrameKind,
    pub(super) frame: LinkFrame,
    pub(super) sent_at: ClockInstant,
    pub(super) utc_ms: i64,
    pub(super) connection_port: u16,
}

#[derive(Clone, Copy, Debug, Default)]
enum Hold {
    #[default]
    Open,
    Next {
        kind: FrameKind,
        skip: usize,
    },
    Captured(Packet),
    Release(Packet),
    Discard(Packet),
}

#[derive(Clone)]
pub(super) struct PacketFlight {
    hold: Arc<Mutex<Hold>>,
    frames: watch::Sender<Vec<InFlightFrame>>,
    steps: watch::Sender<usize>,
    released: watch::Sender<Option<Packet>>,
}

impl PacketFlight {
    pub(super) fn new() -> Self {
        Self {
            hold: Arc::default(),
            frames: watch::channel(Vec::new()).0,
            steps: watch::channel(0).0,
            released: watch::channel(None).0,
        }
    }

    pub(super) fn hold_next(&self, kind: FrameKind) -> usize {
        self.hold_after(kind, 0)
    }

    pub(super) fn hold_after(&self, kind: FrameKind, skip: usize) -> usize {
        let mut hold = self.hold.lock().unwrap();
        assert!(matches!(*hold, Hold::Open));
        *hold = Hold::Next { kind, skip };
        self.frames.borrow().len()
    }

    pub(super) fn release(&self) {
        let mut hold = self.hold.lock().unwrap();
        let Hold::Captured(packet) = *hold else {
            panic!("release needs an in-flight packet");
        };
        *hold = Hold::Release(packet);
    }

    pub(super) async fn release_and_wait(&self) {
        let mut released = self.released.subscribe();
        self.release();
        released.changed().await.unwrap();
    }

    pub(super) async fn discard(&self) {
        {
            let mut hold = self.hold.lock().unwrap();
            let Hold::Captured(packet) = *hold else {
                panic!("discard needs an in-flight packet");
            };
            *hold = Hold::Discard(packet);
        }
        loop {
            self.step().await;
            if matches!(*self.hold.lock().unwrap(), Hold::Open) {
                break;
            }
        }
    }

    pub(super) async fn step(&self) {
        let mut steps = self.steps.subscribe();
        steps.changed().await.unwrap();
    }

    pub(super) fn frames(&self) -> Vec<InFlightFrame> {
        self.frames.borrow().clone()
    }

    pub(super) async fn seen(&self, after: usize, kind: FrameKind) -> InFlightFrame {
        self.seen_nth(after, kind, 0).await
    }

    pub(super) async fn seen_nth(
        &self,
        after: usize,
        kind: FrameKind,
        index: usize,
    ) -> InFlightFrame {
        self.seen_matching(after, |frame| frame.kind == kind, index)
            .await
    }

    pub(super) async fn seen_matching(
        &self,
        after: usize,
        matches_frame: impl Fn(&InFlightFrame) -> bool,
        index: usize,
    ) -> InFlightFrame {
        let mut frames = self.frames.subscribe();
        frames
            .wait_for(|frames| {
                frames
                    .iter()
                    .skip(after)
                    .filter(|entry| matches_frame(entry))
                    .nth(index)
                    .is_some()
            })
            .await
            .unwrap()
            .iter()
            .skip(after)
            .filter(|entry| matches_frame(entry))
            .nth(index)
            .unwrap()
            .clone()
    }

    pub(super) fn run(&self, simulation: &mut Sim<'_>, clock: &ManualClock) -> turmoil::Result {
        // Leave the original packets in Turmoil. Each step selects which queued
        // TCP segments can proceed. A new connection can progress while an old
        // socket's packet is still in flight.
        simulation.hold(MAC, CENTRAL);
        let mut seen_packets = BTreeSet::new();
        let mut delivered_packet = None;
        while !simulation.step()? {
            if let Some(packet) = delivered_packet.take() {
                self.released.send_replace(Some(packet));
            }
            let mut hold = self.hold.lock().map_err(|_| "packet fault lock poisoned")?;
            if let Hold::Discard(packet) = *hold {
                let mut queued = 0;
                simulation.links(|links| {
                    queued = links
                        .flatten()
                        .filter(|segment| segment.pair().0.ip() == packet.from.ip())
                        .count();
                });
                if queued == 0 {
                    return Err("the selected packet disappeared before the fault".into());
                }
                if queued == 1 {
                    simulation.partition_oneway(packet.from.ip(), packet.to.ip());
                    simulation.repair_oneway(packet.from.ip(), packet.to.ip());
                    simulation.hold(MAC, CENTRAL);
                    *hold = Hold::Open;
                }
            }
            simulation.links(|links| {
                for link in links {
                    for sent in link {
                        let (from, to) = sent.pair();
                        if let Protocol::Tcp(Segment::Data(sequence, bytes)) = sent.protocol() {
                            let packet = Packet {
                                from,
                                to,
                                sequence: *sequence,
                            };
                            if seen_packets.insert(packet) {
                                let to_central = to.port() == LINK_PORT;
                                for frame in decode_segment(bytes, to_central) {
                                    let kind = FrameKind::classify(&frame);
                                    if let Hold::Next { kind: target, skip } = *hold
                                        && kind == target
                                    {
                                        *hold = if skip == 0 {
                                            Hold::Captured(packet)
                                        } else {
                                            Hold::Next {
                                                kind: target,
                                                skip: skip - 1,
                                            }
                                        };
                                    }
                                    self.frames.send_modify(|frames| {
                                        frames.push(InFlightFrame {
                                            kind,
                                            frame,
                                            sent_at: clock.monotonic_now(),
                                            utc_ms: clock.utc_now().timestamp_millis(),
                                            connection_port: if to_central {
                                                from.port()
                                            } else {
                                                to.port()
                                            },
                                        });
                                    });
                                }
                            }
                            if matches!(*hold, Hold::Captured(held) | Hold::Discard(held) if held == packet) {
                                continue;
                            }
                            if matches!(*hold, Hold::Release(held) if held == packet) {
                                delivered_packet = Some(packet);
                                *hold = Hold::Open;
                            }
                        }
                        sent.deliver();
                    }
                }
            });
            self.steps.send_modify(|steps| *steps += 1);
        }
        Ok(())
    }
}

pub(super) fn decode_segment(mut bytes: &[u8], to_central: bool) -> Vec<LinkFrame> {
    if bytes.starts_with(b"GET ") || bytes.starts_with(b"HTTP/") {
        let end = bytes.windows(4).position(|bytes| bytes == b"\r\n\r\n");
        bytes = end.and_then(|end| bytes.get(end + 4..)).unwrap_or_default();
    }
    let direction = if to_central {
        FrameDirection::ToCentral
    } else {
        FrameDirection::ToRouter
    };
    let mut frames = Vec::new();
    while !bytes.is_empty() {
        let mut cursor = Cursor::new(bytes);
        let Some((header, payload_size)) = FrameHeader::parse(&mut cursor).unwrap() else {
            // A test may cut the TCP stream after only a frame prefix.
            break;
        };
        let payload_start = usize::try_from(cursor.position()).unwrap();
        let payload_end = payload_start + usize::try_from(payload_size).unwrap();
        let Some(payload) = bytes.get(payload_start..payload_end) else {
            break;
        };
        if header.opcode == coding::OpCode::Data(coding::Data::Text) {
            assert!(
                header.is_final,
                "custody scenario frames fit in one WebSocket frame"
            );
            let mut payload = payload.to_vec();
            if let Some(mask) = header.mask {
                for (byte, mask) in payload.iter_mut().zip(mask.into_iter().cycle()) {
                    *byte ^= mask;
                }
            }
            frames.push(decode_text(std::str::from_utf8(&payload).unwrap(), direction).unwrap());
        }
        bytes = bytes.get(payload_end..).unwrap();
    }
    frames
}
