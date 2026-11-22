use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use approval_request_protocol::{
    ProviderStopCause, Raise, RegisterRunReply, RegisterRunRequest, StopReceiptEnding, StopReply,
    StopRequest,
};
use state_machine_core::{StateMachine, TransitionError};
use switchboard_approval_holder::{
    run_gate::RunGate,
    waiting_request::{LocalNegative, ProviderReply, WriterOutcome},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use turmoil::net::{TcpListener, TcpStream};

use super::fixtures::{GateEvent, Mac, RESPONDER, RequestEffect, RequestEvent, authority, raise};

async fn send(stream: &mut TcpStream, body: &[u8]) -> io::Result<()> {
    let length = u32::try_from(body.len()).map_err(io::Error::other)?;
    stream.write_u32(length).await?;
    stream.write_all(body).await
}

async fn receive(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    let length = usize::try_from(stream.read_u32().await?).map_err(io::Error::other)?;
    if length > 16_384 {
        return Err(io::Error::other("unexpected test frame length"));
    }
    let mut body = vec![0; length];
    stream.read_exact(&mut body).await?;
    Ok(body)
}

async fn central_peer(lost: Arc<AtomicUsize>) -> turmoil::Result {
    let listener = TcpListener::bind("0.0.0.0:7789").await?;
    let registered = serde_json::to_vec(&RegisterRunReply::Registered {})?;
    let standing = serde_json::to_vec(&StopReply::Stands {
        ending: StopReceiptEnding::TimedOut {},
    })?;
    let mut held_connections = Vec::new();
    let mut first_stop = None;
    for connection in 1..=3 {
        let (mut link, _) = listener.accept().await?;
        let register: RegisterRunRequest = serde_json::from_slice(&receive(&mut link).await?)?;
        assert_eq!(register.router_run, authority().router_run);
        if connection == 1 {
            turmoil::partition_oneway("central", "mac");
            send(&mut link, &registered).await?;
            lost.fetch_add(1, Ordering::SeqCst);
            held_connections.push(link);
            continue;
        }

        send(&mut link, &registered).await?;
        if connection == 2 {
            let request: Raise = serde_json::from_slice(&receive(&mut link).await?)?;
            assert_eq!(request, raise());
        }

        let stop_bytes = receive(&mut link).await?;
        let stop: StopRequest = serde_json::from_slice(&stop_bytes)?;
        assert_eq!(
            stop,
            StopRequest::Provider {
                request_id: raise().request_id().clone(),
                router_run: authority().router_run,
                contents_digest: raise().contents_digest()?,
                cause: ProviderStopCause::TurnStopped {},
            }
        );
        if connection == 2 {
            first_stop = Some(stop_bytes);
            turmoil::partition_oneway("central", "mac");
            send(&mut link, &standing).await?;
            lost.fetch_add(1, Ordering::SeqCst);
            held_connections.push(link);
        } else {
            assert_eq!(
                Some(stop_bytes),
                first_stop,
                "a lost Stop reply must be retried with exactly the same JSON bytes"
            );
            send(&mut link, &standing).await?;
        }
    }
    Ok(())
}

#[test]
fn lost_registration_and_stop_replies_recover_across_two_reconnects() {
    let mut sim = turmoil::Builder::new().build();
    let lost = Arc::new(AtomicUsize::new(0));
    sim.host("central", {
        let lost = Arc::clone(&lost);
        move || central_peer(Arc::clone(&lost))
    });
    sim.client("mac", async {
        let mut mac = Mac::new();
        let call_timeout = Duration::from_millis(250);
        let registered = RegisterRunReply::Registered {};

        let mut link = TcpStream::connect("central:7789").await?;
        send(&mut link, &serde_json::to_vec(&mac.welcome(1))?).await?;
        assert!(
            tokio::time::timeout(call_timeout, receive(&mut link))
                .await
                .is_err(),
            "the simulated Register reply must be lost at the link"
        );
        assert_eq!(mac.gate.ready_connection(), None);
        mac.gate_event(GateEvent::RegistrationFailed { connection: 1 });
        mac.lose_connection(1);
        drop(link);
        turmoil::repair_oneway("central", "mac");

        let mut link = TcpStream::connect("central:7789").await?;
        send(&mut link, &serde_json::to_vec(&mac.welcome(2))?).await?;
        assert_eq!(
            RunGate::transition(
                &mac.gate,
                GateEvent::RegistrationReply {
                    connection: 1,
                    reply: registered.clone(),
                }
            ),
            Err(TransitionError::Stale)
        );
        mac.registered(2, serde_json::from_slice(&receive(&mut link).await?)?);
        assert_eq!(mac.gate.ready_connection(), Some(&2));

        let start = mac.publish();
        assert_eq!(start.len(), 1);
        let published_raise = start
            .into_iter()
            .find_map(|effect| match effect {
                RequestEffect::Raise {
                    connection: 2,
                    request,
                } => Some(request),
                _ => None,
            })
            .unwrap();
        send(&mut link, &serde_json::to_vec(&published_raise)?).await?;
        let cause = ProviderStopCause::TurnStopped {};
        mac.wait_event(RequestEvent::LocalStop { cause });
        let negative = ProviderReply::LocalNegative(LocalNegative::Stopped(cause));
        mac.wait_event(RequestEvent::TakeFinalPermit {
            responder: RESPONDER,
            reply: negative.clone(),
        });
        assert_eq!(
            mac.wait_event(RequestEvent::StartWrite {
                responder: RESPONDER,
                reply: negative.clone()
            }),
            [RequestEffect::Submit {
                responder: RESPONDER,
                reply: negative
            }]
        );
        mac.wait_event(RequestEvent::WriteFinished {
            responder: RESPONDER,
            outcome: WriterOutcome::WriteReturned,
        });

        let first_stop = mac
            .publish()
            .into_iter()
            .find_map(|effect| match effect {
                RequestEffect::Stop {
                    connection: 2,
                    request,
                } => Some(request),
                _ => None,
            })
            .unwrap();
        send(&mut link, &serde_json::to_vec(&first_stop)?).await?;
        assert!(
            tokio::time::timeout(call_timeout, receive(&mut link))
                .await
                .is_err(),
            "the simulated authoritative Stop reply must be lost"
        );
        mac.lose_connection(2);
        assert!(
            !mac.wait.is_closed(),
            "the owed Stop must survive the simulated lost reply"
        );
        drop(link);
        turmoil::repair_oneway("central", "mac");

        let mut link = TcpStream::connect("central:7789").await?;
        send(&mut link, &serde_json::to_vec(&mac.welcome(3))?).await?;
        mac.registered(3, serde_json::from_slice(&receive(&mut link).await?)?);
        let effects = mac.publish();
        assert_eq!(effects.len(), 1);
        let second_stop = effects
            .into_iter()
            .find_map(|effect| match effect {
                RequestEffect::Stop {
                    connection: 3,
                    request,
                } => Some(request),
                _ => None,
            })
            .unwrap();
        assert_eq!(second_stop, first_stop);
        send(&mut link, &serde_json::to_vec(&second_stop)?).await?;
        let reply: StopReply = serde_json::from_slice(&receive(&mut link).await?)?;
        assert_eq!(
            mac.wait_event(RequestEvent::StopReply {
                connection: 3,
                reply: reply.clone()
            }),
            [RequestEffect::StopReconciled(reply)]
        );
        assert!(mac.wait.is_closed());
        Ok(())
    });
    sim.run().unwrap();
    assert_eq!(
        lost.load(Ordering::SeqCst),
        2,
        "both timeouts must follow a published reply on the partitioned link"
    );
}
