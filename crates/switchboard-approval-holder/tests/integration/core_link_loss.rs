use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use switchboard_approval_holder::waiting_request::{LinkAttemptDisposition, LinkCallAnswer};

use approval_request_protocol::{
    ApprovalWindow, HolderStopCause, ProviderStopCause, Raise, RaiseReceipt, RaiseReply,
    RegisterRunReply, RegisterRunRequest, RunRefusal, StopReceiptEnding, StopReply, StopRequest,
};
use state_machine_core::{StateMachine, TransitionError};
use switchboard_approval_holder::waiting_request::{LocalNegative, ProviderReply, WriterOutcome};
use switchboard_run_gate::RunGate;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use turmoil::net::{TcpListener, TcpStream};

use super::fixtures::{
    GateEvent, Mac, RESPONDER, RequestEffect, RequestEvent, authority, call, raise,
};

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
    let mut first_raise = None;
    for connection in 1..=4 {
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
            let bytes = receive(&mut link).await?;
            assert_eq!(serde_json::from_slice::<Raise>(&bytes)?, raise());
            first_raise = Some(bytes);
            let refusal = RaiseReply::AuthorityRefused {
                refusal: RunRefusal::RunNotRegistered {},
            };
            send(&mut link, &serde_json::to_vec(&refusal)?).await?;
            continue;
        }
        if connection == 3 {
            assert_eq!(
                Some(receive(&mut link).await?),
                first_raise,
                "new link keeps the Raise body"
            );
            let receipt = RaiseReply::Receipt {
                receipt: RaiseReceipt {
                    request_id: raise().request_id().clone(),
                    window: ApprovalWindow::from_first_save(
                        "2026-11-22T17:00:00Z".parse().unwrap(),
                    )
                    .unwrap(),
                },
            };
            send(&mut link, &serde_json::to_vec(&receipt)?).await?;
        }

        let stop_bytes = receive(&mut link).await?;
        let stop: StopRequest = serde_json::from_slice(&stop_bytes)?;
        assert_eq!(
            stop,
            StopRequest::Provider {
                request_id: raise().request_id().clone(),
                router_run: authority().router_run,
                contents_digest: raise().contents_digest()?,
                cause: ProviderStopCause::TurnStopped,
            }
        );
        if connection == 3 {
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
        mac.gate_event(GateEvent::RegistrationFailed { call: call(1, 0) });
        mac.lose_connection(1);
        drop(link);
        turmoil::repair_oneway("central", "mac");

        let mut link = TcpStream::connect("central:7789").await?;
        send(&mut link, &serde_json::to_vec(&mac.welcome(2))?).await?;
        assert_eq!(
            RunGate::transition(
                &mac.gate,
                GateEvent::RegistrationReply {
                    call: call(1, 0),
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
                RequestEffect::Raise { call, request, .. } if call.link.connection == 2 => {
                    Some(request)
                }
                _ => None,
            })
            .unwrap();
        send(&mut link, &serde_json::to_vec(&published_raise)?).await?;
        let refusal = serde_json::from_slice(&receive(&mut link).await?)?;
        mac.wait_event(RequestEvent::AttemptSettled {
            call: call(2, 1),
            disposition: LinkAttemptDisposition::Answered(LinkCallAnswer::Raise(refusal)),
        });
        assert_eq!(
            mac.gate.ready_connection(),
            None,
            "RunNotRegistered retires the actual carrier"
        );
        drop(link);

        let mut link = TcpStream::connect("central:7789").await?;
        send(&mut link, &serde_json::to_vec(&mac.welcome(3))?).await?;
        mac.registered(3, serde_json::from_slice(&receive(&mut link).await?)?);
        assert_eq!(
            mac.publish(),
            [RequestEffect::Raise {
                call: call(3, 2),
                authority: authority(),
                request: published_raise.clone()
            }]
        );
        send(&mut link, &serde_json::to_vec(&published_raise)?).await?;
        mac.wait_event(RequestEvent::AttemptSettled {
            call: call(3, 2),
            disposition: LinkAttemptDisposition::Answered(LinkCallAnswer::Raise(
                serde_json::from_slice(&receive(&mut link).await?)?,
            )),
        });
        let cause = ProviderStopCause::TurnStopped;
        mac.wait_event(RequestEvent::LocalStop {
            cause: HolderStopCause::TurnStopped,
        });
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
                RequestEffect::Stop { call, request, .. } if call.link.connection == 3 => {
                    Some(request)
                }
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
        mac.lose_connection(3);
        assert!(
            mac.wait.holds_place(),
            "the owed Stop must survive the simulated lost reply"
        );
        drop(link);
        turmoil::repair_oneway("central", "mac");

        let mut link = TcpStream::connect("central:7789").await?;
        send(&mut link, &serde_json::to_vec(&mac.welcome(4))?).await?;
        mac.registered(4, serde_json::from_slice(&receive(&mut link).await?)?);
        let effects = mac.publish();
        assert_eq!(effects.len(), 1);
        let second_stop = effects
            .into_iter()
            .find_map(|effect| match effect {
                RequestEffect::Stop { call, request, .. } if call.link.connection == 4 => {
                    Some(request)
                }
                _ => None,
            })
            .unwrap();
        assert_eq!(second_stop, first_stop);
        send(&mut link, &serde_json::to_vec(&second_stop)?).await?;
        let reply: StopReply = serde_json::from_slice(&receive(&mut link).await?)?;
        assert_eq!(
            mac.wait_event(RequestEvent::AttemptSettled {
                call: call(4, 4),
                disposition: LinkAttemptDisposition::Answered(LinkCallAnswer::Stop(reply))
            }),
            [RequestEffect::StopReconciled(
                StopReceiptEnding::TimedOut {}
            )]
        );
        assert!(!mac.wait.holds_place());
        Ok(())
    });
    sim.run().unwrap();
    assert_eq!(
        lost.load(Ordering::SeqCst),
        2,
        "both timeouts must follow a published reply on the partitioned link"
    );
}
