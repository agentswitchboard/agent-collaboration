use std::{
    future::Future,
    io,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};

use axum::serve::Listener;
use central_link_client::{AdmittedConnection, EnrollmentEnd, ReadyLinkLane};
use collaboration_central_tokio::{
    WorkspaceLink,
    test_support::{MACHINE_LINK_FIXTURE_MACHINE, MachineLinkFixture, start_session_link_fixture},
};
use router_clock::{ClockInstant, ManualClock, RuntimeClock};
use switchboard_central_link::{
    DialAddress,
    lease_source::{LeaseFailure, LeaseFuture, LeaseSource, LinkLease},
    link_driver::{DriverSettings, LinkDriver},
    link_lane::{LinkLaneHandle, link_lane_channel},
    link_policy::{LeaseVersion, LeaseWindow, LinkStatus},
};
use tokio::{
    io::{AsyncRead, ReadBuf},
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
};
use turmoil::net::{TcpListener, TcpStream};

use self::{
    packet_flight::PacketFlight,
    simulation_clock::ObservedClock,
    write_readiness::{HeldWriter, WriterGate},
};

mod custody_cases;
mod packet_flight;
mod simulation_clock;
mod write_readiness;

const CENTRAL: &str = "central";
const MAC: &str = "mac";
const LINK_PORT: u16 = 7789;

struct SimulatedListener(TcpListener);

impl Listener for SimulatedListener {
    type Io = TcpStream;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        self.0.accept().await.unwrap()
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        self.0.local_addr()
    }
}

async fn serve_central(
    clock: Arc<ManualClock>,
    ready: watch::Sender<Option<Arc<MachineLinkFixture>>>,
    mut stop: watch::Receiver<bool>,
    closed: watch::Sender<bool>,
) -> turmoil::Result {
    let fixture = Arc::new(start_session_link_fixture(clock).await.unwrap());
    let link = Arc::new(WorkspaceLink::new(fixture.host.clone()));
    let stopping_link = link.clone();
    let listener = SimulatedListener(TcpListener::bind(("0.0.0.0", LINK_PORT)).await?);
    ready.send_replace(Some(fixture.clone()));
    axum::serve(listener, link.router())
        .with_graceful_shutdown(async move {
            let _ = stop.wait_for(|stop| *stop).await;
            stopping_link.begin_shutdown();
        })
        .await?;
    link.shutdown().await;
    drop(ready.send_replace(None));
    Arc::into_inner(fixture).unwrap().close().await.unwrap();
    closed.send_replace(true);
    Ok(())
}

#[test]
fn real_central_welcomes_the_mac_over_simulated_tcp() {
    run_link_case(|link| async move {
        assert!(matches!(*link.status.borrow(), LinkStatus::Welcomed { .. }));
        link.close().await;
    });
}

struct SimulatedLink {
    clock: Arc<ManualClock>,
    driver_clock: Arc<ObservedClock>,
    central: Arc<MachineLinkFixture>,
    network: PacketFlight,
    lane: LinkLaneHandle,
    status: watch::Receiver<LinkStatus>,
    admission: AdmittedConnection,
    break_socket: oneshot::Sender<()>,
    socket_breakers: mpsc::UnboundedReceiver<oneshot::Sender<()>>,
    dial_results: mpsc::UnboundedReceiver<bool>,
    lease_token: watch::Sender<String>,
    writer_gate: WriterGate,
    read_probe: ReadProbe,
    _settings: watch::Sender<DriverSettings<u8>>,
    shutdown: watch::Sender<bool>,
    driver: JoinHandle<()>,
}

impl SimulatedLink {
    async fn start(
        clock: Arc<ManualClock>,
        network: PacketFlight,
        central: Arc<MachineLinkFixture>,
    ) -> Self {
        let address = format!(
            "ws://127.0.0.1:{LINK_PORT}/v1/organizations/{}/link",
            central.organization_id.as_str(),
        );
        let (current_settings, settings) = watch::channel(DriverSettings {
            binding: Some(1),
            address: DialAddress::test_loopback(&address),
        });
        let (shutdown, shutdown_rx) = watch::channel(false);
        let (status, link_status) = watch::channel(LinkStatus::NotEnrolled);
        let (lane, owner) = link_lane_channel();
        let (breaker, mut socket_breakers) = mpsc::unbounded_channel();
        let (dial_finished, mut dial_results) = mpsc::unbounded_channel();
        let driver_clock = Arc::new(ObservedClock::new(clock.clone()));
        let (lease_token, token) = watch::channel(central.token.clone());
        let writer_gate = WriterGate::new();
        let read_probe = ReadProbe::default();
        let driver = tokio::spawn(
            LinkDriver {
                machine_id: MACHINE_LINK_FIXTURE_MACHINE.parse().unwrap(),
                clock: driver_clock.clone(),
                lease_source: Arc::new(SimulatedLeases {
                    central: central.clone(),
                    clock: clock.clone(),
                    token,
                }),
                settings,
                shutdown: shutdown_rx,
                status,
                lane: owner,
            }
            .run_with_test_transport({
                let writer_gate = writer_gate.clone();
                let read_probe = read_probe.clone();
                move || {
                    let breaker = breaker.clone();
                    let dial_finished = dial_finished.clone();
                    let writer_gate = writer_gate.clone();
                    let read_probe = read_probe.clone();
                    async move {
                        let connected = TcpStream::connect((CENTRAL, LINK_PORT)).await;
                        dial_finished.send(connected.is_ok()).unwrap();
                        let (reader, writer) = connected?.into_split();
                        let (cut, reset) = oneshot::channel();
                        breaker.send(cut).unwrap();
                        Ok(tokio::io::join(
                            ResettableRead {
                                reader,
                                reset,
                                read_probe,
                                read_to_report: None,
                            },
                            HeldWriter {
                                raw: writer,
                                gate: writer_gate,
                            },
                        ))
                    }
                }
            }),
        );
        let admission = lane
            .watch_admitted()
            .wait_for(Option::is_some)
            .await
            .unwrap()
            .clone()
            .unwrap();
        let break_socket = socket_breakers.recv().await.unwrap();
        assert_eq!(
            dial_results.recv().await,
            Some(true),
            "initial simulated TCP dial"
        );
        Self {
            clock,
            driver_clock,
            central,
            network,
            lane,
            status: link_status,
            admission,
            break_socket,
            socket_breakers,
            dial_results,
            lease_token,
            writer_gate,
            read_probe,
            _settings: current_settings,
            shutdown,
            driver,
        }
    }

    async fn reset_socket(&mut self) {
        // Replace the sender first so we can keep the handle for the new
        // socket after the driver has withdrawn this one.
        let (placeholder, _) = oneshot::channel();
        std::mem::replace(&mut self.break_socket, placeholder)
            .send(())
            .unwrap();
        self.lane
            .watch_admitted()
            .wait_for(Option::is_none)
            .await
            .unwrap();
    }

    async fn reconnect(&mut self) {
        let deadline = self.next_retry().await;
        self.start_retry(deadline);
        assert_eq!(
            self.dial_results.recv().await,
            Some(true),
            "replacement TCP dial"
        );
        self.read_replacement().await;
    }

    async fn next_retry(&self) -> ClockInstant {
        assert!(
            self.lane.watch_admitted().borrow().is_none(),
            "only a withdrawn driver can retry"
        );
        self.driver_clock.next_active_deadline().await
    }

    fn start_retry(&mut self, deadline: ClockInstant) {
        self.clock
            .advance(deadline.saturating_duration_since(self.clock.monotonic_now()))
            .unwrap();
    }

    async fn require_isolated_dial_failure(&mut self) {
        assert_eq!(
            self.dial_results.recv().await,
            Some(false),
            "a real replacement TCP dial must fail while the partition is active"
        );
        assert!(self.lane.watch_admitted().borrow().is_none());
    }

    async fn read_replacement(&mut self) {
        let previous = &self.admission.connection;
        let new = self
            .lane
            .watch_admitted()
            .wait_for(|admitted| {
                admitted
                    .as_ref()
                    .is_some_and(|new| &new.connection != previous)
            })
            .await
            .unwrap()
            .clone()
            .unwrap();
        self.admission = new;
        self.break_socket = self.socket_breakers.recv().await.unwrap();
    }

    async fn reconnect_after_partition(&mut self) {
        let token = self.central.mint_machine_token().await.unwrap();
        let runtime = self
            .central
            .host
            .central(&self.central.organization_id)
            .await
            .unwrap();
        let fetches = self.central.keys().fetches();
        // Run the actual verifier at frozen injected time before healthy
        // TCP is possible. The next dial uses this same credential.
        let _verified = runtime.admit_machine_link(&token).await.unwrap();
        assert_eq!(
            self.central.keys().fetches(),
            fetches + 1,
            "the expired issuer cache must refresh before reconnect"
        );
        self.lease_token.send_replace(token);
        turmoil::repair(MAC, CENTRAL);
        turmoil::hold(MAC, CENTRAL);
        self.reconnect().await;
    }

    async fn close(self) {
        self.shutdown.send_replace(true);
        self.driver.await.unwrap();
    }
}

struct SimulatedLeases {
    central: Arc<MachineLinkFixture>,
    clock: Arc<ManualClock>,
    token: watch::Receiver<String>,
}

impl LeaseSource<u8> for SimulatedLeases {
    fn acquire<'lease>(
        &'lease self,
        binding: &'lease u8,
        newer_than: Option<LeaseVersion>,
    ) -> LeaseFuture<'lease, u8> {
        Box::pin(async move {
            let token = match newer_than {
                Some(_) => self
                    .central
                    .mint_machine_token()
                    .await
                    .map_err(|_| LeaseFailure::Fault)?,
                None => self.token.borrow().clone(),
            };
            Ok(LinkLease {
                token: token.try_into().map_err(|_| LeaseFailure::Fault)?,
                window: LeaseWindow {
                    binding: *binding,
                    version: LeaseVersion(newer_than.map_or(1, |old| old.0 + 1)),
                    usable_until: self.clock.monotonic_now() + Duration::from_secs(500),
                },
            })
        })
    }

    fn end(&self, _binding: &u8, _cause: EnrollmentEnd) {}
}

type ReadBytesSender = oneshot::Sender<Vec<u8>>;
type DriverReadReport = (ReadBytesSender, Vec<u8>);

#[derive(Clone, Default)]
struct ReadProbe(Arc<Mutex<Option<ReadBytesSender>>>);

impl ReadProbe {
    fn observe_next_read_turn(&self) -> oneshot::Receiver<Vec<u8>> {
        let (report, observed) = oneshot::channel();
        assert!(self.0.lock().unwrap().replace(report).is_none());
        observed
    }
}

struct ResettableRead<Reader> {
    reader: Reader,
    reset: oneshot::Receiver<()>,
    read_probe: ReadProbe,
    read_to_report: Option<DriverReadReport>,
}

impl<Reader: AsyncRead + Unpin> AsyncRead for ResettableRead<Reader> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if Pin::new(&mut self.reset).poll(context).is_ready() {
            return Poll::Ready(Err(io::ErrorKind::ConnectionReset.into()));
        }
        // Report the previous bytes when the driver re-enters the read
        // stream, rather than while it is still decoding the reply.
        if let Some((report, bytes)) = self.read_to_report.take() {
            let _ = report.send(bytes);
        }
        let before = buffer.filled().len();
        let read = Pin::new(&mut self.reader).poll_read(context, buffer);
        if let Poll::Ready(Ok(())) = read
            && let Some(bytes) = buffer
                .filled()
                .get(before..)
                .filter(|bytes| !bytes.is_empty())
        {
            let report = self.read_probe.0.lock().unwrap().take();
            if let Some(report) = report {
                self.read_to_report = Some((report, bytes.to_vec()));
            }
        }
        read
    }
}

fn run_link_case<Case, Running>(case: Case)
where
    Case: FnOnce(SimulatedLink) -> Running + 'static,
    Running: Future<Output = ()> + 'static,
{
    let clock = Arc::new(ManualClock::new("2030-03-17T17:46:40Z".parse().unwrap()));
    let network = PacketFlight::new();
    let mut simulation = turmoil::Builder::new()
        .enable_tokio_io()
        .rng_seed(42)
        .simulation_duration(Duration::from_secs(120))
        .build();
    let (ready, mut central) = watch::channel(None);
    let (stop, stopped) = watch::channel(false);
    let (closed, mut central_closed) = watch::channel(false);
    simulation.host(CENTRAL, {
        let clock = clock.clone();
        move || {
            serve_central(
                clock.clone(),
                ready.clone(),
                stopped.clone(),
                closed.clone(),
            )
        }
    });
    simulation.client(MAC, {
        let clock = clock.clone();
        let network = network.clone();
        async move {
            let fixture = central.wait_for(Option::is_some).await?.clone().unwrap();
            case(SimulatedLink::start(clock, network, fixture).await).await;
            stop.send_replace(true);
            let _ = central_closed.wait_for(|closed| *closed).await?;
            Ok(())
        }
    });
    let outcome = network.run(&mut simulation, &clock);
    assert!(
        outcome.is_ok(),
        "{outcome:?} at {:?}, message wire: {:?}",
        clock.monotonic_now(),
        network
            .frames()
            .iter()
            .map(|frame| (frame.kind, frame.connection_port, frame.sent_at))
            .collect::<Vec<_>>()
    );
}
