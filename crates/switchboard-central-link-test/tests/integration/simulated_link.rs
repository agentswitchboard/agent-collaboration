use std::{
    collections::VecDeque,
    future::Future,
    io,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
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
use workspace_ids::{MachineId, RouterRunId};

use self::{
    central_receive::{ObservedCentralStream, SocketFrameTrace, SocketObserver},
    packet_flight::PacketFlight,
    simulation_clock::ObservedClock,
    write_readiness::{HeldWriter, WriterGate},
};

pub(super) mod central_receive;
mod custody_cases;
mod mac_capabilities;
mod machine_identity_case;
mod packet_flight;
mod registration_cases;
mod registration_preface;
mod simulation_clock;
mod write_readiness;

const CENTRAL: &str = "central";
const MAC: &str = "mac";
const LINK_PORT: u16 = 7789;

struct SimulatedListener {
    listener: TcpListener,
    trace: Option<SocketFrameTrace>,
    clock: Arc<ManualClock>,
}

impl Listener for SimulatedListener {
    type Io = ObservedCentralStream<TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        let (socket, remote) = self.listener.accept().await.unwrap();
        (
            ObservedCentralStream::new(
                socket,
                self.trace.clone(),
                remote.port(),
                self.clock.clone(),
            ),
            remote,
        )
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        self.listener.local_addr()
    }
}

async fn serve_central(
    clock: Arc<ManualClock>,
    trace: Option<SocketFrameTrace>,
    ready: watch::Sender<Option<Arc<MachineLinkFixture>>>,
    mut stop: watch::Receiver<bool>,
    closed: watch::Sender<bool>,
) -> turmoil::Result {
    let fixture = Arc::new(start_session_link_fixture(clock.clone()).await.unwrap());
    let link = Arc::new(WorkspaceLink::new(fixture.host.clone()));
    let stopping_link = link.clone();
    let listener = SimulatedListener {
        listener: TcpListener::bind(("0.0.0.0", LINK_PORT)).await?,
        trace,
        clock,
    };
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
    run_link_case(|mut link| async move {
        link.status
            .wait_for(|status| matches!(status, LinkStatus::Connected { .. }))
            .await
            .unwrap();
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
        let machine = MACHINE_LINK_FIXTURE_MACHINE.parse().unwrap();
        let token = central.token.clone();
        Self::start_enrolled(clock, network, central, machine, token, None).await
    }

    async fn start_enrolled(
        clock: Arc<ManualClock>,
        network: PacketFlight,
        central: Arc<MachineLinkFixture>,
        machine: MachineId,
        token: String,
        prior_run: Option<RouterRunId>,
    ) -> Self {
        Self::start_enrolled_with_send_trace(
            clock, network, central, machine, token, prior_run, None,
        )
        .await
    }

    async fn start_enrolled_with_send_trace(
        clock: Arc<ManualClock>,
        network: PacketFlight,
        central: Arc<MachineLinkFixture>,
        machine: MachineId,
        token: String,
        prior_run: Option<RouterRunId>,
        sent: Option<SocketFrameTrace>,
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
        let (lease_token, token) = watch::channel(token);
        let writer_gate = WriterGate::new();
        let read_probe = ReadProbe::default();
        let driver = tokio::spawn(
            LinkDriver {
                machine_id: machine,
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
                let prior_run = Arc::new(Mutex::new(prior_run));
                let observed_clock = clock.clone();
                move || {
                    let breaker = breaker.clone();
                    let dial_finished = dial_finished.clone();
                    let writer_gate = writer_gate.clone();
                    let read_probe = read_probe.clone();
                    let prior_run = prior_run.clone();
                    let sent = sent.clone();
                    let observed_clock = observed_clock.clone();
                    async move {
                        let connected = TcpStream::connect((CENTRAL, LINK_PORT)).await;
                        dial_finished.send(connected.is_ok()).unwrap();
                        let stream = connected?;
                        let observer = sent.map(|trace| {
                            SocketObserver::new(
                                trace,
                                stream.local_addr().unwrap().port(),
                                observed_clock,
                            )
                        });
                        let (reader, writer) = stream.into_split();
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
                                observer,
                                registration_preface: prior_run
                                    .lock()
                                    .unwrap()
                                    .take()
                                    .map(registration_preface::RegistrationPreface::Waiting),
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
    fn registration_target(
        &self,
        _binding: &u8,
    ) -> Result<switchboard_central_link::lease_source::ApprovalServiceTarget, LeaseFailure> {
        let organization_id = self.central.organization_id.clone();
        Ok(switchboard_central_link::lease_source::ApprovalServiceTarget { organization_id })
    }

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
struct ReadProbe {
    report: Arc<Mutex<Option<ReadBytesSender>>>,
    discard: Arc<Mutex<Option<DiscardedRead>>>,
    replay: Arc<Mutex<DeferredRead>>,
}

struct DiscardedRead {
    matches: fn(&[u8]) -> bool,
    report: ReadBytesSender,
}

#[derive(Default)]
struct DeferredRead {
    bytes: VecDeque<u8>,
    waker: Option<Waker>,
}

impl ReadProbe {
    fn observe_next_read_turn(&self) -> oneshot::Receiver<Vec<u8>> {
        let (report, observed) = oneshot::channel();
        assert!(self.report.lock().unwrap().replace(report).is_none());
        observed
    }

    fn discard_next_read(&self) -> oneshot::Receiver<Vec<u8>> {
        self.discard_matching_read(|_| true)
    }

    fn discard_matching_read(&self, matches: fn(&[u8]) -> bool) -> oneshot::Receiver<Vec<u8>> {
        let (report, observed) = oneshot::channel();
        assert!(
            self.discard
                .lock()
                .unwrap()
                .replace(DiscardedRead { matches, report })
                .is_none()
        );
        observed
    }

    fn replay_read(&self, bytes: Vec<u8>) {
        let mut replay = self.replay.lock().unwrap();
        assert!(replay.bytes.is_empty());
        replay.bytes.extend(bytes);
        if let Some(waker) = replay.waker.take() {
            waker.wake();
        }
    }
}

struct ResettableRead<Reader> {
    reader: Reader,
    reset: oneshot::Receiver<()>,
    read_probe: ReadProbe,
    read_to_report: Option<DriverReadReport>,
}

impl<Reader> ResettableRead<Reader> {
    fn note_read(&mut self, bytes: &[u8]) {
        if !bytes.is_empty() {
            let report = self.read_probe.report.lock().unwrap().take();
            if let Some(report) = report {
                self.read_to_report = Some((report, bytes.to_vec()));
            }
        }
    }
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
        let replayed = {
            let mut replay = self.read_probe.replay.lock().unwrap();
            if !replay.bytes.is_empty() {
                let length = replay.bytes.len().min(buffer.remaining());
                buffer.put_slice(replay.bytes.make_contiguous().get(..length).unwrap());
                replay.bytes.drain(..length);
                true
            } else {
                replay.waker = Some(context.waker().clone());
                false
            }
        };
        if replayed {
            self.note_read(buffer.filled().get(before..).unwrap());
            return Poll::Ready(Ok(()));
        }
        let discard_armed = self.read_probe.discard.lock().unwrap().is_some();
        if discard_armed {
            let mut bytes = [0; 4096];
            let capacity = bytes.len().min(buffer.remaining());
            let mut held = ReadBuf::new(bytes.get_mut(..capacity).unwrap());
            let read = Pin::new(&mut self.reader).poll_read(context, &mut held);
            if matches!(read, Poll::Ready(Ok(()))) && !held.filled().is_empty() {
                let discarded = {
                    let mut discard = self.read_probe.discard.lock().unwrap();
                    if discard
                        .as_ref()
                        .is_some_and(|watch| (watch.matches)(held.filled()))
                    {
                        discard.take()
                    } else {
                        None
                    }
                };
                if let Some(discarded) = discarded {
                    discarded.report.send(held.filled().to_vec()).unwrap();
                    context.waker().wake_by_ref();
                    return Poll::Pending;
                }
                buffer.put_slice(held.filled());
                self.note_read(held.filled());
            }
            return read;
        }
        let read = Pin::new(&mut self.reader).poll_read(context, buffer);
        if let Poll::Ready(Ok(())) = read
            && let Some(bytes) = buffer.filled().get(before..)
        {
            self.note_read(bytes);
        }
        read
    }
}

fn run_link_case<Case, Running>(case: Case)
where
    Case: FnOnce(SimulatedLink) -> Running + 'static,
    Running: Future<Output = ()> + 'static,
{
    run_link_case_with_network(PacketFlight::new(), case);
}

fn run_link_case_with_network<Case, Running>(network: PacketFlight, case: Case)
where
    Case: FnOnce(SimulatedLink) -> Running + 'static,
    Running: Future<Output = ()> + 'static,
{
    run_link_case_with_trace(network, None, case);
}

fn run_link_case_with_trace<Case, Running>(
    network: PacketFlight,
    trace: Option<SocketFrameTrace>,
    case: Case,
) where
    Case: FnOnce(SimulatedLink) -> Running + 'static,
    Running: Future<Output = ()> + 'static,
{
    run_native_central_case(network, trace, |clock, network, fixture| async move {
        case(SimulatedLink::start(clock, network, fixture).await).await;
    });
}

fn run_native_central_case<Case, Running>(
    network: PacketFlight,
    trace: Option<SocketFrameTrace>,
    case: Case,
) where
    Case: FnOnce(Arc<ManualClock>, PacketFlight, Arc<MachineLinkFixture>) -> Running + 'static,
    Running: Future<Output = ()> + 'static,
{
    let clock = Arc::new(ManualClock::new("2030-03-17T17:46:40Z".parse().unwrap()));
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
                trace.clone(),
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
            case(clock, network, fixture).await;
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
