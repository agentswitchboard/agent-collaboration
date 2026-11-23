use std::{
    future::Future,
    io,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use axum::serve::Listener;
use central_link_client::{AdmittedConnection, ReadyLinkLane};
use collaboration_central_tokio::{
    WorkspaceLink,
    test_support::{MACHINE_LINK_FIXTURE_MACHINE, MachineLinkFixture, start_session_link_fixture},
};
use router_clock::{ManualClock, RuntimeClock};
use switchboard_central_link::{
    DialAddress,
    link_driver::{DriverSettings, LinkDriver},
    link_lane::{LinkLaneHandle, link_lane_channel},
    link_policy::LinkStatus,
};
use tokio::{
    io::{AsyncRead, ReadBuf},
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
};
use turmoil::net::{TcpListener, TcpStream};

use self::packet_flight::PacketFlight;
use super::RealLeases;

mod custody_cases;
mod packet_flight;

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
    central: Arc<MachineLinkFixture>,
    network: PacketFlight,
    lane: LinkLaneHandle,
    status: watch::Receiver<LinkStatus>,
    admission: AdmittedConnection,
    break_socket: oneshot::Sender<()>,
    socket_breakers: mpsc::UnboundedReceiver<oneshot::Sender<()>>,
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
        let (acquisitions, _) = mpsc::unbounded_channel();
        let (endings, _) = mpsc::unbounded_channel();
        let (breaker, mut socket_breakers) = mpsc::unbounded_channel();
        let driver = tokio::spawn(
            LinkDriver {
                machine_id: MACHINE_LINK_FIXTURE_MACHINE.parse().unwrap(),
                clock: clock.clone(),
                lease_source: Arc::new(RealLeases {
                    central: central.clone(),
                    mac_clock: clock.clone(),
                    acquisitions,
                    endings,
                }),
                settings,
                shutdown: shutdown_rx,
                status,
                lane: owner,
            }
            .run_with_test_transport(move || {
                let breaker = breaker.clone();
                async move {
                    let (reader, writer) =
                        TcpStream::connect((CENTRAL, LINK_PORT)).await?.into_split();
                    let (cut, reset) = oneshot::channel();
                    breaker.send(cut).unwrap();
                    Ok(tokio::io::join(ResettableRead { reader, reset }, writer))
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
        Self {
            clock,
            central,
            network,
            lane,
            status: link_status,
            admission,
            break_socket,
            socket_breakers,
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
        let deadline = self.clock.monotonic_now() + Duration::from_secs(1);
        self.clock.not_due_check_now(deadline).await;
        self.clock.advance(Duration::from_secs(1)).unwrap();
        self.read_replacement().await;
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
        let after = self.network.frames().len();
        turmoil::repair(MAC, CENTRAL);
        for _ in 0..12 {
            self.clock.advance(Duration::from_secs(10)).unwrap();
            // Source/host bookkeeping (including authentication) gets a
            // virtual step before we drive the next dial deadline.
            for _ in 0..25 {
                self.network.step().await;
                if self
                    .network
                    .frames()
                    .iter()
                    .skip(after)
                    .any(|frame| frame.kind == packet_flight::FrameKind::Welcome)
                {
                    self.read_replacement().await;
                    return;
                }
            }
        }
        assert!(
            self.lane.watch_admitted().borrow().is_some(),
            "Central must admit the link after the partition heals"
        );
    }

    async fn close(self) {
        self.shutdown.send_replace(true);
        self.driver.await.unwrap();
    }
}

struct ResettableRead<Reader> {
    reader: Reader,
    reset: oneshot::Receiver<()>,
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
        Pin::new(&mut self.reader).poll_read(context, buffer)
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
