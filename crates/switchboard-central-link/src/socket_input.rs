//! Poll incoming frames, lease results and user commands in the socket's current order.

use std::{
    future::{Future, pending, poll_fn},
    task::Poll,
};

use futures_util::{FutureExt, Stream, StreamExt};
use router_clock::ClockInstant;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::{Error as WebSocketError, Message};

use crate::{
    admitted_lane_socket::{AdmittedSocketLane, EventPermit, ReadAdmission},
    lease_source::{LeaseFailure, LeaseFuture, LinkLease},
    lifecycle_lane::LifecycleRequest,
    link_lane::{LaneRequest, LinkLaneOwner},
};

pub(super) enum SocketInput<Binding> {
    Timer,
    Frame(Option<Result<Message, WebSocketError>>, Option<EventPermit>),
    EventSpace,
    Lease(Result<LinkLease<Binding>, LeaseFailure>),
    Command(LaneRequest),
    Lifecycle {
        request: LifecycleRequest,
        write_until: ClockInstant,
    },
}

#[derive(Clone, Copy)]
pub(super) enum SocketPriority {
    ReadFrame,
    AcquireLease,
    AcceptCommand,
    AcceptLifecycle,
}

pub(super) async fn next_socket_input<Binding>(
    socket: &mut (impl Stream<Item = Result<Message, WebSocketError>> + Unpin),
    acquisition: &mut Option<LeaseFuture<'_, Binding>>,
    lanes: &mut LinkLaneOwner,
    admitted: &AdmittedSocketLane<'_>,
    priority: SocketPriority,
    lifecycle_write_window: impl Fn() -> Option<ClockInstant>,
) -> SocketInput<Binding> {
    let reading = read_socket(socket, admitted.read_admission());
    let leasing = acquire_if_pending(acquisition).map(SocketInput::Lease);
    let command =
        receive_command(&mut lanes.commands, admitted.can_offer()).map(SocketInput::Command);
    let lifecycle = receive_lifecycle(
        &mut lanes.lifecycle.commands,
        admitted.is_admitted(),
        lifecycle_write_window,
    )
    .map(|(request, write_until)| SocketInput::Lifecycle {
        request,
        write_until,
    });
    match priority {
        SocketPriority::ReadFrame => poll_in_order(reading, leasing, command, lifecycle).await,
        SocketPriority::AcquireLease => poll_in_order(leasing, command, lifecycle, reading).await,
        SocketPriority::AcceptCommand => poll_in_order(command, reading, leasing, lifecycle).await,
        SocketPriority::AcceptLifecycle => {
            poll_in_order(leasing, lifecycle, reading, command).await
        }
    }
}

async fn receive_lifecycle(
    commands: &mut mpsc::Receiver<LifecycleRequest>,
    admitted: bool,
    write_window: impl Fn() -> Option<ClockInstant>,
) -> (LifecycleRequest, ClockInstant) {
    poll_fn(|context| {
        // Eligibility can change after the driver's wait starts. Check it on
        // the actual receive poll, before taking anything from the queue.
        if admitted
            && let Some(write_until) = write_window()
            && let Poll::Ready(Some(request)) = commands.poll_recv(context)
        {
            return Poll::Ready((request, write_until));
        }
        Poll::Pending
    })
    .await
}

async fn read_socket<Binding>(
    socket: &mut (impl Stream<Item = Result<Message, WebSocketError>> + Unpin),
    admission: ReadAdmission,
) -> SocketInput<Binding> {
    match admission {
        ReadAdmission::AwaitingWelcome => SocketInput::Frame(socket.next().await, None),
        ReadAdmission::Admitted(events) => match events.reserve_owned().await {
            Ok(permit) => SocketInput::Frame(socket.next().await, Some(permit)),
            Err(_) => SocketInput::Frame(None, None),
        },
        ReadAdmission::AwaitingEventSpace { events, slots } => {
            match events.reserve_many(slots).await {
                Ok(_) => SocketInput::EventSpace,
                Err(_) => SocketInput::Frame(None, None),
            }
        }
    }
}

async fn poll_in_order<Output>(
    first: impl Future<Output = Output>,
    second: impl Future<Output = Output>,
    third: impl Future<Output = Output>,
    fourth: impl Future<Output = Output>,
) -> Output {
    tokio::select! {
        biased;
        result = first => result,
        result = second => result,
        result = third => result,
        result = fourth => result,
    }
}

async fn receive_command<Command>(
    commands: &mut mpsc::Receiver<Command>,
    can_offer: bool,
) -> Command {
    if can_offer && let Some(command) = commands.recv().await {
        return command;
    }
    pending().await
}

async fn acquire_if_pending<Binding>(
    acquisition: &mut Option<LeaseFuture<'_, Binding>>,
) -> Result<LinkLease<Binding>, LeaseFailure> {
    match acquisition {
        Some(future) => future.await,
        None => pending().await,
    }
}
