//! Poll incoming frames, lease results and user commands in the socket's current order.

use std::future::{Future, pending};

use central_link_client::ConnectionAttempt;
use futures_util::{FutureExt, Stream, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::{Error as WebSocketError, Message};

use crate::{
    admitted_lane_socket::{EventPermit, ReadAdmission},
    central_call_lane::CentralCallHandle,
    lease_source::{LeaseFailure, LeaseFuture, LinkLease},
    link_lane::LaneRequest,
};

pub(super) enum SocketInput<Binding> {
    Timer,
    Frame(Option<Result<Message, WebSocketError>>, Option<EventPermit>),
    EventSpace,
    Lease(Result<LinkLease<Binding>, LeaseFailure>),
    Command(LaneRequest),
    Call,
}

#[derive(Clone, Copy)]
pub(super) enum SocketPriority {
    ReadFrame,
    AcquireLease,
    AcceptCommand,
    AcceptCall,
}

pub(super) async fn next_socket_input<Binding>(
    socket: &mut (impl Stream<Item = Result<Message, WebSocketError>> + Unpin),
    acquisition: &mut Option<LeaseFuture<'_, Binding>>,
    commands: &mut mpsc::Receiver<LaneRequest>,
    can_offer: bool,
    read_admission: ReadAdmission,
    priority: SocketPriority,
    calls: Option<(&CentralCallHandle, &ConnectionAttempt)>,
) -> SocketInput<Binding> {
    let reading = read_socket(socket, read_admission);
    let leasing = acquire_if_pending(acquisition).map(SocketInput::Lease);
    let command = receive_command(commands, can_offer).map(SocketInput::Command);
    let call = async {
        match calls {
            Some((lane, attempt)) => lane.ready_to_write(attempt).await,
            None => pending().await,
        }
        SocketInput::Call
    };
    match priority {
        SocketPriority::ReadFrame => poll_in_order(reading, leasing, command, call).await,
        SocketPriority::AcquireLease => poll_in_order(leasing, command, call, reading).await,
        SocketPriority::AcceptCommand => poll_in_order(command, call, reading, leasing).await,
        SocketPriority::AcceptCall => poll_in_order(call, reading, leasing, command).await,
    }
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

async fn receive_command(
    commands: &mut mpsc::Receiver<LaneRequest>,
    can_offer: bool,
) -> LaneRequest {
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
