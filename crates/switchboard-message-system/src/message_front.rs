use switchboard_machine_enrollment::MachineId;
use switchboard_message_custody::custody_task::CustodyTaskHandle;
use tokio::sync::watch;

use crate::FrontRefusal;

/// Entry to the custody tool calls for this collaboration runtime.
#[derive(Clone)]
pub struct MessageFront(watch::Receiver<Result<ForwardingMessageFront, FrontRefusal>>);

pub(crate) type FrontPublication = watch::Sender<Result<ForwardingMessageFront, FrontRefusal>>;

#[derive(Clone)]
pub(crate) struct ForwardingMessageFront {
    pub(crate) calls: CustodyTaskHandle,
    pub(crate) machine_id: MachineId,
}

impl MessageFront {
    pub(crate) fn refusing(reason: FrontRefusal) -> (FrontPublication, Self) {
        let (publication, front) = watch::channel(Err(reason));
        (publication, Self(front))
    }

    /// Returns custody's own reserve-then-commit door, with its admission checks.
    pub fn custody(&self) -> Result<CustodyTaskHandle, FrontRefusal> {
        self.0
            .borrow()
            .as_ref()
            .map(|front| front.calls.clone())
            .map_err(|reason| *reason)
    }

    pub(crate) fn forwarding_machine(&self) -> Result<MachineId, FrontRefusal> {
        self.0
            .borrow()
            .as_ref()
            .map(|front| front.machine_id.clone())
            .map_err(|reason| *reason)
    }
}
