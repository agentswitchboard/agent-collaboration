use switchboard_message_custody::custody_task::CustodyTaskHandle;
use tokio::sync::watch;

use crate::FrontRefusal;

/// Entry to the custody tool calls for this collaboration runtime.
#[derive(Clone)]
pub struct MessageFront(watch::Receiver<Result<CustodyTaskHandle, FrontRefusal>>);

pub(crate) type FrontPublication = watch::Sender<Result<CustodyTaskHandle, FrontRefusal>>;

impl MessageFront {
    pub(crate) fn refusing(reason: FrontRefusal) -> (FrontPublication, Self) {
        let (publication, front) = watch::channel(Err(reason));
        (publication, Self(front))
    }

    /// Returns custody's own reserve-then-commit door, with its admission checks.
    pub fn custody(&self) -> Result<CustodyTaskHandle, FrontRefusal> {
        self.0.borrow().clone()
    }
}
