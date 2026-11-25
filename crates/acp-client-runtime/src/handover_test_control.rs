//! Deterministic test pauses for the ordered writer's last question and publication.

use std::sync::{Mutex, PoisonError, mpsc};
use switchboard_harness_contracts::{DispatchAuthorization, InputId};
use tokio::sync::oneshot;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HandoverPausePoint {
    BeforeOffer,
    BeforeQuestion,
    BeforePublication,
}

pub struct HandoverPause {
    pub reached: oneshot::Receiver<DispatchAuthorization>,
    pub release: mpsc::Sender<()>,
}

struct ArmedHandoverPause {
    point: HandoverPausePoint,
    input: InputId,
    reached: oneshot::Sender<DispatchAuthorization>,
    release: mpsc::Receiver<()>,
}

#[derive(Default)]
pub(crate) struct HandoverTestControl(Mutex<Option<ArmedHandoverPause>>);

impl HandoverTestControl {
    pub(crate) fn arm(&self, point: HandoverPausePoint, input: InputId) -> HandoverPause {
        let (send_reached, reached) = oneshot::channel();
        let (release, wait_for_release) = mpsc::channel();
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .replace(ArmedHandoverPause {
                point,
                input,
                reached: send_reached,
                release: wait_for_release,
            });
        HandoverPause { reached, release }
    }

    pub(crate) fn reach(&self, point: HandoverPausePoint, authorization: &DispatchAuthorization) {
        let armed = {
            let mut pause = self.0.lock().unwrap_or_else(PoisonError::into_inner);
            let matched = pause
                .as_ref()
                .is_some_and(|pause| pause.point == point && pause.input == authorization.input_id);
            if matched { pause.take() } else { None }
        };
        if let Some(armed) = armed
            && armed.reached.send(authorization.clone()).is_ok()
        {
            let _released = armed.release.recv();
        }
    }
}
