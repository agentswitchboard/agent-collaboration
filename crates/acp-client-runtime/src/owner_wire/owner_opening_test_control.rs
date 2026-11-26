//! One-shot test barriers at the actual opening effect boundaries.

use super::OwnerAcpWire;
use std::{
    collections::BTreeMap,
    sync::{Mutex, mpsc},
};
use tokio::sync::oneshot;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum OpeningPausePoint {
    BeforeCloseOffer,
    BeforeCloseCommit,
    RestoreDeferred,
    ResumeReaderBeforeCommit,
    CloseReceiptPublished,
    ReaderHandoffAccepted,
    CreatedUpdateIngress,
    WriterStopDelivered,
    BeforeSetterTake,
    AfterSetterTake,
    ReaderStoppedDelivered,
}

pub struct OpeningPause {
    pub reached: oneshot::Receiver<()>,
    pub release: mpsc::Sender<()>,
}

#[derive(Default)]
pub(super) struct OpeningTestControls(Mutex<BTreeMap<OpeningPausePoint, Barrier>>);

struct Barrier {
    reached: oneshot::Sender<()>,
    release: mpsc::Receiver<()>,
}

impl OwnerAcpWire {
    pub fn pause_opening_for_test(&self, point: OpeningPausePoint) -> OpeningPause {
        let (signal, reached) = oneshot::channel();
        let (release, wait) = mpsc::channel();
        let previous = self
            .opening_test
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                point,
                Barrier {
                    reached: signal,
                    release: wait,
                },
            );
        assert!(previous.is_none(), "one live barrier per opening effect");
        OpeningPause { reached, release }
    }

    pub(crate) fn at_opening_effect_for_test(&self, point: OpeningPausePoint) {
        let barrier = self
            .opening_test
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&point);
        if let Some(barrier) = barrier {
            let _test_gone = barrier.reached.send(());
            let _test_gone = barrier.release.recv();
        }
    }
}
