//! One-shot test barriers at the actual opening effect boundaries.

use super::OwnerAcpWire;
use crate::acp_transport::TransportRun;
use crate::owner_opening_disposal::{
    OpeningDisposalKind, OpeningDisposalProbe, OpeningDisposalToken,
};
use std::{
    collections::BTreeMap,
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
};
use tokio::sync::oneshot;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum OpeningPausePoint {
    BeforeCloseOffer,
    BeforeCloseCommit,
    RestoreDeferred,
    RestoreReceiptBuffered,
    ConfigureReceiptBuffered,
    ResumeReaderBeforeCommit,
    CloseReceiptPublished,
    ReaderHandoffAccepted,
    BeforeFailedHandoffCommit,
    CreatedUpdateIngress,
    WriterStopDelivered,
    BeforeSetterTake,
    AfterSetterTake,
    ReaderStoppedDelivered,
    BeforeReadmitWire,
}

pub struct OpeningPause {
    pub reached: oneshot::Receiver<()>,
    pub release: mpsc::Sender<()>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OpeningReaderEnd {
    Retired,
    Finished,
    Failed,
    IngressClosed,
    HandoffClosed,
}

#[derive(Default)]
pub(super) struct OpeningTestControls {
    pauses: Mutex<BTreeMap<OpeningPausePoint, Barrier>>,
    disposals: Mutex<BTreeMap<OpeningDisposalKind, OpeningDisposalProbe>>,
    reader_ending: Mutex<Option<oneshot::Sender<(TransportRun, OpeningReaderEnd)>>>,
    close_restored_reader_start: AtomicBool,
}

struct Barrier {
    reached: oneshot::Sender<()>,
    release: mpsc::Receiver<()>,
}

impl OwnerAcpWire {
    pub fn observe_next_reader_ending_for_test(
        &self,
    ) -> oneshot::Receiver<(TransportRun, OpeningReaderEnd)> {
        let (receipt, observed) = oneshot::channel();
        let prior = self
            .opening_test
            .reader_ending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .replace(receipt);
        assert!(prior.is_none(), "one pending reader-ending observation");
        observed
    }

    pub(crate) fn observe_reader_ending_for_test(&self, run: TransportRun, end: OpeningReaderEnd) {
        let receipt = self
            .opening_test
            .reader_ending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(receipt) = receipt {
            let _test_gone = receipt.send((run, end));
        }
    }

    pub fn close_next_restored_reader_start_for_test(&self) {
        let old = self
            .opening_test
            .close_restored_reader_start
            .swap(true, Ordering::AcqRel);
        assert!(!old, "one pending restored reader handoff closure");
    }

    pub(crate) fn take_restored_start_closure_for_test(&self) -> bool {
        self.opening_test
            .close_restored_reader_start
            .swap(false, Ordering::AcqRel)
    }

    #[cfg(feature = "test-observation")]
    pub fn stop_ordered_writer_for_test(
        &self,
        run: crate::acp_transport::TransportRun,
    ) -> Result<(), crate::acp_transport::TransportRefusal> {
        self.transport
            .enqueue_control(run, super::AcpFrame::FixtureStopWriter)
    }

    pub fn watch_opening_disposal_for_test(
        &self,
        kind: OpeningDisposalKind,
    ) -> OpeningDisposalProbe {
        let probe = OpeningDisposalProbe::default();
        let previous = self
            .opening_test
            .disposals
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(kind, probe.clone());
        assert!(
            previous.is_none(),
            "one pending witness per opening resource kind"
        );
        probe
    }

    pub(crate) fn take_opening_disposal_for_test<T>(
        &self,
        kind: OpeningDisposalKind,
        table: &Mutex<T>,
    ) -> Option<OpeningDisposalToken> {
        self.opening_test
            .disposals
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&kind)
            .map(|probe| probe.bind(table))
    }

    #[cfg(feature = "test-observation")]
    pub fn opening_route_is_registered_for_test(
        &self,
        run: crate::acp_transport::TransportRun,
    ) -> bool {
        self.transport.opening_route_is_registered_for_test(run)
    }

    #[cfg(feature = "test-observation")]
    pub fn replace_closing_marker_for_test(
        &self,
        run: crate::acp_transport::TransportRun,
        previous: provider_session_machines::session_lifecycle_machine::CloseAttempt,
        replacement: provider_session_machines::session_lifecycle_machine::CloseAttempt,
    ) -> bool {
        self.transport
            .replace_closing_marker_for_test(run, previous, replacement)
    }

    pub fn pause_opening_for_test(&self, point: OpeningPausePoint) -> OpeningPause {
        let (signal, reached) = oneshot::channel();
        let (release, wait) = mpsc::channel();
        let previous = self
            .opening_test
            .pauses
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
            .pauses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&point);
        if let Some(barrier) = barrier {
            let _test_gone = barrier.reached.send(());
            let _test_gone = barrier.release.recv();
        }
    }
}
