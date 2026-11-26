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

#[cfg(feature = "test-observation")]
pub struct OpeningAsyncPause {
    pub reached: oneshot::Receiver<()>,
    pub release: oneshot::Sender<()>,
}

#[cfg(feature = "test-observation")]
impl OpeningAsyncPause {
    fn new() -> (Self, AsyncBarrier) {
        let (signal, reached) = oneshot::channel();
        let (release, wait) = oneshot::channel();
        (Self { reached, release }, AsyncBarrier { signal, wait })
    }
}

#[cfg(feature = "test-observation")]
pub(super) struct AsyncBarrier {
    signal: oneshot::Sender<()>,
    wait: oneshot::Receiver<()>,
}

#[cfg(feature = "test-observation")]
impl AsyncBarrier {
    pub(super) async fn pause(self) {
        let _test_gone = self.signal.send(());
        let _test_gone = self.wait.await;
    }
}

#[cfg(feature = "test-observation")]
pub struct OrderedWriterPause {
    pub before_dequeue: OpeningAsyncPause,
    pub during_drain: OpeningAsyncPause,
}

#[cfg(feature = "test-observation")]
pub struct WriterExitObservation {
    pub join_pending: oneshot::Receiver<()>,
    pub reap_started: oneshot::Receiver<()>,
}

#[cfg(feature = "test-observation")]
#[derive(Default)]
struct WriterExitSignals {
    join_pending: Option<oneshot::Sender<()>>,
    reap_started: Option<oneshot::Sender<()>>,
}

#[cfg(feature = "test-observation")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderConnectionEnd {
    ChildExited,
    SdkFailed,
    SdkReturned,
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
    #[cfg(feature = "test-observation")]
    before_input_eof: Mutex<Option<AsyncBarrier>>,
    #[cfg(feature = "test-observation")]
    provider_end: Mutex<Option<oneshot::Sender<ProviderConnectionEnd>>>,
    #[cfg(feature = "test-observation")]
    writer_exit: Mutex<WriterExitSignals>,
}

struct Barrier {
    reached: oneshot::Sender<()>,
    release: mpsc::Receiver<()>,
}

impl OwnerAcpWire {
    #[cfg(feature = "test-observation")]
    pub fn hold_ordered_writer_for_test(
        &self,
        run: TransportRun,
    ) -> Result<OrderedWriterPause, crate::acp_transport::TransportRefusal> {
        let (before_dequeue, dequeue_barrier) = OpeningAsyncPause::new();
        let (during_drain, drain_barrier) = OpeningAsyncPause::new();
        self.transport.enqueue_control(
            run,
            super::AcpFrame::FixturePauseWriter {
                before_dequeue: dequeue_barrier,
                during_drain: drain_barrier,
            },
        )?;
        Ok(OrderedWriterPause {
            before_dequeue,
            during_drain,
        })
    }

    #[cfg(feature = "test-observation")]
    pub fn observe_writer_exit_for_test(&self) -> WriterExitObservation {
        let (pending_signal, join_pending) = oneshot::channel();
        let (reap_signal, reap_started) = oneshot::channel();
        let mut signals = self
            .opening_test
            .writer_exit
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(signals.join_pending.replace(pending_signal).is_none());
        assert!(signals.reap_started.replace(reap_signal).is_none());
        WriterExitObservation {
            join_pending,
            reap_started,
        }
    }

    #[cfg(feature = "test-observation")]
    pub(crate) async fn await_observed_writer_for_test(
        &self,
        mut writer: tokio::task::JoinHandle<()>,
    ) -> Result<(), tokio::task::JoinError> {
        std::future::poll_fn(|context| {
            let result = std::future::Future::poll(std::pin::Pin::new(&mut writer), context);
            if result.is_pending() {
                let signal = self
                    .opening_test
                    .writer_exit
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .join_pending
                    .take();
                if let Some(signal) = signal {
                    let _test_gone = signal.send(());
                }
            }
            result
        })
        .await
    }

    #[cfg(feature = "test-observation")]
    pub(crate) fn publish_provider_reap_start_for_test(&self) {
        let signal = self
            .opening_test
            .writer_exit
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .reap_started
            .take();
        if let Some(signal) = signal {
            let _test_gone = signal.send(());
        }
    }

    #[cfg(feature = "test-observation")]
    pub fn hold_provider_eof_for_test(&self) -> OpeningAsyncPause {
        let (pause, barrier) = OpeningAsyncPause::new();
        let prior = self
            .opening_test
            .before_input_eof
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .replace(barrier);
        assert!(prior.is_none(), "one pending provider EOF pause");
        pause
    }

    #[cfg(feature = "test-observation")]
    pub(crate) async fn before_provider_eof_for_test(&self) {
        let barrier = self
            .opening_test
            .before_input_eof
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(barrier) = barrier {
            barrier.pause().await;
        }
    }

    #[cfg(feature = "test-observation")]
    pub fn observe_provider_end_for_test(&self) -> oneshot::Receiver<ProviderConnectionEnd> {
        let (signal, observed) = oneshot::channel();
        let prior = self
            .opening_test
            .provider_end
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .replace(signal);
        assert!(
            prior.is_none(),
            "one pending provider connection exit observation"
        );
        observed
    }

    #[cfg(feature = "test-observation")]
    pub(crate) fn publish_provider_end_for_test(&self, cause: ProviderConnectionEnd) {
        let receipt = self
            .opening_test
            .provider_end
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(receipt) = receipt {
            let _test_gone = receipt.send(cause);
        }
    }
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
