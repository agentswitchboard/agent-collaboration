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
fn arm_async_pause(slot: &Mutex<Option<AsyncBarrier>>) -> OpeningAsyncPause {
    let (pause, barrier) = OpeningAsyncPause::new();
    let prior = slot
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .replace(barrier);
    assert!(prior.is_none(), "one pending pause at this boundary");
    pause
}

#[cfg(feature = "test-observation")]
async fn pause_async_gate(slot: &Mutex<Option<AsyncBarrier>>) {
    let barrier = slot
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take();
    if let Some(barrier) = barrier {
        barrier.pause().await;
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
    #[cfg(feature = "test-observation")]
    before_taken_reply: Mutex<Option<AsyncBarrier>>,
    #[cfg(feature = "test-observation")]
    after_sdk_drop: Mutex<Option<AsyncBarrier>>,
    #[cfg(feature = "test-observation")]
    fatal_callback: tokio_util::sync::CancellationToken,
    #[cfg(feature = "test-observation")]
    transport_end: crate::provider_transport_diagnostics::TransportEndObserver,
    #[cfg(feature = "test-observation")]
    buffer_restore_success: AtomicBool,
    #[cfg(feature = "test-observation")]
    terminal_admission: super::prompt_terminal::TerminalAdmissionObserver,
    #[cfg(feature = "test-observation")]
    before_terminal_receive: Mutex<Option<AsyncBarrier>>,
    #[cfg(feature = "test-observation")]
    after_terminal_receive: Mutex<Option<AsyncBarrier>>,
}

struct Barrier {
    reached: oneshot::Sender<()>,
    release: mpsc::Receiver<()>,
}

impl OwnerAcpWire {
    #[cfg(feature = "test-observation")]
    pub(super) fn terminal_admission_observer_for_test(
        &self,
    ) -> super::prompt_terminal::TerminalAdmissionObserver {
        self.opening_test.terminal_admission.clone()
    }

    #[cfg(feature = "test-observation")]
    pub fn observe_terminal_admission_for_test(
        &self,
    ) -> oneshot::Receiver<(crate::owner_wire::TurnId, super::TerminalAdmission)> {
        self.opening_test.terminal_admission.observe()
    }

    #[cfg(feature = "test-observation")]
    pub fn hold_reader_before_terminal_receive_for_test(&self) -> OpeningAsyncPause {
        arm_async_pause(&self.opening_test.before_terminal_receive)
    }

    #[cfg(feature = "test-observation")]
    pub(super) async fn before_terminal_receive_for_test(&self) {
        pause_async_gate(&self.opening_test.before_terminal_receive).await;
    }

    #[cfg(feature = "test-observation")]
    pub fn hold_reader_with_terminal_in_hand_for_test(&self) -> OpeningAsyncPause {
        arm_async_pause(&self.opening_test.after_terminal_receive)
    }

    #[cfg(feature = "test-observation")]
    pub(super) async fn after_terminal_receive_for_test(&self) {
        pause_async_gate(&self.opening_test.after_terminal_receive).await;
    }

    #[cfg(feature = "test-observation")]
    pub fn buffer_next_restore_success_for_test(&self) {
        let already_armed = self
            .opening_test
            .buffer_restore_success
            .swap(true, Ordering::AcqRel);
        assert!(!already_armed, "one pending restore success buffer");
    }

    #[cfg(feature = "test-observation")]
    pub(crate) fn take_restore_success_buffer_for_test(&self) -> bool {
        self.opening_test
            .buffer_restore_success
            .swap(false, Ordering::AcqRel)
    }

    #[cfg(feature = "test-observation")]
    pub(crate) fn transport_end_observer_for_test(
        &self,
    ) -> crate::provider_transport_diagnostics::TransportEndObserver {
        self.opening_test.transport_end.clone()
    }

    #[cfg(feature = "test-observation")]
    pub fn observe_transport_end_for_test(&self) -> oneshot::Receiver<super::TransportEnd> {
        self.opening_test.transport_end.observe()
    }

    #[cfg(feature = "test-observation")]
    pub fn hold_taken_reply_for_test(&self) -> OpeningAsyncPause {
        arm_async_pause(&self.opening_test.before_taken_reply)
    }

    #[cfg(feature = "test-observation")]
    pub(crate) async fn before_taken_reply_for_test(&self) {
        pause_async_gate(&self.opening_test.before_taken_reply).await;
    }

    #[cfg(feature = "test-observation")]
    pub fn hold_sdk_drop_before_retirement_for_test(&self) -> OpeningAsyncPause {
        arm_async_pause(&self.opening_test.after_sdk_drop)
    }

    #[cfg(feature = "test-observation")]
    pub(crate) async fn after_sdk_drop_for_test(&self) {
        pause_async_gate(&self.opening_test.after_sdk_drop).await;
    }

    #[cfg(feature = "test-observation")]
    pub fn trigger_fatal_callback_for_test(&self) {
        self.opening_test.fatal_callback.cancel();
    }

    #[cfg(feature = "test-observation")]
    pub(crate) async fn fatal_callback_requested_for_test(&self) {
        self.opening_test.fatal_callback.cancelled().await;
    }

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
        arm_async_pause(&self.opening_test.before_input_eof)
    }

    #[cfg(feature = "test-observation")]
    pub(crate) async fn before_provider_eof_for_test(&self) {
        pause_async_gate(&self.opening_test.before_input_eof).await;
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
