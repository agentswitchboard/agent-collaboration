//! Run owned opening effects after their row has been committed.

use super::{DeferredRestore, Effect, OwnerAcpWire, RestoreDecision};

impl Effect {
    pub(in super::super) fn faulted(&self) -> bool {
        match self {
            Self::Fault(_) => true,
            Self::DropSignal(_, next) | Self::DiscardAfter(_, next) => next.faulted(),
            Self::OfferCreate(_)
            | Self::OfferRestore(_)
            | Self::Discarded(_)
            | Self::ReleaseBefore(..)
            | Self::Readmit(..)
            | Self::Rollback(_)
            | Self::None
            | Self::Configure { .. }
            | Self::Settings(_)
            | Self::RefuseSetter(_)
            | Self::ReserveSetter(_)
            | Self::WriteSetter(_)
            | Self::WithdrawSetters(..)
            | Self::StartReader(_)
            | Self::CloseOffer(..)
            | Self::CloseConfirmed(..)
            | Self::Deferred
            | Self::ResolveDeferredRestore { .. }
            | Self::ResolveRestore { .. }
            | Self::Release(_)
            | Self::ReaderEnded { .. } => false,
        }
    }

    pub(in super::super) fn fence(&self) {
        match self {
            Self::WithdrawSetters(setters, next) => {
                for setter in setters {
                    let _taken = setter.claim.was_taken_or_withdraw();
                }
                next.fence();
            }
            Self::Fault(fault) => fault.fence(),
            Self::ReaderEnded { release, .. }
            | Self::Release(release)
            | Self::ResolveDeferredRestore { release, .. }
            | Self::ResolveRestore {
                release: Some(release),
                ..
            } if release.retire => release.state.mark_retired(),
            Self::DropSignal(_, next)
            | Self::DiscardAfter(_, next)
            | Self::ReleaseBefore(_, next) => next.fence(),
            Self::OfferCreate(_)
            | Self::OfferRestore(_)
            | Self::Discarded(_)
            | Self::Readmit(..)
            | Self::Rollback(_)
            | Self::None
            | Self::Configure { .. }
            | Self::Settings(_)
            | Self::RefuseSetter(_)
            | Self::ReserveSetter(_)
            | Self::WriteSetter(_)
            | Self::StartReader(_)
            | Self::CloseOffer(..)
            | Self::CloseConfirmed(..)
            | Self::Deferred
            | Self::ResolveDeferredRestore { .. }
            | Self::ResolveRestore { .. }
            | Self::Release(_)
            | Self::ReaderEnded { .. } => {}
        }
    }

    pub(in super::super) fn finish(self, wire: &OwnerAcpWire) -> bool {
        match self {
            Self::WithdrawSetters(setters, next) => {
                drop(setters);
                next.finish(wire)
            }
            Self::Fault(fault) => {
                fault.release(wire);
                false
            }
            Self::None | Self::Rollback(None) => true,
            Self::ReaderEnded { release, .. } | Self::Release(release) => {
                release.run(wire);
                true
            }
            Self::CloseConfirmed(release, deferred) => {
                release.run(wire);
                if let Some(deferred) = deferred {
                    deferred.outcome.send(RestoreDecision::Superseded);
                }
                true
            }
            Self::ResolveDeferredRestore {
                deferred,
                decision,
                release,
            } => {
                let DeferredRestore { milestone, outcome } = deferred;
                drop(milestone);
                outcome.send(decision);
                release.run(wire);
                true
            }
            Self::ResolveRestore {
                outcome,
                decision,
                release,
            } => {
                if let Some(release) = release {
                    release.run(wire);
                }
                outcome.send(decision);
                true
            }
            Self::DropSignal(signal, next) => {
                drop(signal);
                next.finish(wire)
            }
            Self::Discarded(command) => {
                drop(command);
                false
            }
            Self::DiscardAfter(command, next) => {
                drop(command);
                next.finish(wire)
            }
            Self::ReleaseBefore(previous, next) => {
                drop(previous);
                next.finish(wire)
            }
            Self::Readmit(_, _) => false,
            Self::Deferred
            | Self::Rollback(Some(_))
            | Self::Configure { .. }
            | Self::Settings(_)
            | Self::RefuseSetter(_)
            | Self::ReserveSetter(_)
            | Self::WriteSetter(_)
            | Self::StartReader(_)
            | Self::CloseOffer(_, _)
            | Self::OfferCreate(_)
            | Self::OfferRestore(_) => false,
        }
    }
}
