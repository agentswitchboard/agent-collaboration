use super::{
    Call, CallKey, CapacityRefusalExposure, CentralDuty, Effects, FamilyStopCause, LinkAttempt,
    LocalNegative, MatchedRunRefusal, ProviderReply, RaiseRefusal, RunAuthority, WaitEffect,
    WaitError, WaitRefusal, WaitState,
};
use approval_request_protocol::{RaiseReply, RunRefusal, StopReply, StopRequest};
use state_machine_core::TransitionError;

impl<Responder: Clone + Eq, Connection: Clone + Eq> WaitState<Responder, Connection> {
    pub(super) fn publish<Enrollment>(
        &mut self,
        authority: RunAuthority<Enrollment>,
        call: CallKey<Connection>,
    ) -> Result<WaitEffect<Responder, Connection, Enrollment>, WaitError> {
        match &mut self.central {
            CentralDuty::Unsent | CentralDuty::Raising(Call::Due) => {
                let frozen = self.frozen.as_ref().ok_or(TransitionError::Stale)?;
                self.central = CentralDuty::Raising(Call::InFlight(call.clone()));
                Ok(WaitEffect::Raise {
                    call,
                    authority,
                    request: frozen.body.clone(),
                })
            }
            CentralDuty::Stopping {
                request,
                call: pending @ Call::Due,
            } => {
                *pending = Call::InFlight(call.clone());
                Ok(WaitEffect::Stop {
                    call,
                    authority,
                    request: request.clone(),
                })
            }
            CentralDuty::Raising(Call::InFlight(_))
            | CentralDuty::WaitingForEnding
            | CentralDuty::AwaitingRaiseDisposition { .. }
            | CentralDuty::CapacityRefusalExposure(_)
            | CentralDuty::Stopping {
                call: Call::InFlight(_),
                ..
            }
            | CentralDuty::Refusing
            | CentralDuty::Done => Err(TransitionError::Stale),
        }
    }

    pub(super) fn begin_stop(&mut self, cause: FamilyStopCause) -> Result<(), WaitError> {
        match &self.central {
            CentralDuty::Unsent => {
                self.central = CentralDuty::Refusing;
                return Ok(());
            }
            CentralDuty::Raising(Call::Due) if self.earlier_unresolved_raises.is_empty() => {
                self.central = CentralDuty::Refusing;
                return Ok(());
            }
            CentralDuty::Raising(_)
            | CentralDuty::WaitingForEnding
            | CentralDuty::CapacityRefusalExposure(_) => {}
            CentralDuty::AwaitingRaiseDisposition { .. }
            | CentralDuty::Stopping { .. }
            | CentralDuty::Refusing
            | CentralDuty::Done => return Err(TransitionError::Stale),
        }
        let request = self.stop_request(cause)?;
        if let CentralDuty::Raising(Call::InFlight(raise)) = &self.central
            && self.earlier_unresolved_raises.is_empty()
        {
            self.central = CentralDuty::AwaitingRaiseDisposition {
                raise: raise.clone(),
                stop: request,
            };
        } else {
            self.preserve_raising_call();
            self.owe_stop(request);
        }
        Ok(())
    }

    fn stop_request(&self, cause: FamilyStopCause) -> Result<StopRequest, WaitError> {
        let frozen = self.frozen.as_ref().ok_or(TransitionError::Stale)?;
        let request_id = frozen.body.request_id().clone();
        let router_run = frozen.body.router_run().clone();
        let contents_digest = frozen.digest.clone();
        Ok(match cause {
            FamilyStopCause::Call(cause) => StopRequest::UnprovenCall {
                request_id,
                router_run,
                contents_digest,
                cause,
            },
            FamilyStopCause::Provider(cause) => StopRequest::Provider {
                request_id,
                router_run,
                contents_digest,
                cause,
            },
        })
    }

    fn owe_stop(&mut self, request: StopRequest) {
        self.earlier_unresolved_raises.clear();
        self.central = CentralDuty::Stopping {
            request,
            call: Call::Due,
        };
    }

    pub(super) fn stop_after_raise_proven_by_ending(&mut self) {
        if let CentralDuty::AwaitingRaiseDisposition { raise, stop } = &self.central {
            self.displaced_raise = Some(raise.clone());
            self.owe_stop(stop.clone());
        }
    }

    pub(super) fn preserve_raising_call(&mut self) {
        if let CentralDuty::Raising(Call::InFlight(call)) = &self.central {
            self.displaced_raise = Some(call.clone());
        }
    }

    pub(super) fn settle_unanswered(
        &mut self,
        key: &CallKey<Connection>,
        may_have_reached: bool,
    ) -> Result<(), WaitError> {
        match &self.central {
            CentralDuty::Raising(Call::InFlight(raise)) if raise == key => {
                if may_have_reached && !self.earlier_unresolved_raises.contains(key) {
                    self.earlier_unresolved_raises.push(key.clone());
                }
                self.central = CentralDuty::Raising(Call::Due);
                Ok(())
            }
            CentralDuty::AwaitingRaiseDisposition { raise, stop } if raise == key => {
                if may_have_reached {
                    self.owe_stop(stop.clone());
                } else {
                    self.central = CentralDuty::Refusing;
                }
                Ok(())
            }
            _ => {
                if !may_have_reached && self.resolve_refused_raise_attempt(key) {
                    Ok(())
                } else {
                    self.retry_stop_or_drain_raise(|call| call == key)
                }
            }
        }
    }

    pub(super) fn retire_connection(
        &mut self,
        link: &LinkAttempt<Connection>,
    ) -> Result<(), WaitError> {
        self.retry_stop_or_drain_raise(|call| &call.link == link)
    }

    fn retry_stop_or_drain_raise(
        &mut self,
        matches_call: impl Fn(&CallKey<Connection>) -> bool,
    ) -> Result<(), WaitError> {
        let displaced = self.displaced_raise.as_ref().is_some_and(&matches_call);
        if displaced {
            self.displaced_raise = None;
        }
        match &mut self.central {
            CentralDuty::Stopping { call, .. } if matches!(&*call, Call::InFlight(key) if matches_call(key)) =>
            {
                *call = Call::Due;
                Ok(())
            }
            _ if displaced => Ok(()),
            _ => Err(TransitionError::Stale),
        }
    }

    pub(super) fn receive_raise<Enrollment>(
        &mut self,
        key: CallKey<Connection>,
        reply: RaiseReply,
        effects: &mut Effects<Responder, Connection, Enrollment>,
    ) -> Result<(), WaitError> {
        if self.displaced_raise.as_ref() == Some(&key) {
            self.displaced_raise = None;
            // Local Stop or an ending can supersede the Raise's local duty
            // while the independently correlated authority signal is in flight.
            if let RaiseReply::AuthorityRefused { refusal } = reply {
                self.report_refusal(key, refusal, effects)?;
            }
            return Ok(());
        }
        if self.receive_refused_raise_exposure(&key, &reply)? {
            return Ok(());
        }
        let pending_stop = match &self.central {
            CentralDuty::Raising(Call::InFlight(current)) if current == &key => None,
            CentralDuty::AwaitingRaiseDisposition { raise, stop } if raise == &key => Some(stop),
            _ => return Err(TransitionError::Stale),
        };
        let refusal = match reply {
            RaiseReply::Receipt { receipt } => {
                self.check_raise_receipt(&receipt)?;
                if let Some(stop) = pending_stop {
                    self.owe_stop(stop.clone());
                } else {
                    self.earlier_unresolved_raises.clear();
                    self.central = CentralDuty::WaitingForEnding;
                }
                return Ok(());
            }
            RaiseReply::AuthorityRefused { refusal } => {
                self.report_refusal(key, refusal, effects)?;
                self.central = if pending_stop.is_some() {
                    CentralDuty::Refusing
                } else {
                    CentralDuty::Raising(Call::Due)
                };
                return Ok(());
            }
            RaiseReply::AlreadyStopped {} => RaiseRefusal::AlreadyStopped,
            RaiseReply::SessionEnded {} => RaiseRefusal::SessionEnded,
            RaiseReply::ContentsDiffer {} => RaiseRefusal::ContentsDiffer,
            RaiseReply::TooManyOutstanding {} => RaiseRefusal::TooManyOutstanding,
        };
        if pending_stop.is_none() {
            self.claim_available(ProviderReply::LocalNegative(LocalNegative::RaiseRefused(
                refusal,
            )))?;
        }
        self.central = if refusal == RaiseRefusal::TooManyOutstanding
            && !self.earlier_unresolved_raises.is_empty()
        {
            CentralDuty::CapacityRefusalExposure(CapacityRefusalExposure::UnresolvedAttempts(
                std::mem::take(&mut self.earlier_unresolved_raises),
            ))
        } else {
            CentralDuty::Refusing
        };
        Ok(())
    }

    fn check_raise_receipt(
        &self,
        receipt: &approval_request_protocol::RaiseReceipt,
    ) -> Result<(), WaitError> {
        if self.request_id.as_ref() == Some(&receipt.request_id) {
            Ok(())
        } else {
            Err(TransitionError::Domain(
                WaitRefusal::ReceiptForDifferentRequest,
            ))
        }
    }

    fn receive_refused_raise_exposure(
        &mut self,
        key: &CallKey<Connection>,
        reply: &RaiseReply,
    ) -> Result<bool, WaitError> {
        let CentralDuty::CapacityRefusalExposure(CapacityRefusalExposure::UnresolvedAttempts(
            attempts,
        )) = &self.central
        else {
            return Ok(false);
        };
        if !attempts.contains(key) {
            return Ok(false);
        }
        match reply {
            RaiseReply::Receipt { receipt } => {
                self.check_raise_receipt(receipt)?;
                self.central = CentralDuty::CapacityRefusalExposure(
                    CapacityRefusalExposure::ConfirmedAdmission,
                );
            }
            RaiseReply::AlreadyStopped {} => self.central = CentralDuty::Refusing,
            RaiseReply::SessionEnded {}
            | RaiseReply::ContentsDiffer {}
            | RaiseReply::TooManyOutstanding {}
            | RaiseReply::AuthorityRefused { .. } => {
                self.resolve_refused_raise_attempt(key);
            }
        }
        Ok(true)
    }

    fn resolve_refused_raise_attempt(&mut self, key: &CallKey<Connection>) -> bool {
        let CentralDuty::CapacityRefusalExposure(CapacityRefusalExposure::UnresolvedAttempts(
            attempts,
        )) = &mut self.central
        else {
            return false;
        };
        let Some(index) = attempts.iter().position(|attempt| attempt == key) else {
            return false;
        };
        attempts.remove(index);
        if attempts.is_empty() {
            self.central = CentralDuty::Refusing;
        }
        true
    }

    pub(super) fn receive_stop<Enrollment>(
        &mut self,
        key: CallKey<Connection>,
        reply: StopReply,
        effects: &mut Effects<Responder, Connection, Enrollment>,
    ) -> Result<(), WaitError> {
        let CentralDuty::Stopping {
            request:
                StopRequest::Provider {
                    request_id,
                    contents_digest,
                    ..
                }
                | StopRequest::UnprovenCall {
                    request_id,
                    contents_digest,
                    ..
                },
            call: Call::InFlight(current),
        } = &self.central
        else {
            return Err(TransitionError::Stale);
        };
        if current != &key {
            return Err(TransitionError::Stale);
        }
        match reply {
            StopReply::AuthorityRefused { refusal } => {
                self.retry_stop_or_drain_raise(|call| call == &key)?;
                self.report_refusal(key, refusal, effects)?;
            }
            StopReply::Stands { ending } => {
                self.central = CentralDuty::Done;
                effects.push(WaitEffect::StopReconciled(ending));
            }
            StopReply::NotVisible {} => {
                effects.push(WaitEffect::StopNotVisible {
                    request_id: request_id.clone(),
                    contents_digest: contents_digest.clone(),
                });
                self.central = CentralDuty::Done;
            }
        }
        Ok(())
    }

    fn report_refusal<Enrollment>(
        &self,
        call: CallKey<Connection>,
        refusal: RunRefusal,
        effects: &mut Effects<Responder, Connection, Enrollment>,
    ) -> Result<(), WaitError> {
        let router_run = self.router_run.clone().ok_or(TransitionError::Stale)?;
        effects.push(WaitEffect::RunRefused(MatchedRunRefusal {
            call,
            router_run,
            refusal,
        }));
        Ok(())
    }
}
