use super::{
    Call, CallKey, CentralDuty, Effects, LinkAttempt, LocalNegative, MatchedRunRefusal,
    ProviderReply, RaiseRefusal, RunAuthority, WaitEffect, WaitError, WaitRefusal, WaitState,
};
use approval_request_protocol::{
    ProviderStopCause, RaiseReply, RunRefusal, StopReply, StopRequest,
};
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
            | CentralDuty::Stopping {
                call: Call::InFlight(_),
                ..
            }
            | CentralDuty::Refusing
            | CentralDuty::Done => Err(TransitionError::Stale),
        }
    }

    pub(super) fn begin_stop(&mut self, cause: ProviderStopCause) -> Result<(), WaitError> {
        let frozen = self.frozen.as_ref().ok_or(TransitionError::Stale)?;
        let request = StopRequest::Provider {
            request_id: frozen.body.request_id().clone(),
            router_run: frozen.body.router_run().clone(),
            contents_digest: frozen.digest.clone(),
            cause,
        };
        self.preserve_raising_call();
        self.central = CentralDuty::Stopping {
            request,
            call: Call::Due,
        };
        Ok(())
    }

    pub(super) fn preserve_raising_call(&mut self) {
        if let CentralDuty::Raising(Call::InFlight(call)) = &self.central {
            self.displaced_raise = Some(call.clone());
        }
    }

    pub(super) fn end_call(&mut self, key: &CallKey<Connection>) -> Result<(), WaitError> {
        self.end_matching_calls(|call| call == key)
    }

    pub(super) fn retire_connection(
        &mut self,
        link: &LinkAttempt<Connection>,
    ) -> Result<(), WaitError> {
        self.end_matching_calls(|call| &call.link == link)
    }

    fn end_matching_calls(
        &mut self,
        matches_call: impl Fn(&CallKey<Connection>) -> bool,
    ) -> Result<(), WaitError> {
        let displaced = self.displaced_raise.as_ref().is_some_and(&matches_call);
        if displaced {
            self.displaced_raise = None;
        }
        match &mut self.central {
            CentralDuty::Raising(call) | CentralDuty::Stopping { call, .. } if matches!(&*call, Call::InFlight(key) if matches_call(key)) =>
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
        if !matches!(&self.central, CentralDuty::Raising(Call::InFlight(current)) if current == &key)
        {
            return Err(TransitionError::Stale);
        }
        let refusal = match reply {
            RaiseReply::Receipt { receipt } => {
                if self.request_id.as_ref() != Some(&receipt.request_id) {
                    return Err(TransitionError::Domain(
                        WaitRefusal::ReceiptForDifferentRequest,
                    ));
                }
                self.central = CentralDuty::WaitingForEnding;
                return Ok(());
            }
            RaiseReply::AuthorityRefused { refusal } => {
                self.report_refusal(key, refusal, effects)?;
                self.central = CentralDuty::Raising(Call::Due);
                return Ok(());
            }
            RaiseReply::AlreadyStopped {} => RaiseRefusal::AlreadyStopped,
            RaiseReply::SessionEnded {} => RaiseRefusal::SessionEnded,
            RaiseReply::ContentsDiffer {} => RaiseRefusal::ContentsDiffer,
            RaiseReply::TooManyOutstanding {} => RaiseRefusal::TooManyOutstanding,
        };
        self.claim_available(ProviderReply::LocalNegative(LocalNegative::RaiseRefused(
            refusal,
        )))?;
        self.central = CentralDuty::Refusing;
        Ok(())
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
                self.end_call(&key)?;
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
