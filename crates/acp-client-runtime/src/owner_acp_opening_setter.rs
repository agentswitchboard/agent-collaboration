//! The restore setter's reservation and actual ordered write.

use super::{
    Command, Effect, LifecycleRefusal, OpenAttempt, OpeningSlot, OwnerOpenings,
    lifecycle_transport_refusal, lock,
};
use provider_session_machines::harness_lifecycle::{
    RestoreSettingRefusal, SettingAttempt, SettingFact,
};
use switchboard_harness_contracts::ReplyAdmission;

impl OwnerOpenings {
    pub(in super::super) fn put_setting_back(
        &self,
        attempt: SettingAttempt,
        reserve_reply: &mut dyn FnMut() -> ReplyAdmission<SettingFact>,
    ) -> Result<(), RestoreSettingRefusal> {
        use RestoreSettingRefusal::Harness;
        self.observe_health().map_err(Harness)?;
        let identity = OpenAttempt::Restore(attempt.restore);
        let slot = OpeningSlot::for_attempt(identity);
        let session = self
            .session
            .get()
            .ok_or(Harness(LifecycleRefusal::RouteUnavailable))?;
        let (instance, planned) = {
            let mut attempts = lock(&self.attempts);
            let Some(row) = attempts.targeted(identity) else {
                return Err(Harness(self.opening_refusal(&attempts, identity)));
            };
            let instance = row.instance().clone();
            let planned = attempts.apply(
                slot,
                &instance,
                Command::PutSettingBack {
                    attempt: Box::new(attempt),
                    session: session.clone(),
                },
            );
            (instance, planned)
        };
        let claim = match super::create::require_effect(planned, &self.wire).map_err(Harness)? {
            Effect::ReserveSetter(claim) => claim,
            Effect::RefuseSetter(reason) => return Err(Harness(reason)),
            _ => return Err(Harness(LifecycleRefusal::OperationNotOffered)),
        };
        let reply = match reserve_reply() {
            ReplyAdmission::Reserved(reply) => reply,
            ReplyAdmission::Refused(reason) => {
                let removed =
                    lock(&self.attempts).apply(slot, &instance, Command::SetterDone(claim));
                if let Ok(effect) = removed {
                    effect.finish(&self.wire);
                }
                return Err(RestoreSettingRefusal::ReplyReservation(reason));
            }
        };
        #[cfg(any(test, feature = "test-observation"))]
        let reply = self.observe_reply(crate::owner_wire::OpeningDisposalKind::SetterReply, reply);
        let reserved = lock(&self.attempts).apply(
            slot,
            &instance,
            Command::SetterReserved(claim.clone(), reply),
        );
        let Effect::WriteSetter(write) =
            super::create::require_effect(reserved, &self.wire).map_err(Harness)?
        else {
            return Err(Harness(LifecycleRefusal::OperationNotOffered));
        };
        #[cfg(any(test, feature = "test-observation"))]
        self.wire
            .at_opening_effect_for_test(crate::owner_wire::OpeningPausePoint::BeforeSetterTake);
        // Once enqueue succeeds the ordered carrier owns the reserved reply.
        // SetterDone removes the local plan. The writer settles the reply
        // even when it stops before taking the queued frame.
        let written = self.wire.reapply_exact_setting(
            &write.route,
            write.restore,
            write.setting,
            write.plan,
            &claim,
            write.reply,
        );
        #[cfg(any(test, feature = "test-observation"))]
        if written.is_ok() {
            self.wire
                .at_opening_effect_for_test(crate::owner_wire::OpeningPausePoint::AfterSetterTake);
        }
        let removed = lock(&self.attempts).apply(slot, &instance, Command::SetterDone(claim));
        if let Ok(effect) = removed {
            effect.finish(&self.wire);
        }
        if written == Err(crate::acp_transport::TransportRefusal::AuthorizationMismatch) {
            let attempts = lock(&self.attempts);
            return Err(Harness(self.opening_refusal(&attempts, identity)));
        }
        written.map_err(|reason| Harness(lifecycle_transport_refusal(reason)))
    }
}
