use super::{OwnerRejection, OwnerResult, OwnerTransition};
use crate::session_cell_machine::{QueuedOperation, SessionCellEvent};
use crate::session_owner::{
    AuthorizationStatus, InputWait, OwnerInput, PromptAdmission, RevokeReason, SessionOwnerState,
    UnstartedNotice,
};

impl SessionOwnerState {
    pub(super) fn unstarted_notice(&self) -> Option<UnstartedNotice> {
        if self
            .ports
            .unsent_prompts
            .values()
            .any(|reason| *reason == RevokeReason::NoticeYieldedToPrompt)
        {
            return Some(UnstartedNotice::Withdrawing);
        }
        let notice = self.prompts.values().find(|pending| {
            pending.admission == PromptAdmission::Notice && pending.wait != InputWait::Wire
        })?;
        let withdrawing = notice.wait == InputWait::UndoStore
            || self.ports.authorizations.iter().any(|(key, pending)| {
                key.input_id == notice.input.input
                    && pending.status
                        == AuthorizationStatus::Revoking(RevokeReason::NoticeYieldedToPrompt)
            });
        Some(if withdrawing {
            UnstartedNotice::Withdrawing
        } else {
            UnstartedNotice::Owned(notice.input.clone())
        })
    }
}

impl OwnerTransition {
    pub(super) fn yield_notice(&mut self, notice: OwnerInput) -> OwnerResult {
        self.require_live(notice.generation)?;
        if self.next.unstarted_notice() != Some(UnstartedNotice::Owned(notice.clone())) {
            return Err(OwnerRejection::IgnoredLate);
        }
        if self.revoke_input(
            notice.generation,
            &notice.input,
            RevokeReason::NoticeYieldedToPrompt,
        ) {
            return Ok(());
        }
        let operation = QueuedOperation(notice.input.as_str().to_owned());
        self.next
            .ports
            .unsent_prompts
            .insert(notice, RevokeReason::NoticeYieldedToPrompt);
        let effects = self.cell_move(SessionCellEvent::CancelEntry { operation })?;
        self.apply_entry_effects(effects)
    }
}
