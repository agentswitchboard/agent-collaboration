//! Runs the local handover minute and commits decisions around the managed writer.

use std::{collections::BTreeMap, sync::Arc};

use router_clock::RuntimeClock;
use runtime_primitives::SyncTime;
use switchboard_machine_core_store::{
    DirectHandoff, DirectHandoffRow, HandoffCommit, NoticeAttemptId,
};
use tokio::sync::watch;
use workspace_ids::SessionRef;
use workspace_model::central_mail::MessageDelivery;

use crate::{
    custody_run_group::{CustodyRunState, wait_for_stop},
    flush_marks::next_mark_after,
    handover::{HandoffEvent, HandoverError, changes_for},
    handover_notice::{ClaimedNotice, ManagedClaudeNoticePort, NoticeAuthority},
    offer_taker::SharedRecipientInbox,
    recipient_sessions::RecipientSessionAnswer,
};

pub(super) struct HandoverRunner {
    port: Arc<dyn ManagedClaudeNoticePort>,
    inbox: SharedRecipientInbox,
    clock: Arc<dyn RuntimeClock>,
    authority: Arc<NoticeAuthority>,
    stop: watch::Receiver<CustodyRunState>,
}

impl HandoverRunner {
    pub(super) fn new(
        port: Arc<dyn ManagedClaudeNoticePort>,
        inbox: SharedRecipientInbox,
        clock: Arc<dyn RuntimeClock>,
        authority: Arc<NoticeAuthority>,
        stop: watch::Receiver<CustodyRunState>,
    ) -> Self {
        Self {
            port,
            inbox,
            clock,
            authority,
            stop,
        }
    }

    pub(super) async fn run(mut self, mut report: impl FnMut(HandoverError)) {
        let mut mark = next_mark_after(self.clock.now());
        if let Err(error) = self.recover_writing().await {
            report(error);
        }
        loop {
            let wake = self.clock.wall_deadline(mark);
            tokio::select! {
                biased;
                () = wait_for_stop(&mut self.stop) => return,
                () = self.clock.sleep_until(wake) => {}
            }
            let now = self.clock.now();
            if now < mark {
                continue;
            }
            let next = next_mark_after(now);
            let following = next_mark_after(mark);
            // A delayed poll must not catch up an old minute's notice.
            if now < following
                && let Err(error) = self.visit_mark(following).await
            {
                report(error);
            }
            mark = next;
        }
    }

    async fn recover_writing(&self) -> Result<(), HandoverError> {
        let rows = self.inbox.lock().await.direct_handoffs().await?;
        for (target, rows) in group_by_recipient(
            rows.into_iter()
                .filter(|row| matches!(row.handoff, DirectHandoff::Writing { .. })),
        ) {
            let (changes, _) = changes_for(&rows, HandoffEvent::RecoverWriting)?;
            self.inbox
                .lock()
                .await
                .commit_direct_handoffs(&target, &changes)
                .await?;
        }
        Ok(())
    }

    async fn visit_mark(&mut self, following: SyncTime) -> Result<(), HandoverError> {
        let mut inbox = self.inbox.lock().await;
        let directory = inbox.recipient_directory();
        let rows = inbox.waiting_notice_handoffs().await?;
        drop(inbox);
        let waiting = rows.into_iter().filter(|row| {
            row.delivery == MessageDelivery::Auto && row.handoff == DirectHandoff::Waiting
        });
        for (target, mut rows) in group_by_recipient(waiting) {
            if self.stop.borrow().stopped || self.clock.now() >= following {
                return Ok(());
            }
            let now = self.clock.utc_now();
            let (expired, waiting): (Vec<_>, Vec<_>) =
                rows.into_iter().partition(|row| row.expires_at <= now);
            if !expired.is_empty() {
                let (changes, _) = changes_for(&expired, HandoffEvent::Cutoff { now })?;
                self.inbox
                    .lock()
                    .await
                    .commit_direct_handoffs(&target, &changes)
                    .await?;
            }
            rows = waiting;
            let Some(expiry) = rows.iter().map(|row| row.expires_at).min() else {
                continue;
            };
            let latest = self.clock.wall_deadline(SyncTime::from_unix_millis(
                expiry.timestamp_millis().min(following.unix_millis()),
            ));
            let lookup = directory.query_until(&target.session_id, self.clock.as_ref(), latest);
            let recipient = tokio::select! {
                biased;
                () = wait_for_stop(&mut self.stop) => return Ok(()),
                answer = lookup => answer,
            };
            if recipient != RecipientSessionAnswer::KnownOnce {
                continue;
            }
            let ready = tokio::select! {
                biased;
                () = wait_for_stop(&mut self.stop) => return Ok(()),
                () = self.clock.sleep_until(latest) => continue,
                ready = self.port.ready(&target) => ready,
            };
            let Ok(ready) = ready else { continue };
            let now = self.clock.utc_now();
            if self.clock.monotonic_now() >= latest || self.stop.borrow().stopped {
                continue;
            }
            let attempt = NoticeAttemptId::mint(now);
            let (claims, offer) =
                changes_for(&rows, HandoffEvent::EligibleAtMark { attempt, now })?;
            if !offer
                || self
                    .inbox
                    .lock()
                    .await
                    .commit_direct_handoffs(&target, &claims)
                    .await?
                    != HandoffCommit::Committed
            {
                continue;
            }
            let notice = ClaimedNotice::new(
                target.clone(),
                attempt,
                claims.iter().map(|row| row.message_id.clone()).collect(),
                latest,
                expiry,
                self.authority.clone(),
            )?;
            let outcome = tokio::select! {
                biased;
                () = wait_for_stop(&mut self.stop) => return Ok(()),
                outcome = async { ready.offer(notice).await } => outcome,
                () = self.clock.sleep_until(latest) => {
                    crate::handover::NoticeWriteOutcome::NotKnown(
                        crate::handover::NoticeUncertainty::WriterOutcomeLost,
                    )
                },
            };
            for (row, claim) in rows.iter_mut().zip(claims) {
                row.handoff = claim.next;
            }
            let (settled, _) = changes_for(
                &rows,
                HandoffEvent::WriteFinished {
                    attempt,
                    outcome,
                    now: self.clock.utc_now(),
                },
            )?;
            self.inbox
                .lock()
                .await
                .commit_direct_handoffs(&target, &settled)
                .await?;
        }
        Ok(())
    }
}

fn group_by_recipient(
    rows: impl IntoIterator<Item = DirectHandoffRow>,
) -> BTreeMap<SessionRef, Vec<DirectHandoffRow>> {
    let mut groups: BTreeMap<_, Vec<_>> = BTreeMap::new();
    for row in rows {
        groups.entry(row.target.clone()).or_default().push(row);
    }
    groups
}
