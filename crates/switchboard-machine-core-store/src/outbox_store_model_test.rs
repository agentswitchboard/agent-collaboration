use super::{MARK, RETENTION_MS, accepted, accepted_reply, caller, item, open, uuid_v7};
use crate::{
    LocalOutboxRefusal, MacOutbox, MachineLocalStore, OutboxCancellation, OutboxInsert,
    OutboxRefusal, OutboxRequestStatus, OutboxWriteOutcome, PendingWriteState,
};
use hegel::{TestCase, generators as gs};
use std::collections::BTreeMap;
use workspace_ids::RequestId;
use workspace_model::PostText;

/// A write's state in the model, as the store should report it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ModelState {
    Unsent,
    Sent,
    SentAnswerLost,
    Accepted,
    Refused,
    /// Past the 15-minute deadline shared by thread posts and direct messages.
    Ended(OutboxCancellation),
}

#[derive(Clone, Copy, Debug)]
struct ModelWrite {
    flush_at: i64,
    state: ModelState,
    answered_at: Option<i64>,
    expires_at: i64,
    ended_at: Option<i64>,
}

impl ModelWrite {
    fn status(&self) -> OutboxRequestStatus {
        match self.state {
            ModelState::Unsent | ModelState::SentAnswerLost => OutboxRequestStatus::InOutbox {
                flush_at_ms: self.flush_at,
            },
            ModelState::Sent => OutboxRequestStatus::Sent,
            ModelState::Accepted => OutboxRequestStatus::Answered(accepted_reply()),
            ModelState::Refused => OutboxRequestStatus::Refused(OutboxRefusal::Local(
                LocalOutboxRefusal::RequestIdConflict,
            )),
            ModelState::Ended(reason) => OutboxRequestStatus::Ended { reason },
        }
    }
}

/// Each model write's own text, so a leftover body is attributable.
fn body_of(request_id: &str) -> String {
    format!("unsent text {request_id}")
}

fn post_text(request_id: &RequestId) -> PostText {
    PostText::try_from(body_of(request_id.as_str())).unwrap()
}

const OUTBOX_WINDOW_MS: i64 = 15 * 60 * 1000;

/// The real SQLite store against a model of each write's state.
struct OutboxStoreModel {
    runtime: tokio::runtime::Runtime,
    _root: tempfile::TempDir,
    store: MachineLocalStore,
    model: BTreeMap<RequestId, ModelWrite>,
    next_seed: u16,
    now: i64,
}

impl OutboxStoreModel {
    fn ids_in(&self, states: &[ModelState]) -> Vec<RequestId> {
        self.model
            .iter()
            .filter(|(_, write)| states.contains(&write.state))
            .map(|(id, _)| id.clone())
            .collect()
    }
}

#[hegel::state_machine]
impl OutboxStoreModel {
    #[rule(weight = 3)]
    fn store_a_write(&mut self, tc: TestCase) {
        tc.assume(self.next_seed < 1_000);
        let marks_ahead = tc.draw(gs::integers::<i64>().min_value(1).max_value(3));
        let body = body_of(&uuid_v7(self.next_seed));
        let write = item(self.next_seed, &body, MARK + marks_ahead * 60_000);
        self.next_seed += 1;
        let result = self
            .runtime
            .block_on(MacOutbox::new(&mut self.store).insert(&write, self.now));
        assert_eq!(result.unwrap(), OutboxInsert::Stored);
        self.model.insert(
            write.request,
            ModelWrite {
                flush_at: write.flush_at_ms,
                state: ModelState::Unsent,
                answered_at: None,
                expires_at: self.now + OUTBOX_WINDOW_MS,
                ended_at: None,
            },
        );
    }

    #[rule(weight = 2)]
    fn send_waiting_writes(&mut self, tc: TestCase) {
        let send_at = self.now + 1_000;
        let waiting: Vec<RequestId> = self
            .ids_in(&[ModelState::Unsent, ModelState::SentAnswerLost])
            .into_iter()
            .filter(|id| self.model[id].expires_at > send_at)
            .collect();
        tc.assume(!waiting.is_empty());
        let count = tc.draw(
            gs::integers::<usize>()
                .min_value(1)
                .max_value(waiting.len()),
        );
        let batch = &waiting[..count];
        self.now += 1_000;
        let sent = self
            .runtime
            .block_on(MacOutbox::new(&mut self.store).mark_sent(batch, self.now))
            .unwrap();
        assert_eq!(
            sent.iter()
                .map(|write| write.request.clone())
                .collect::<Vec<_>>(),
            batch
        );
        for id in batch {
            if let Some(write) = self.model.get_mut(id) {
                write.state = ModelState::Sent;
            }
        }
    }

    #[rule(weight = 2)]
    fn answer_or_return_a_sent_write(&mut self, tc: TestCase) {
        let sent = self.ids_in(&[ModelState::Sent]);
        tc.assume(!sent.is_empty());
        let id = tc.draw(gs::sampled_from(sent));
        self.now += 1_000;
        let now = self.now;
        let choice = tc.draw(gs::integers::<u8>().max_value(2));
        let mut outbox = MacOutbox::new(&mut self.store);
        let next = match choice {
            0 => {
                let ended = self
                    .runtime
                    .block_on(outbox.return_unanswered(std::slice::from_ref(&id), now))
                    .unwrap();
                if self.model[&id].expires_at <= now {
                    assert_eq!(
                        ended
                            .iter()
                            .map(|w| w.request_id.clone())
                            .collect::<Vec<_>>(),
                        vec![id.clone()],
                        "a sent write past its deadline ends, never returns"
                    );
                    ModelState::Ended(OutboxCancellation::NotConfirmedWithin15Minutes)
                } else {
                    assert!(ended.is_empty());
                    ModelState::SentAnswerLost
                }
            }
            1 => {
                let refusal = OutboxWriteOutcome::Refused(OutboxRefusal::Local(
                    LocalOutboxRefusal::RequestIdConflict,
                ));
                self.runtime
                    .block_on(outbox.record_outcome(&id, &refusal, now))
                    .unwrap();
                ModelState::Refused
            }
            _ => {
                self.runtime
                    .block_on(outbox.record_outcome(&id, &accepted(), now))
                    .unwrap();
                ModelState::Accepted
            }
        };
        if let Some(write) = self.model.get_mut(&id) {
            write.state = next;
            if matches!(next, ModelState::Accepted | ModelState::Refused) {
                write.answered_at = Some(now);
            }
            if matches!(next, ModelState::Ended(_)) {
                write.ended_at = Some(now);
            }
        }
    }

    #[rule]
    fn overdue_waiting_writes_end(&mut self, _: TestCase) {
        let now = self.now;
        let mut ended: Vec<(RequestId, OutboxCancellation)> = self
            .runtime
            .block_on(MacOutbox::new(&mut self.store).end_overdue(now))
            .unwrap()
            .into_iter()
            .map(|write| (write.request_id, write.reason))
            .collect();
        ended.sort_by(|left, right| left.0.cmp(&right.0));
        let mut expected = Vec::new();
        for (id, write) in &mut self.model {
            if matches!(write.state, ModelState::Unsent | ModelState::SentAnswerLost)
                && write.expires_at <= now
            {
                // A write that never left the Mac is not sent; one that left is unconfirmed.
                let reason = if write.state == ModelState::Unsent {
                    OutboxCancellation::NotSentWithin15Minutes
                } else {
                    OutboxCancellation::NotConfirmedWithin15Minutes
                };
                write.state = ModelState::Ended(reason);
                write.ended_at = Some(now);
                expected.push((id.clone(), reason));
            }
        }
        assert_eq!(ended, expected);
    }

    #[rule]
    fn time_passes_and_answered_writes_age_out(&mut self, tc: TestCase) {
        self.now += tc.draw(
            gs::integers::<i64>()
                .min_value(0)
                .max_value(2 * RETENTION_MS),
        );
        let deleted = self
            .runtime
            .block_on(MacOutbox::new(&mut self.store).delete_ended_before_retention(self.now))
            .unwrap();
        let cutoff = self.now - RETENTION_MS;
        let before = self.model.len();
        self.model.retain(|_, write| {
            write
                .answered_at
                .or(write.ended_at)
                .is_none_or(|ended| ended > cutoff)
        });
        assert_eq!(usize::try_from(deleted).unwrap(), before - self.model.len());
    }

    #[invariant]
    fn the_store_agrees_with_the_model(&mut self, _: TestCase) {
        for (id, write) in &self.model {
            let stored = self
                .runtime
                .block_on(MacOutbox::new(&mut self.store).request_status(&caller(1), id));
            assert_eq!(stored.unwrap(), write.status());
        }
        let mut expected_waiting: Vec<(i64, RequestId)> = self
            .model
            .iter()
            .filter(|(_, write)| {
                matches!(write.state, ModelState::Unsent | ModelState::SentAnswerLost)
            })
            .map(|(id, write)| (write.flush_at, id.clone()))
            .collect();
        expected_waiting.sort();
        assert_eq!(
            self.runtime
                .block_on(MacOutbox::new(&mut self.store).waiting())
                .unwrap(),
            expected_waiting
        );
        let expected_pending: Vec<(RequestId, PendingWriteState)> = self
            .model
            .iter()
            .filter_map(|(id, write)| {
                let state = match write.state {
                    ModelState::Unsent => PendingWriteState::Unsent {
                        body: post_text(id),
                    },
                    ModelState::Sent | ModelState::SentAnswerLost => {
                        PendingWriteState::SentAnswerLost {
                            body: post_text(id),
                        }
                    }
                    ModelState::Accepted => PendingWriteState::Answered(accepted_reply()),
                    ModelState::Refused => return None,
                    ModelState::Ended(reason) => PendingWriteState::Cancelled { reason },
                };
                Some((id.clone(), state))
            })
            .collect();
        let pending = self
            .runtime
            .block_on(MacOutbox::new(&mut self.store).pending_for(&caller(1)))
            .unwrap();
        assert_eq!(
            pending
                .into_iter()
                .map(|write| (write.request_id, write.state))
                .collect::<Vec<_>>(),
            expected_pending
        );
        let mut expected_outgoing: Vec<(i64, RequestId)> = self
            .model
            .iter()
            .filter_map(|(id, write)| write.answered_at.map(|at| (at, id.clone())))
            .collect();
        expected_outgoing.sort_by(|left, right| right.cmp(left));
        let outgoing = self
            .runtime
            .block_on(MacOutbox::new(&mut self.store).outgoing_for(&caller(1), u16::MAX))
            .unwrap();
        assert_eq!(
            outgoing
                .into_iter()
                .map(|write| (write.answered_at_ms, write.request_id))
                .collect::<Vec<_>>(),
            expected_outgoing
        );
        // Owner, 2026-11-15: unsent text stays on the Mac only until central
        // answers. An answered or refused row keeps no text, in the row itself.
        for (id, write) in &self.model {
            if !matches!(
                write.state,
                ModelState::Accepted | ModelState::Refused | ModelState::Ended(_)
            ) {
                continue;
            }
            let stored: String = self
                .runtime
                .block_on(
                    sqlx::query_scalar(
                        "SELECT payload_json FROM outbox_items WHERE request_id = ?",
                    )
                    .bind(id.as_str())
                    .fetch_one(&mut self.store.connection),
                )
                .unwrap();
            assert!(
                !stored.contains(&body_of(id.as_str())),
                "answered row {id} still holds its text"
            );
        }
        let foreign = self
            .runtime
            .block_on(MacOutbox::new(&mut self.store).pending_for(&caller(2)))
            .unwrap();
        assert!(
            foreign.is_empty(),
            "another caller sees none of these writes"
        );
    }
}

#[hegel::test(test_cases = 25)]
fn the_outbox_store_tracks_every_write_like_its_model(tc: TestCase) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let root = tempfile::tempdir().unwrap();
    let store = runtime.block_on(open(&root));
    let machine = OutboxStoreModel {
        runtime,
        _root: root,
        store,
        model: BTreeMap::new(),
        next_seed: 0,
        now: MARK - 30_000,
    };
    hegel::stateful::machine(machine).run(tc);
}
