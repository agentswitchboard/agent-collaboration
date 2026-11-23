use super::*;

#[hegel::state_machine]
impl EngineModel {
    #[rule]
    fn create_wake(&mut self, tc: TestCase) {
        tc.assume(self.subjects.len() < 16);
        let marks = tc.draw(gs::integers::<i32>().min_value(0).max_value(11));
        let due = next_mark(self.engine().now()) + minutes(5 * i64::from(marks));
        let key = self.block(self.engine().wake(due, "stateful wake"));
        tc.note(&format!("wake {key} due {due}"));
        self.subjects.push(Subject {
            kind: Kind::Wake,
            key,
            group: None,
            due,
            eligible_at: due,
            fate: Fate::Live,
            fired: false,
            expected: ExpectedRecord::None,
            corrupted_target: None,
        });
        self.settle();
    }

    #[rule]
    fn create_probe(&mut self, tc: TestCase) {
        tc.assume(self.subjects.len() < 16);
        let seconds = tc.draw(gs::integers::<i64>().min_value(1).max_value(1_800));
        let due = self.engine().now() + chrono::Duration::seconds(seconds);
        // Minted from the held clock, never the system clock.
        let key = uuid::Uuid::parse_str(&self.engine().fresh()).unwrap();
        let group = uuid::Uuid::parse_str(&self.engine().fresh()).unwrap();
        let seeded = self.block(async {
            self.engine()
                .runtime()
                .await
                .seed_probe(SeedProbe {
                    key,
                    group,
                    due: Some(due),
                    // The probe table keeps completed claims unique.
                    claim: key.to_string(),
                })
                .await
                .unwrap()
        });
        assert!(matches!(
            seeded,
            central_storage::BatchOutcome::Applied { .. }
        ));
        tc.note(&format!("probe {key} due {due}"));
        self.subjects.push(Subject {
            kind: Kind::Probe,
            key: key.to_string(),
            group: Some(group.to_string()),
            due,
            eligible_at: due,
            fate: Fate::Live,
            fired: false,
            expected: ExpectedRecord::None,
            corrupted_target: None,
        });
        // The seed is not an event; the next event or alarm publishes it.
        self.block(self.engine().after_event());
        self.settle();
    }

    #[rule]
    fn cancel_wake(&mut self, tc: TestCase) {
        let Some(index) = self.pick(&tc, |s| {
            s.kind == Kind::Wake && s.fate == Fate::Live && s.corrupted_target.is_none()
        }) else {
            return;
        };
        let wake_id = self.subjects[index].key.clone();
        let answer = self.block(self.engine().central(CentralMailOperation::CancelWake {
            wake_id: wake_id.clone().try_into().unwrap(),
        }));
        tc.note(&format!("cancel wake {wake_id}: {answer:?}"));
        if matches!(
            answer,
            CentralMailResult::Done(CentralMailAnswer::WakeCancelled { .. })
        ) {
            // R4: a finished subject has no failure record.
            self.subjects[index].fate = Fate::Finished;
            self.subjects[index].expected = ExpectedRecord::None;
        } else {
            self.request_consumed_fault(&wake_id);
        }
        self.settle();
    }

    #[rule]
    fn stop_probe(&mut self, tc: TestCase) {
        let Some(index) = self.pick(&tc, |s| s.kind == Kind::Probe && s.fate == Fate::Live) else {
            return;
        };
        let loss = tc.draw(gs::integers::<u8>().min_value(0).max_value(2));
        let key = self.subjects[index].key.clone();
        let kind = match loss {
            1 => Some((
                EngineCommitFaultKind::AppliedAnswerLost,
                ArmedFault::AppliedAnswerLost,
            )),
            2 => Some((
                EngineCommitFaultKind::NotAppliedAnswerLost,
                ArmedFault::NotAppliedAnswerLost,
            )),
            _ => None,
        };
        if let Some((_, armed)) = kind {
            self.arm(key.clone(), armed);
        }
        let request_id = self.engine().fresh();
        let outcome = self.block(async {
            let runtime = self.engine().runtime().await;
            if let Some((kind, _)) = kind {
                runtime
                    .runtime()
                    .statement_host()
                    .inject_engine_commit_fault(EngineCommitFault {
                        key: InstanceKey(key.clone()),
                        kind,
                    });
            }
            request_probe_as_engine_admin(
                self.engine(),
                &runtime,
                ProbeRequest {
                    request_id: request_id.clone().try_into().unwrap(),
                    key: InstanceKey(key.clone()),
                    event: ProbeEvent::Stop,
                },
            )
            .await
        });
        tc.note(&format!("stop probe {key} (loss {loss}): {outcome:?}"));
        self.request_consumed_fault(&key);
        // The answer is stored with its effects, or neither.
        let stopped = self.block(self.engine().count(
            "SELECT COUNT(*) FROM engine_probe_items WHERE item_id=? AND state='stopped'",
            vec![text(&key)],
        )) == 1;
        let stored = self.block(self.engine().rows(
            "SELECT answer FROM runtime_request_log WHERE request_id=?",
            vec![text(&request_id)],
        ));
        let stored_refusal = match stored.first().map(|row| &row[0]) {
            None => None,
            Some(SqlValue::Text(answer)) => Some(answer.contains("\"refused\"")),
            Some(other) => panic!("stored answer {other:?}"),
        };
        // The stop and its stored answer commit together; a stored answer
        // without the stop is a refusal, which has no effect.
        match (stopped, stored_refusal) {
            (true, Some(false)) | (false, Some(true) | None) => {}
            other => panic!("stop effect and stored answer disagree: {other:?} for {outcome:?}"),
        }
        match &outcome {
            EngineRequestOutcome::Answer {
                receipt: EngineReceipt::Applied { .. },
            } => assert!(stopped, "an applied stop has its effect"),
            EngineRequestOutcome::Answer {
                receipt: EngineReceipt::Refused { .. },
            } => assert_eq!(stored_refusal, Some(true), "a refusal is stored"),
            _ => {}
        }
        if stopped {
            self.subjects[index].fate = Fate::Finished;
            self.subjects[index].expected = ExpectedRecord::None;
        }
        self.settle();
    }

    #[rule]
    fn fault_next_commit(&mut self, tc: TestCase) {
        let Some(index) = self.pick(&tc, |s| s.fate == Fate::Live) else {
            return;
        };
        let key = self.subjects[index].key.clone();
        tc.note(&format!("reject the next commit of {key}"));
        self.arm(key.clone(), ArmedFault::Rejected);
        self.block(async {
            self.engine()
                .runtime()
                .await
                .runtime()
                .statement_host()
                .inject_engine_commit_fault(EngineCommitFault {
                    key: InstanceKey(key),
                    kind: EngineCommitFaultKind::DatabaseCode(19),
                });
        });
    }

    #[rule(weight = 2.0)]
    fn corrupt_wake(&mut self, tc: TestCase) {
        let Some(index) = self.pick(&tc, |s| {
            s.kind == Kind::Wake && s.fate == Fate::Live && s.corrupted_target.is_none()
        }) else {
            return;
        };
        let key = self.subjects[index].key.clone();
        tc.assume(!self.subjects[index].fired);
        tc.assume(!self.fault_armed_on(&key));
        let original = self.block(self.engine().rows(
            "SELECT target FROM automation_wakes WHERE wake_id=?",
            vec![text(&key)],
        ))[0][0]
            .clone();
        self.block(self.engine().raw(
            "UPDATE automation_wakes SET target='not-json' WHERE wake_id=?",
            vec![text(&key)],
        ));
        tc.note(&format!("corrupt wake {key}"));
        self.subjects[index].corrupted_target = Some(original);
    }

    #[rule(weight = 3.0)]
    fn repair_retired(&mut self, tc: TestCase) {
        // The model's own expectation picks the subject, not the store's record.
        // The admin routes hide the test machine's kind (P8 contract).
        let index = self.pick(&tc, |s| {
            s.kind == Kind::Wake && s.fate == Fate::Live && s.expected == ExpectedRecord::Retired
        });
        let Some(index) = index else {
            return;
        };
        let (kind, key) = (self.subjects[index].kind, self.subjects[index].key.clone());
        tc.assume(!self.fault_armed_on(&key));
        let attempt = self.block(self.engine().attempt_id(kind.machine(), &key));
        if tc.draw(gs::booleans()) {
            if let Some(original) = self.subjects[index].corrupted_target.take() {
                self.block(self.engine().raw(
                    "UPDATE automation_wakes SET target=? WHERE wake_id=?",
                    vec![original, text(&key)],
                ));
            }
            let answer = self.block(self.engine().retry(&attempt));
            self.subjects[index].eligible_at = self.engine().now();
            self.subjects[index].expected = ExpectedRecord::None;
            tc.note(&format!("retry {key}: {answer}"));
            assert_eq!(answer, serde_json::json!({"result":"engineRetried"}));
        } else {
            let answer = self.block(self.engine().discard(&attempt));
            tc.note(&format!("discard {key}: {answer}"));
            assert_eq!(answer, serde_json::json!({"result":"engineDiscarded"}));
            self.subjects[index].fate = Fate::Discarded;
            self.subjects[index].expected = ExpectedRecord::None;
        }
        self.settle();
    }

    #[rule]
    fn fire_alarm_event(&mut self, _: TestCase) {
        self.block(self.engine().after_event());
        self.settle();
    }

    #[rule]
    fn advance_seconds(&mut self, tc: TestCase) {
        let seconds = tc.draw(gs::integers::<u64>().min_value(1).max_value(120));
        let clock = self.engine().f.clock.clone();
        self.block(clock.advance(std::time::Duration::from_secs(seconds)))
            .unwrap();
        self.settle();
    }

    #[rule(weight = 2.0)]
    fn advance_minutes(&mut self, tc: TestCase) {
        let minutes = tc.draw(gs::integers::<u64>().min_value(5).max_value(120));
        let clock = self.engine().f.clock.clone();
        self.block(clock.advance(std::time::Duration::from_secs(minutes * 60)))
            .unwrap();
        self.settle();
    }

    #[rule(weight = 0.3)]
    fn restart(&mut self, _: TestCase) {
        let mut engine = self.engine.take().expect("engine open");
        self.runtime.block_on(engine.f.reopen()).unwrap();
        self.engine = Some(engine);
        // A new process: the floor, the receipts and armed faults are in memory.
        self.batch_starts.clear();
        self.seen_units = 0;
        self.seen_batches = 0;
        self.armed_fault = None;
        self.fault_pending = false;
        self.block(self.engine().after_event());
        self.settle();
    }

    #[invariant(always_run)]
    fn pending_work_keeps_a_future_alarm(&self, _: TestCase) {
        let now = self.now_ms();
        let pending = self.subjects.iter().any(|subject| {
            subject.fate == Fate::Live
                && !subject.fired
                && subject.expected != ExpectedRecord::Retired
        });
        let alarm = self.block(self.engine().alarm_ms());
        if pending {
            assert!(
                alarm.is_some_and(|alarm| alarm > now),
                "pending timed work needs a physical alarm after now {now}: {alarm:?}"
            );
        }
    }

    #[invariant(always_run)]
    fn every_occurrence_fires_once_and_none_is_lost(&self, _: TestCase) {
        let now = self.now_ms();
        for subject in &self.subjects {
            let fired = self.fired_in_store(subject);
            assert_eq!(
                fired,
                i64::from(subject.fired),
                "{} fired {fired} times; the model expects {}",
                subject.key,
                i64::from(subject.fired)
            );
            if subject.fate != Fate::Live || subject.fired {
                continue;
            }
            // When the next due batch must have worked it, unless retired.
            let deadline = match subject.expected {
                ExpectedRecord::None => Some(subject.eligible_at.timestamp_millis()),
                ExpectedRecord::Failing { next_try_ms } => Some(next_try_ms),
                ExpectedRecord::Retired => None,
            };
            if let Some(deadline) = deadline {
                assert!(
                    (deadline.div_euclid(CADENCE_MS) + 1) * CADENCE_MS > now,
                    "{} due {} ({:?}) was never worked by {now}",
                    subject.key,
                    subject.due,
                    subject.expected
                );
            }
        }
    }

    #[invariant(always_run)]
    fn failure_records_follow_the_retirement_lifecycle(&self, _: TestCase) {
        for subject in &self.subjects {
            let stored = self.block(self.engine().record(subject.kind.machine(), &subject.key));
            subject.expected.assert_stored(&subject.key, &stored);
        }
        let rows = self.block(self.engine().rows(
            "SELECT machine_kind,subject_key FROM engine_work_attempts",
            vec![],
        ));
        for row in rows {
            let [SqlValue::Text(kind), SqlValue::Text(key)] = row.as_slice() else {
                panic!("failure record shape {row:?}")
            };
            assert!(
                self.subjects
                    .iter()
                    .any(|subject| subject.kind.machine() == kind && &subject.key == key),
                "a failure record for no subject of this test: {kind} {key}"
            );
        }
    }

    #[invariant(always_run)]
    fn projected_wakes_and_alarm_inputs_match_the_reference(&self, _: TestCase) {
        use fleet_runtime_ports::CentralStore;
        self.block(async {
            let central = self.engine().runtime().await;
            let store = central.runtime().statement_host();
            let reference_alarm = store
                .read(central_storage::Statement {
                    sql: include_str!(concat!(
                        env!("CARGO_MANIFEST_DIR"),
                        "/../fleet-engine/src/queries/read_alarm_inputs.sql"
                    )),
                    args: vec![],
                })
                .await
                .unwrap();
            let projected_alarm = store.engine_alarm_inputs().await.unwrap();
            let alarm_cell = |cell: Option<i64>| cell.map_or(SqlValue::Null, integer);
            assert_eq!(
                reference_alarm.rows,
                [[
                    alarm_cell(projected_alarm.earliest_on_grid_due_unix_millis),
                    alarm_cell(projected_alarm.earliest_legacy_due_unix_millis),
                ]]
            );

            let now = self.engine().now();
            let horizon = now + chrono::Duration::hours(24);
            let reference_list = store
                .read(central_storage::Statement {
                    sql: include_str!(concat!(
                        env!("CARGO_MANIFEST_DIR"),
                        "/../fleet-wakes/src/queries/read_due_wakes.sql"
                    )),
                    args: vec![integer(horizon.timestamp_millis()), integer(250)],
                })
                .await
                .unwrap();
            let projected_list = fleet_engine::read_due_rows(
                store,
                fleet_runtime_ports::DueListKind::Wakes,
                horizon,
                250,
            )
            .await
            .unwrap_or_else(|_| panic!("projected wake discovery failed"));
            let projected: Vec<_> = projected_list
                .into_iter()
                .map(|row| {
                    vec![
                        text(row.key.0),
                        integer(row.occurrence_ms),
                        integer(row.effective_due_ms),
                    ]
                })
                .collect();
            assert_eq!(reference_list.rows, projected);
        });
    }

    #[invariant(always_run)]
    fn bursts_are_spaced_except_across_an_alarm_mark(&self, _: TestCase) {
        let mut starts = self.batch_starts.clone();
        starts.sort_unstable();
        for pair in starts.windows(2) {
            assert!(
                pair[1] - pair[0] >= SPACING_MS
                    || pair[1].div_euclid(CADENCE_MS) > pair[0].div_euclid(CADENCE_MS),
                "due batches started {} ms apart: {pair:?}",
                pair[1] - pair[0]
            );
        }
        let snapshot = self
            .block(self.engine().runtime())
            .runtime()
            .due_batch_snapshot();
        if let Some(alarm) = self.block(self.engine().alarm_ms()) {
            assert_eq!(alarm.rem_euclid(CADENCE_MS), 0, "off-mark alarm");
            if snapshot.leftovers_pending
                && let Some(start) = snapshot.last_start_unix_millis
            {
                assert!(
                    alarm > start,
                    "the capped batch at {start} published {alarm}"
                );
            }
        }
    }
}
