use super::engine_host_binding::{EngineHost, FleetEngine};
use super::{
    event_source_policy::{EventExecutionPolicy, EventSource},
    machine_store_adapter::{MachineStoreAdapter, SubjectLoadFailure, VersionedSubjectState},
    transition_batch_writes::{
        BatchApplyFailure, CentralTransitionBatchWriter, TransitionBatchWriter,
    },
};
use central_engine_model::{
    Context, InstanceKey, ItemFailureClass, Machine, RefusalTag, TransitionError,
};
use central_storage::StoreFailure;
use fleet_contracts::FailureOutcome;

pub enum MachineEventOutcome<State, Error> {
    TransitionCommitted(State),
    TransitionRefused(TransitionError<Error>),
    SubjectMissing,
    AttemptFailureRecorded(ItemFailureClass),
    /// A request whose subject or batch is broken gets a typed answer; only due
    /// units charge an attempt record (P2, R3).
    RequestItemFailed(ItemFailureClass),
    Superseded,
    StorageAnswerLost,
    StorageFailed(StoreFailure),
}
impl<State, Error> From<FailureOutcome> for MachineEventOutcome<State, Error> {
    fn from(outcome: FailureOutcome) -> Self {
        match outcome {
            FailureOutcome::Recorded(record) => Self::AttemptFailureRecorded(record.last_failure),
            FailureOutcome::Superseded => Self::Superseded,
            FailureOutcome::Uncertain => Self::StorageAnswerLost,
            FailureOutcome::StoreDown(failure) => Self::StorageFailed(failure),
        }
    }
}
struct ReducerIds<'a>(&'a dyn fleet_runtime_ports::IdSource);
impl central_engine_model::IdSource for ReducerIds<'_> {
    fn next_id(&mut self) -> uuid::Uuid {
        self.0.new_id()
    }
}

struct AcknowledgementEventSource;
impl<SubjectMachine: Machine> EventSource<SubjectMachine> for AcknowledgementEventSource {
    fn event_execution_policy(&self) -> EventExecutionPolicy {
        EventExecutionPolicy::RequestOrAck
    }
}

impl<Host: EngineHost> FleetEngine<'_, Host> {
    /// Acknowledgements use request policy and carry no receipt statements.
    pub async fn apply_acknowledgement_event<SubjectMachine, SubjectStore>(
        &self,
        adapter: &SubjectStore,
        key: InstanceKey,
        event: SubjectMachine::Event,
    ) -> MachineEventOutcome<SubjectMachine::State, SubjectMachine::Error>
    where
        SubjectMachine: Machine,
        SubjectMachine::State: Send + Sync,
        SubjectMachine::Event: Send + Sync,
        SubjectMachine::Effect: Send + Sync,
        SubjectMachine::Error: Send + Sync,
        SubjectStore: MachineStoreAdapter<SubjectMachine>,
    {
        self.apply_machine_event::<SubjectMachine, SubjectStore, _>(
            adapter,
            key,
            event,
            &AcknowledgementEventSource,
        )
        .await
    }

    /// Handles one event for one instance. The event's entry (complete_event_with_observation for due
    /// units, the request entry for requests) has already checked the organization
    /// once for this event (P3), so the machine event handler skips the stamp read.
    pub(crate) async fn apply_machine_event<SubjectMachine, SubjectStore, ExecutionSource>(
        &self,
        adapter: &SubjectStore,
        key: InstanceKey,
        event: SubjectMachine::Event,
        source: &ExecutionSource,
    ) -> MachineEventOutcome<SubjectMachine::State, SubjectMachine::Error>
    where
        SubjectMachine: Machine,
        SubjectMachine::State: Send + Sync,
        SubjectMachine::Event: Send + Sync,
        SubjectMachine::Effect: Send + Sync,
        SubjectMachine::Error: Send + Sync,
        SubjectStore: MachineStoreAdapter<SubjectMachine>,
        ExecutionSource: EventSource<SubjectMachine>,
    {
        let handler = CentralTransitionBatchWriter {
            central_store: self.central_store(),
            #[cfg(feature = "test-support")]
            test_pause_points: self.engine_test_pause_points(),
        };
        for _ in 0..2 {
            // R4: a failing subject retries on its next try, and a retired one is
            // never worked, whether or not its row decodes, so the attempt record
            // decides before the row is loaded.
            if source.event_execution_policy() == EventExecutionPolicy::DueWork
                && let Some(outcome) = self
                    .check_due_attempt_before_subject_load::<SubjectMachine, SubjectStore>(
                        adapter, &key,
                    )
                    .await
            {
                return outcome;
            }
            #[cfg(feature = "test-support")]
            if source.event_execution_policy() == EventExecutionPolicy::DueWork {
                self.engine_test_pause_points()
                    .reached_engine(fleet_runtime::PausePoint::AfterEngineAttemptGate, &key.0)
                    .await;
            }
            let prior = match adapter.load_subject_state(&key).await {
                Ok(Some(prior)) => prior,
                Ok(None) => {
                    if source.event_execution_policy() != EventExecutionPolicy::RequestOrAck {
                        return MachineEventOutcome::SubjectMissing;
                    }
                    let receipt = match source.plan_missing_subject_receipt() {
                        Ok(receipt) => receipt,
                        Err(failure) => return MachineEventOutcome::StorageFailed(failure),
                    };
                    return match handler.apply_receipt_only_batch(receipt).await {
                        Ok(()) => MachineEventOutcome::SubjectMissing,
                        Err(failure) => map_receipt_write_failure(failure),
                    };
                }
                Err(SubjectLoadFailure::CorruptSubjectRow {
                    failure_class: class,
                    ..
                }) if source.event_execution_policy() == EventExecutionPolicy::RequestOrAck => {
                    return MachineEventOutcome::RequestItemFailed(class);
                }
                Err(SubjectLoadFailure::CorruptSubjectRow {
                    failure_class: class,
                    observed_subject_version: version,
                }) => {
                    return self
                        .record_attempt_failure(
                            &adapter.failure_attempt_subject(&key),
                            adapter.attempt_failure_writer(),
                            version.as_ref(),
                            class,
                            SubjectMachine::retirement_threshold(),
                        )
                        .await
                        .into();
                }
                Err(SubjectLoadFailure::StorageAnswerLost) => {
                    return MachineEventOutcome::StorageAnswerLost;
                }
                Err(SubjectLoadFailure::StoreReadFailed(failure)) => {
                    return MachineEventOutcome::StorageFailed(failure);
                }
            };
            if source.event_execution_policy() == EventExecutionPolicy::DueWork
                && let Some(outcome) = self
                    .check_due_attempt_after_subject_load::<SubjectMachine, SubjectStore>(
                        adapter, &key, &prior,
                    )
                    .await
            {
                return outcome;
            }
            #[cfg(feature = "test-support")]
            self.engine_test_pause_points()
                .reached_engine(fleet_runtime::PausePoint::AfterEngineLoad, &key.0)
                .await;
            let now = match self.current_engine_time() {
                Ok(now) => now,
                Err(failure) => return MachineEventOutcome::StorageFailed(failure),
            };
            let transition = {
                let mut ids = ReducerIds(self.host_id_source());
                SubjectMachine::transition(
                    &prior.decoded_state,
                    event.clone(),
                    &mut Context { now, ids: &mut ids },
                )
            };
            let (next, effects) = match transition {
                Ok(outcome) => outcome,
                Err(error) => match source.event_execution_policy() {
                    EventExecutionPolicy::DueWork => {
                        let refusal = match error {
                            TransitionError::Stale => return MachineEventOutcome::Superseded,
                            TransitionError::NotAllowed { state, event } => {
                                format!("{state}:{event}")
                            }
                            TransitionError::Domain(error) => error.refusal_tag().to_owned(),
                        };
                        return self
                            .record_attempt_failure(
                                &adapter.failure_attempt_subject(&key),
                                adapter.attempt_failure_writer(),
                                &prior.observed_subject_version,
                                ItemFailureClass::Refused { refusal },
                                SubjectMachine::retirement_threshold(),
                            )
                            .await
                            .into();
                    }
                    EventExecutionPolicy::AdminDiscard => {
                        return MachineEventOutcome::TransitionRefused(error);
                    }
                    EventExecutionPolicy::RequestOrAck => {
                        let receipt = match source.plan_refusal_receipt(&error) {
                            Ok(receipt) => receipt,
                            Err(failure) => return MachineEventOutcome::StorageFailed(failure),
                        };
                        return match handler.apply_receipt_only_batch(receipt).await {
                            Ok(()) => MachineEventOutcome::TransitionRefused(error),
                            Err(failure) => map_receipt_write_failure(failure),
                        };
                    }
                },
            };
            let receipt = match source.plan_accepted_receipt(&next) {
                Ok(receipt) => receipt,
                Err(failure) => return MachineEventOutcome::StorageFailed(failure),
            };
            match handler
                .apply_guarded_transition_batch::<SubjectMachine, SubjectStore>(
                    adapter,
                    &key,
                    &prior,
                    &next,
                    &effects,
                    super::transition_batch_writes::TransitionReceipt {
                        statements: receipt,
                        applied_at: now,
                    },
                )
                .await
            {
                Ok(()) => {
                    for delivery in effects
                        .iter()
                        .filter_map(|effect| adapter.mail_ready_from_effect(effect))
                    {
                        self.0.offer_ready_mail(delivery);
                    }
                    #[cfg(feature = "test-support")]
                    self.engine_test_pause_points()
                        .reached_engine(fleet_runtime::PausePoint::AfterEngineCommit, &key.0)
                        .await;
                    return MachineEventOutcome::TransitionCommitted(next);
                }
                Err(BatchApplyFailure::Superseded) => {
                    if source.event_execution_policy() == EventExecutionPolicy::AdminDiscard {
                        return MachineEventOutcome::Superseded;
                    }
                }
                Err(BatchApplyFailure::Rejected(class))
                    if source.event_execution_policy() == EventExecutionPolicy::RequestOrAck =>
                {
                    return MachineEventOutcome::RequestItemFailed(class);
                }
                Err(BatchApplyFailure::Rejected(class)) => {
                    return self
                        .record_attempt_failure(
                            &adapter.failure_attempt_subject(&key),
                            adapter.attempt_failure_writer(),
                            &prior.observed_subject_version,
                            class,
                            SubjectMachine::retirement_threshold(),
                        )
                        .await
                        .into();
                }
                Err(BatchApplyFailure::StorageAnswerLost) => {
                    return MachineEventOutcome::StorageAnswerLost;
                }
                Err(BatchApplyFailure::StorageFailed(failure)) => {
                    return MachineEventOutcome::StorageFailed(failure);
                }
            }
        }
        MachineEventOutcome::Superseded
    }

    async fn check_due_attempt_after_subject_load<
        SubjectMachine: Machine,
        SubjectStore: MachineStoreAdapter<SubjectMachine>,
    >(
        &self,
        adapter: &SubjectStore,
        key: &InstanceKey,
        prior: &VersionedSubjectState<SubjectMachine::State, SubjectStore::Version>,
    ) -> Option<MachineEventOutcome<SubjectMachine::State, SubjectMachine::Error>> {
        let subject = adapter.failure_attempt_subject(key);
        match super::attempt_store::read_guarded_attempt(self.central_store(), &subject).await {
            Ok(Some(stored)) => match &stored.decoded_failure_record {
                Err(corruption) => Some(
                    self.write_observed_attempt_failure(
                        &subject,
                        adapter.attempt_failure_writer(),
                        &prior.observed_subject_version,
                        corruption.to_corrupt_row_failure(),
                        SubjectMachine::retirement_threshold(),
                        Some(&stored),
                    )
                    .await
                    .into(),
                ),
                Ok(record) => self.due_attempt_skip_outcome(&record.state),
            },
            Ok(None) => None,
            Err(StoreFailure::AnswerLost) => Some(MachineEventOutcome::StorageAnswerLost),
            Err(failure) => Some(MachineEventOutcome::StorageFailed(failure)),
        }
    }

    /// The pre-load half of the due gate. A damaged control row needs the
    /// loaded row's version to be replaced, so it passes here and is replaced
    /// by `check_due_attempt_after_subject_load` after the load.
    async fn check_due_attempt_before_subject_load<
        SubjectMachine: Machine,
        SubjectStore: MachineStoreAdapter<SubjectMachine>,
    >(
        &self,
        adapter: &SubjectStore,
        key: &InstanceKey,
    ) -> Option<MachineEventOutcome<SubjectMachine::State, SubjectMachine::Error>> {
        match super::attempt_store::read_guarded_attempt(
            self.central_store(),
            &adapter.failure_attempt_subject(key),
        )
        .await
        {
            Ok(Some(stored)) => match &stored.decoded_failure_record {
                Ok(record) => self.due_attempt_skip_outcome(&record.state),
                Err(_) => None,
            },
            Ok(None) => None,
            Err(StoreFailure::AnswerLost) => Some(MachineEventOutcome::StorageAnswerLost),
            Err(failure) => Some(MachineEventOutcome::StorageFailed(failure)),
        }
    }

    /// Retired is never worked; Failing waits until its next try.
    fn due_attempt_skip_outcome<State, Error>(
        &self,
        state: &central_engine_model::AttemptState,
    ) -> Option<MachineEventOutcome<State, Error>> {
        match state {
            central_engine_model::AttemptState::Retired { .. } => {
                Some(MachineEventOutcome::Superseded)
            }
            central_engine_model::AttemptState::Failing { next_try_at } => {
                match self.current_engine_time() {
                    Ok(now) if *next_try_at <= now => None,
                    Ok(_) => Some(MachineEventOutcome::Superseded),
                    Err(failure) => Some(MachineEventOutcome::StorageFailed(failure)),
                }
            }
        }
    }
}
fn map_receipt_write_failure<State, Error>(
    failure: BatchApplyFailure,
) -> MachineEventOutcome<State, Error> {
    match failure {
        BatchApplyFailure::Superseded => MachineEventOutcome::Superseded,
        BatchApplyFailure::StorageAnswerLost => MachineEventOutcome::StorageAnswerLost,
        BatchApplyFailure::StorageFailed(failure) => MachineEventOutcome::StorageFailed(failure),
        BatchApplyFailure::Rejected(_) => MachineEventOutcome::StorageFailed(
            fleet_runtime_ports::workspace_decode_error::corrupt("runtime_request_log.claim"),
        ),
    }
}
