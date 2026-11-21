use crate::engine_host_binding::{EngineHost, FleetEngine};
use crate::{
    event_source_policy::{EventSource, SourcePolicy},
    machine_store_adapter::{MachineStoreAdapter, SubjectLoadFailure, VersionedSubjectState},
    transition_batch_writes::{ApplyFailure, EffectHandler, ObjectHostEffects},
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
    EventSuperseded,
    StorageAnswerLost,
    StorageFailed(StoreFailure),
}
impl<State, Error> From<FailureOutcome> for MachineEventOutcome<State, Error> {
    fn from(outcome: FailureOutcome) -> Self {
        match outcome {
            FailureOutcome::Recorded(record) => Self::AttemptFailureRecorded(record.last_failure),
            FailureOutcome::Superseded => Self::EventSuperseded,
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

struct AckSource;
impl<M: Machine> EventSource<M> for AckSource {
    fn policy(&self) -> SourcePolicy {
        SourcePolicy::Request
    }
}

impl<R: EngineHost> FleetEngine<'_, R> {
    /// Acknowledgements use request policy and carry no receipt statements.
    pub async fn apply_acknowledgement_event<M, A>(
        &self,
        adapter: &A,
        key: InstanceKey,
        event: M::Event,
    ) -> MachineEventOutcome<M::State, M::Error>
    where
        M: Machine,
        M::State: Send + Sync,
        M::Event: Send + Sync,
        M::Effect: Send + Sync,
        M::Error: Send + Sync,
        A: MachineStoreAdapter<M>,
    {
        self.handle::<M, A, _>(adapter, key, event, &AckSource)
            .await
    }

    /// Handles one event for one instance. The event's entry (after_event for due
    /// units, the request entry for requests) has already checked the organization
    /// once for this event (P3), so handle does not read the stamp again.
    pub(crate) async fn handle<M, Adapter, Source>(
        &self,
        adapter: &Adapter,
        key: InstanceKey,
        event: M::Event,
        source: &Source,
    ) -> MachineEventOutcome<M::State, M::Error>
    where
        M: Machine,
        M::State: Send + Sync,
        M::Event: Send + Sync,
        M::Effect: Send + Sync,
        M::Error: Send + Sync,
        Adapter: MachineStoreAdapter<M>,
        Source: EventSource<M>,
    {
        let handler = ObjectHostEffects {
            host: self.host(),
            #[cfg(feature = "test-support")]
            pauses: self.pauses(),
        };
        for _ in 0..2 {
            // R4: a failing subject retries on its next try, and a retired one is
            // never worked, whether or not its row decodes, so the attempt record
            // decides before the row is loaded.
            if source.policy() == SourcePolicy::Due
                && let Some(outcome) = self.due_attempt_wait::<M, Adapter>(adapter, &key).await
            {
                return outcome;
            }
            #[cfg(feature = "test-support")]
            if source.policy() == SourcePolicy::Due {
                self.pauses()
                    .reached_engine(fleet_runtime::PausePoint::AfterEngineAttemptGate, &key.0)
                    .await;
            }
            let prior = match adapter.load_subject_state(&key).await {
                Ok(Some(prior)) => prior,
                Ok(None) => {
                    if source.policy() != SourcePolicy::Request {
                        return MachineEventOutcome::SubjectMissing;
                    }
                    let receipt = match source.missing() {
                        Ok(receipt) => receipt,
                        Err(failure) => return MachineEventOutcome::StorageFailed(failure),
                    };
                    return match handler.apply_receipt(receipt).await {
                        Ok(()) => MachineEventOutcome::SubjectMissing,
                        Err(failure) => receipt_failure(failure),
                    };
                }
                Err(SubjectLoadFailure::CorruptSubjectRow {
                    failure_class: class,
                    ..
                }) if source.policy() == SourcePolicy::Request => {
                    return MachineEventOutcome::RequestItemFailed(class);
                }
                Err(SubjectLoadFailure::CorruptSubjectRow {
                    failure_class: class,
                    observed_subject_version: version,
                }) => {
                    return self
                        .record_failure(
                            &adapter.failure_attempt_subject(&key),
                            adapter.write_attempt_failure_sql(),
                            &version,
                            class,
                            M::retirement_threshold(),
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
            if source.policy() == SourcePolicy::Due
                && let Some(outcome) = self
                    .due_attempt_gate::<M, Adapter>(adapter, &key, &prior)
                    .await
            {
                return outcome;
            }
            #[cfg(feature = "test-support")]
            self.pauses()
                .reached_engine(fleet_runtime::PausePoint::AfterEngineLoad, &key.0)
                .await;
            let now = match self.now() {
                Ok(now) => now,
                Err(failure) => return MachineEventOutcome::StorageFailed(failure),
            };
            let transition = {
                let mut ids = ReducerIds(self.ids());
                M::transition(
                    &prior.decoded_state,
                    event.clone(),
                    &mut Context { now, ids: &mut ids },
                )
            };
            let (next, effects) = match transition {
                Ok(outcome) => outcome,
                Err(error) => match source.policy() {
                    SourcePolicy::Due => {
                        let refusal = match error {
                            TransitionError::Stale => return MachineEventOutcome::EventSuperseded,
                            TransitionError::NotAllowed { state, event } => {
                                format!("{state}:{event}")
                            }
                            TransitionError::Domain(error) => error.refusal_tag().to_owned(),
                        };
                        return self
                            .record_failure(
                                &adapter.failure_attempt_subject(&key),
                                adapter.write_attempt_failure_sql(),
                                &prior.observed_subject_version,
                                ItemFailureClass::Refused { refusal },
                                M::retirement_threshold(),
                            )
                            .await
                            .into();
                    }
                    SourcePolicy::Discard => return MachineEventOutcome::TransitionRefused(error),
                    SourcePolicy::Request => {
                        let receipt = match source.refused(&error) {
                            Ok(receipt) => receipt,
                            Err(failure) => return MachineEventOutcome::StorageFailed(failure),
                        };
                        return match handler.apply_receipt(receipt).await {
                            Ok(()) => MachineEventOutcome::TransitionRefused(error),
                            Err(failure) => receipt_failure(failure),
                        };
                    }
                },
            };
            let receipt = match source.accepted(&next) {
                Ok(receipt) => receipt,
                Err(failure) => return MachineEventOutcome::StorageFailed(failure),
            };
            match handler
                .apply::<M, Adapter>(adapter, &key, &prior, &next, &effects, receipt)
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
                    self.pauses()
                        .reached_engine(fleet_runtime::PausePoint::AfterEngineCommit, &key.0)
                        .await;
                    return MachineEventOutcome::TransitionCommitted(next);
                }
                Err(ApplyFailure::Superseded) => {
                    if source.policy() == SourcePolicy::Discard {
                        return MachineEventOutcome::EventSuperseded;
                    }
                }
                Err(ApplyFailure::Rejected(class)) if source.policy() == SourcePolicy::Request => {
                    return MachineEventOutcome::RequestItemFailed(class);
                }
                Err(ApplyFailure::Rejected(class)) => {
                    return self
                        .record_failure(
                            &adapter.failure_attempt_subject(&key),
                            adapter.write_attempt_failure_sql(),
                            &prior.observed_subject_version,
                            class,
                            M::retirement_threshold(),
                        )
                        .await
                        .into();
                }
                Err(ApplyFailure::Uncertain) => return MachineEventOutcome::StorageAnswerLost,
                Err(ApplyFailure::StoreDown(failure)) => {
                    return MachineEventOutcome::StorageFailed(failure);
                }
            }
        }
        MachineEventOutcome::EventSuperseded
    }

    async fn due_attempt_gate<M: Machine, Adapter: MachineStoreAdapter<M>>(
        &self,
        adapter: &Adapter,
        key: &InstanceKey,
        prior: &VersionedSubjectState<M::State>,
    ) -> Option<MachineEventOutcome<M::State, M::Error>> {
        let subject = adapter.failure_attempt_subject(key);
        match crate::attempt_store::load_attempt(self.host(), &subject).await {
            Ok(Some(stored)) => match &stored.record {
                Err(corruption) => Some(
                    self.write_failure(
                        &subject,
                        adapter.write_attempt_failure_sql(),
                        &prior.observed_subject_version,
                        corruption.failure_class(),
                        M::retirement_threshold(),
                        Some(&stored),
                    )
                    .await
                    .into(),
                ),
                Ok(record) => self.wait_for_next_try(&record.state),
            },
            Ok(None) => None,
            Err(StoreFailure::AnswerLost) => Some(MachineEventOutcome::StorageAnswerLost),
            Err(failure) => Some(MachineEventOutcome::StorageFailed(failure)),
        }
    }

    /// The pre-load half of the due gate. A damaged control row needs the
    /// loaded row's version to be replaced, so it passes here and is replaced
    /// by `due_attempt_gate` after the load.
    async fn due_attempt_wait<M: Machine, Adapter: MachineStoreAdapter<M>>(
        &self,
        adapter: &Adapter,
        key: &InstanceKey,
    ) -> Option<MachineEventOutcome<M::State, M::Error>> {
        match crate::attempt_store::load_attempt(self.host(), &adapter.failure_attempt_subject(key))
            .await
        {
            Ok(Some(stored)) => match &stored.record {
                Ok(record) => self.wait_for_next_try(&record.state),
                Err(_) => None,
            },
            Ok(None) => None,
            Err(StoreFailure::AnswerLost) => Some(MachineEventOutcome::StorageAnswerLost),
            Err(failure) => Some(MachineEventOutcome::StorageFailed(failure)),
        }
    }

    /// Retired is never worked; Failing waits until its next try.
    fn wait_for_next_try<State, Error>(
        &self,
        state: &central_engine_model::AttemptState,
    ) -> Option<MachineEventOutcome<State, Error>> {
        match state {
            central_engine_model::AttemptState::Retired { .. } => {
                Some(MachineEventOutcome::EventSuperseded)
            }
            central_engine_model::AttemptState::Failing { next_try_at } => match self.now() {
                Ok(now) if *next_try_at <= now => None,
                Ok(_) => Some(MachineEventOutcome::EventSuperseded),
                Err(failure) => Some(MachineEventOutcome::StorageFailed(failure)),
            },
        }
    }
}
fn receipt_failure<State, Error>(failure: ApplyFailure) -> MachineEventOutcome<State, Error> {
    match failure {
        ApplyFailure::Superseded => MachineEventOutcome::EventSuperseded,
        ApplyFailure::Uncertain => MachineEventOutcome::StorageAnswerLost,
        ApplyFailure::StoreDown(failure) => MachineEventOutcome::StorageFailed(failure),
        ApplyFailure::Rejected(_) => MachineEventOutcome::StorageFailed(
            fleet_runtime_ports::workspace_decode_error::corrupt("runtime_request_log.claim"),
        ),
    }
}
