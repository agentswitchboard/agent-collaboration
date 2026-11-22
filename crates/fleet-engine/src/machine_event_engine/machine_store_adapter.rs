use central_engine_model::{FailureRecord, InstanceKey, ItemFailureClass, Machine};
use central_storage::{CorruptReason, GuardedStatement, ReadOutcome, StoreFailure};
use fleet_contracts::{AttemptGuard, EngineSubject, MailDeliveryReady, SubjectVersion};
use fleet_runtime_ports::workspace_decode_error::corrupt;
use std::future::Future;

pub struct VersionedSubjectState<State, Version> {
    pub decoded_state: State,
    pub observed_subject_version: Version,
}

pub enum SubjectLoadFailure<Version> {
    CorruptSubjectRow {
        failure_class: ItemFailureClass,
        observed_subject_version: Version,
    },
    StorageAnswerLost,
    StoreReadFailed(StoreFailure),
}

/// Builds the observed owner's attempt write from its original guard cells.
pub type SubjectAttemptWriter<Version> = fn(
    &EngineSubject,
    &FailureRecord,
    Option<&AttemptGuard>,
    &Version,
) -> Result<GuardedStatement, StoreFailure>;

/// One machine's row codec and static statements. Reducers never see SQL.
pub trait MachineStoreAdapter<SubjectMachine: Machine>: Sync {
    type Version: Send + Sync;

    fn load_subject_state(
        &self,
        key: &InstanceKey,
    ) -> impl Future<
        Output = Result<
            Option<VersionedSubjectState<SubjectMachine::State, Self::Version>>,
            SubjectLoadFailure<Self::Version>,
        >,
    > + Send;
    fn plan_subject_state_write(
        &self,
        key: &InstanceKey,
        prior: &VersionedSubjectState<SubjectMachine::State, Self::Version>,
        next: &SubjectMachine::State,
    ) -> Result<GuardedStatement, StoreFailure>;
    fn plan_machine_effect_writes(
        &self,
        key: &InstanceKey,
        effect: &SubjectMachine::Effect,
    ) -> Result<Vec<GuardedStatement>, StoreFailure>;
    fn mail_ready_from_effect(
        &self,
        _effect: &SubjectMachine::Effect,
    ) -> Option<MailDeliveryReady> {
        None
    }
    fn attempt_failure_writer(&self) -> SubjectAttemptWriter<Self::Version>;
    fn failure_attempt_subject(&self, key: &InstanceKey) -> EngineSubject {
        EngineSubject {
            kind: SubjectMachine::KIND,
            key: key.clone(),
        }
    }
}

pub(crate) fn classify_corrupt_subject_row(reason: CorruptReason) -> ItemFailureClass {
    let (CorruptReason::MissingColumn { column }
    | CorruptReason::UnknownKind { column }
    | CorruptReason::BadValue { column }) = reason;
    ItemFailureClass::CorruptRow {
        column,
        raw_value: None,
    }
}

pub fn classify_subject_read_failure<Version>(
    failure: StoreFailure,
) -> SubjectLoadFailure<Version> {
    match failure {
        StoreFailure::AnswerLost => SubjectLoadFailure::StorageAnswerLost,
        failure @ (StoreFailure::Unavailable
        | StoreFailure::Corrupt { .. }
        | StoreFailure::AlarmUnpublishable { .. }) => SubjectLoadFailure::StoreReadFailed(failure),
    }
}
pub fn classify_subject_decode_failure<Version>(
    failure: StoreFailure,
    version: Version,
) -> SubjectLoadFailure<Version> {
    match failure {
        StoreFailure::Corrupt { reason } => SubjectLoadFailure::CorruptSubjectRow {
            failure_class: classify_corrupt_subject_row(reason),
            observed_subject_version: version,
        },
        failure @ (StoreFailure::Unavailable
        | StoreFailure::AnswerLost
        | StoreFailure::AlarmUnpublishable { .. }) => classify_subject_read_failure(failure),
    }
}
pub fn read_observed_subject_version(
    rows: &ReadOutcome,
) -> Result<Option<SubjectVersion>, SubjectLoadFailure<SubjectVersion>> {
    match rows.rows.as_slice() {
        [] => Ok(None),
        [row] => Ok(Some(SubjectVersion(row.clone()))),
        _ => Err(classify_subject_read_failure(corrupt("engine.subject_key"))),
    }
}
