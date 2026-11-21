#[cfg(feature = "test-support")]
use super::request_receipt_store::{EngineReceipt, EngineRefusal, ReceiptClaim};
use central_engine_model::{Machine, TransitionError};
use central_storage::{GuardedStatement, StoreFailure};
#[cfg(feature = "test-support")]
use serde::Serialize;

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum EventExecutionPolicy {
    DueWork,
    RequestEvent,
    AdminDiscard,
}

pub(crate) enum ReceiptStatements {
    None,
    #[cfg_attr(
        not(feature = "test-support"),
        expect(
            dead_code,
            reason = "only explicit machine requests carry these receipts"
        )
    )]
    RequestReceiptWrites(Vec<GuardedStatement>),
    AdminReceiptWrites(Vec<GuardedStatement>),
}
impl ReceiptStatements {
    pub fn into_statements(self) -> Vec<GuardedStatement> {
        match self {
            Self::None => vec![],
            Self::RequestReceiptWrites(statements) | Self::AdminReceiptWrites(statements) => {
                statements
            }
        }
    }
}

/// Static source-specific receipt construction keeps serialization off the
/// Machine contract. SubjectStore due event has no receipt; request states opt into Serde.
pub(crate) trait EventSource<SubjectMachine: Machine>: Sync {
    fn event_execution_policy(&self) -> EventExecutionPolicy;
    fn plan_accepted_receipt(
        &self,
        _state: &SubjectMachine::State,
    ) -> Result<ReceiptStatements, StoreFailure> {
        Ok(ReceiptStatements::None)
    }
    fn plan_refusal_receipt(
        &self,
        _error: &TransitionError<SubjectMachine::Error>,
    ) -> Result<ReceiptStatements, StoreFailure> {
        Ok(ReceiptStatements::None)
    }
    fn plan_missing_subject_receipt(&self) -> Result<ReceiptStatements, StoreFailure> {
        Ok(ReceiptStatements::None)
    }
}

pub(crate) struct DueEventSource;
impl<SubjectMachine: Machine> EventSource<SubjectMachine> for DueEventSource {
    fn event_execution_policy(&self) -> EventExecutionPolicy {
        EventExecutionPolicy::DueWork
    }
}

#[cfg(feature = "test-support")]
pub(crate) struct RetainedRequestEventSource(pub ReceiptClaim);
#[cfg(feature = "test-support")]
impl<SubjectMachine: Machine> EventSource<SubjectMachine> for RetainedRequestEventSource
where
    SubjectMachine::State: Serialize,
{
    fn event_execution_policy(&self) -> EventExecutionPolicy {
        EventExecutionPolicy::RequestEvent
    }
    fn plan_accepted_receipt(
        &self,
        state: &SubjectMachine::State,
    ) -> Result<ReceiptStatements, StoreFailure> {
        self.0
            .plan_receipt_claim_writes(&EngineReceipt::<_, SubjectMachine::Error>::Applied {
                state,
            })
            .map(ReceiptStatements::RequestReceiptWrites)
    }
    fn plan_refusal_receipt(
        &self,
        error: &TransitionError<SubjectMachine::Error>,
    ) -> Result<ReceiptStatements, StoreFailure> {
        let error = match error {
            TransitionError::Domain(error) => EngineRefusal::Domain { error },
            TransitionError::NotAllowed { state, event } => EngineRefusal::NotAllowed {
                state: (*state).into(),
                event: (*event).into(),
            },
            TransitionError::Stale => EngineRefusal::Stale,
        };
        self.0
            .plan_receipt_claim_writes(&EngineReceipt::<SubjectMachine::State, _>::Refused {
                error,
            })
            .map(ReceiptStatements::RequestReceiptWrites)
    }
    fn plan_missing_subject_receipt(&self) -> Result<ReceiptStatements, StoreFailure> {
        self.0
            .plan_receipt_claim_writes(
                &EngineReceipt::<SubjectMachine::State, SubjectMachine::Error>::NotFound,
            )
            .map(ReceiptStatements::RequestReceiptWrites)
    }
}
