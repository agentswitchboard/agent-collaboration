use super::{
    attempt_records::{StoredAttempt, decode_attempt_with_guard},
    attempt_statements::build_attempt_read_statement,
};
use central_storage::StoreFailure;
use fleet_contracts::EngineSubject;
use fleet_runtime_ports::CentralStore;

/// Keep storage refusal separate from a readable but corrupt control record.
pub(crate) async fn read_guarded_attempt<StoreBackend: CentralStore>(
    host: &StoreBackend,
    subject: &EngineSubject,
) -> Result<Option<StoredAttempt>, StoreFailure> {
    let rows = host.read(build_attempt_read_statement(subject)).await?;
    decode_attempt_with_guard(&rows)
}
