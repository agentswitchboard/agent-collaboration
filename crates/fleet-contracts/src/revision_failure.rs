use central_storage::StoreFailure;
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum AuthorizationRevisionFailure {
    #[error("organization mismatch")]
    OrganizationMismatch,
    #[error("authorization revision conflicts with the retained revision")]
    Conflict,
    #[error("organization floor missing")]
    OrganizationFloorMissing,
    #[error("organization sign-in unavailable")]
    Unavailable,
    #[error("organization sign-in outcome unknown")]
    OutcomeUnknown,
}
impl From<StoreFailure> for AuthorizationRevisionFailure {
    fn from(error: StoreFailure) -> Self {
        match error {
            StoreFailure::AnswerLost => Self::OutcomeUnknown,
            StoreFailure::Unavailable
            | StoreFailure::Corrupt { .. }
            | StoreFailure::AlarmUnpublishable { .. } => Self::Unavailable,
        }
    }
}
