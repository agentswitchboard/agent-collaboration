//! Typed failures from physical alarm publication.
#[derive(
    Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize, thiserror::Error,
)]
#[serde(rename_all = "camelCase")]
pub enum AlarmPublicationFailure {
    #[error("this store has no engine publication clock")]
    ClockUnavailable,
    #[error("the decided time is outside the portable alarm range")]
    EarliestDueOutOfRange,
    #[error("the publication clock is outside the portable alarm range")]
    NowOutOfRange,
    #[error("no later five-minute mark is within the portable alarm range")]
    NoLaterGridMark,
}
