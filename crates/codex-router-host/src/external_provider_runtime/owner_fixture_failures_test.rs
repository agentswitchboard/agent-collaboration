use super::*;
use crate::provider_acp_session_loading::ProviderSessionLoadOutcome;
use acp_client_runtime::RestoreFailure;
use provider_session_machines::session_owner::RestorePhase;

#[test]
fn fixture_only_restore_wording_tracks_the_original_phases() {
    for (failure, phase, expected) in [
        (
            RestoreFailure::preflight(ExternalProviderRuntimeError::SinkClosed),
            RestorePhase::Preflight,
            "session event consumer closed",
        ),
        (
            RestoreFailure::attempted(ExternalProviderRuntimeError::HistoryReplayUnavailable),
            RestorePhase::Attempted,
            "the Router could not publish the restored session's replay or settings, so reconnect and retry this session",
        ),
        (
            RestoreFailure::attempted(ExternalProviderRuntimeError::RestoreNoAnswer),
            RestorePhase::Attempted,
            "the provider did not answer the restore in time, so retry this session",
        ),
        (
            RestoreFailure::attempted(ExternalProviderRuntimeError::Operation(
                "lost restore".to_owned(),
            )),
            RestorePhase::Attempted,
            "provider ACP operation failed: lost restore, so retry this session",
        ),
    ] {
        assert_eq!(
            legacy_load_error_outcome(&failure),
            ProviderSessionLoadOutcome::Unavailable {
                phase,
                reason: expected.to_owned()
            }
        );
    }
}
