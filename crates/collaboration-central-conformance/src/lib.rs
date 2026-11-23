//! Shared Central fixtures for both central hosts' test suites.
#![forbid(unsafe_code)]
pub mod alarm_input_scenarios;
#[cfg(feature = "session-link-test-support")]
pub mod approval_authority_scenarios;
#[cfg(feature = "session-link-test-support")]
pub mod approval_link_scenarios;
#[cfg(feature = "session-link-test-support")]
pub mod approval_provider_scenarios;
mod machine_allowance_fixture;
#[cfg(feature = "session-link-test-support")]
pub mod parent_approval_scenarios;
#[cfg(feature = "session-link-test-support")]
pub mod session_link_scenarios;
#[cfg(feature = "session-link-test-support")]
pub mod session_project_fixture;
#[cfg(feature = "session-link-test-support")]
pub mod session_project_scenarios;
pub use machine_allowance_fixture::REPLACE_FIXTURE_ALLOWANCES_SQL;
#[cfg(all(feature = "signin-test-support", not(target_arch = "wasm32")))]
pub mod signin_fixtures;
#[cfg(feature = "signin-test-support")]
pub mod workspace_schema_fixture;

/// The error-boxing result every shared fixture and host scenario returns.
pub type ConformanceResult<TValue> = Result<TValue, Box<dyn std::error::Error + Send + Sync>>;
