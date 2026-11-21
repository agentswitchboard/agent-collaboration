//! Engine query bindings and the frozen main oracle for conformance.

#[cfg(all(test, not(feature = "test-support")))]
pub(crate) const ALARM_INPUTS: &str =
    include_str!("../../tests/fixtures/main_engine_alarm_inputs.sql");
#[cfg(feature = "test-support")]
pub(crate) const ALARM_INPUTS: &str =
    include_str!("../../tests/fixtures/main_fixture_engine_alarm_inputs.sql");

pub(crate) fn alarm_update() -> central_storage::AlarmUpdate {
    central_storage::AlarmUpdate::Engine(central_storage::EngineAlarmPlan::Recompute)
}

#[cfg(test)]
#[path = "query_registry_test.rs"]
mod tests;

#[cfg(feature = "test-support")]
pub(crate) const PROBE_LIST: &str = include_str!("queries/read_due_probes.sql");
