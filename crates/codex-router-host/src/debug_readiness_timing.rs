//! Opt-in debug proof timings containing fixed stage names and no configuration values.
pub fn record(stage: &'static str, started: std::time::Instant) {
    record_sample(stage, || (started.elapsed(), chrono::Utc::now()));
}

pub(crate) fn record_with_clock(
    stage: &'static str,
    started: router_clock::ClockInstant,
    clock: &dyn router_clock::RuntimeClock,
) {
    record_sample(stage, || {
        (
            clock.monotonic_now().saturating_duration_since(started),
            clock.utc_now(),
        )
    });
}

fn record_sample(
    stage: &'static str,
    sample: impl FnOnce() -> (std::time::Duration, chrono::DateTime<chrono::Utc>),
) {
    #[cfg(debug_assertions)]
    if std::env::var_os("CODEX_ROUTER_DEBUG_READINESS_TIMING").as_deref()
        == Some(std::ffi::OsStr::new("1"))
    {
        let (elapsed, at) = sample();
        eprintln!(
            "{}",
            serde_json::json!({"kind":"debugReadinessTiming","stage":stage,"elapsedMs":elapsed.as_millis(),"at":at.to_rfc3339()})
        );
    }
    #[cfg(not(debug_assertions))]
    let _ = (stage, sample);
}
