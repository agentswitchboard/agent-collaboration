use std::{
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use super::{REFRESHED, TOKEN};

static LOG_CAPTURE: LogCapture = LogCapture {
    saw_info: AtomicBool::new(false),
    saw_token: AtomicBool::new(false),
    saw_refreshed: AtomicBool::new(false),
};

struct LogCapture {
    saw_info: AtomicBool,
    saw_token: AtomicBool,
    saw_refreshed: AtomicBool,
}

impl log::Log for LogCapture {
    fn enabled(&self, _metadata: &log::Metadata<'_>) -> bool {
        true
    }

    fn log(&self, record: &log::Record<'_>) {
        let message = record.args().to_string();
        self.saw_info.fetch_or(
            record.level() == log::Level::Info && record.target() == "credential_diagnostics_probe",
            Ordering::Relaxed,
        );
        self.saw_token
            .fetch_or(message.contains(TOKEN), Ordering::Relaxed);
        self.saw_refreshed
            .fetch_or(message.contains(REFRESHED), Ordering::Relaxed);
    }

    fn flush(&self) {}
}

#[derive(Clone, Default)]
pub(super) struct TraceCapture(Arc<Mutex<Vec<u8>>>);

impl io::Write for TraceCapture {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .map_err(|_| io::Error::other("trace capture lock poisoned"))?
            .write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(super) fn capture_trace() -> TraceCapture {
    log::set_logger(&LOG_CAPTURE).unwrap();
    log::set_max_level(log::LevelFilter::Trace);
    let capture = TraceCapture::default();
    let writer = capture.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .without_time()
        .with_writer(move || writer.clone())
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();
    log::info!(target: "credential_diagnostics_probe", "log sink is live");
    tracing::trace!("tracing sink is live");
    capture
}

impl TraceCapture {
    pub(super) fn assert_credentials_absent(&self) {
        let bytes = self.0.lock().unwrap();
        let traces = String::from_utf8_lossy(&bytes);
        assert!(LOG_CAPTURE.saw_info.load(Ordering::Relaxed));
        assert!(traces.contains("tracing sink is live"));
        assert!(
            !LOG_CAPTURE.saw_token.load(Ordering::Relaxed)
                && !LOG_CAPTURE.saw_refreshed.load(Ordering::Relaxed)
                && !traces.contains(TOKEN)
                && !traces.contains(REFRESHED),
            "upgrade, renewal, and peer Close must keep credentials out of diagnostics"
        );
    }
}
