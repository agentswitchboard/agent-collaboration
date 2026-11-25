use std::{
    collections::BTreeSet,
    fmt::Write as _,
    io,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use super::{REFRESHED, TOKEN};

const DRIVER_TARGET: &str = "switchboard_central_link";
static HEX_MARKERS: OnceLock<[String; 2]> = OnceLock::new();

static LOG_CAPTURE: LogCapture = LogCapture {
    saw_inactive_close: AtomicBool::new(false),
    driver_exposed_marker: AtomicBool::new(false),
    registration_mismatches: AtomicUsize::new(0),
    dependency_targets: Mutex::new(BTreeSet::new()),
};

struct LogCapture {
    saw_inactive_close: AtomicBool,
    driver_exposed_marker: AtomicBool,
    registration_mismatches: AtomicUsize,
    dependency_targets: Mutex<BTreeSet<(String, log::Level)>>,
}

impl log::Log for LogCapture {
    fn enabled(&self, _metadata: &log::Metadata<'_>) -> bool {
        true
    }

    fn log(&self, record: &log::Record<'_>) {
        let message = record.args().to_string();
        let contains_marker = contains_credential_marker(&message);
        if record.target() == DRIVER_TARGET
            || record.target().starts_with("switchboard_central_link::")
        {
            self.saw_inactive_close.fetch_or(
                record.level() == log::Level::Trace
                    && message.contains("close classified as Some(Inactive)"),
                Ordering::Relaxed,
            );
            self.driver_exposed_marker
                .fetch_or(contains_marker, Ordering::Relaxed);
            if message == "Central run registration reply did not match the pending attempt" {
                self.registration_mismatches.fetch_add(1, Ordering::Relaxed);
            }
        } else if contains_marker {
            self.dependency_targets
                .lock()
                .unwrap()
                .insert((record.target().to_owned(), record.level()));
        }
    }

    fn flush(&self) {}
}

pub(super) fn registration_mismatches() -> usize {
    LOG_CAPTURE.registration_mismatches.load(Ordering::Relaxed)
}

fn contains_credential_marker(message: &str) -> bool {
    let hex_markers = HEX_MARKERS.get_or_init(|| {
        [TOKEN, REFRESHED].map(|token| {
            let mut encoded = String::with_capacity(token.len() * 2);
            for byte in token.bytes() {
                write!(encoded, "{byte:02x}").unwrap();
            }
            encoded
        })
    });
    message.contains(TOKEN)
        || message.contains(REFRESHED)
        || hex_markers.iter().any(|marker| message.contains(marker))
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
        .with_env_filter(tracing_subscriber::EnvFilter::new(
            "switchboard_central_link=trace",
        ))
        .without_time()
        .with_writer(move || writer.clone())
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();
    tracing::trace!(target: DRIVER_TARGET, "tracing sink is live");
    capture
}

impl TraceCapture {
    pub(super) fn assert_credentials_absent(&self) {
        let bytes = self.0.lock().unwrap();
        let traces = String::from_utf8_lossy(&bytes);
        assert!(
            LOG_CAPTURE.saw_inactive_close.load(Ordering::Relaxed),
            "the driver's typed Close classification reached the TRACE capture"
        );
        assert!(traces.contains("tracing sink is live"));
        assert!(
            !LOG_CAPTURE.driver_exposed_marker.load(Ordering::Relaxed)
                && !contains_credential_marker(&traces),
            "upgrade, renewal, and peer Close must keep credentials out of driver diagnostics"
        );
        // The fixture uses synthetic markers. Report only dependency targets and levels.
        eprintln!(
            "dependency targets carrying fixture markers: {:?}",
            LOG_CAPTURE.dependency_targets.lock().unwrap()
        );
    }
}
