//! A sample service with one clock read in production code and one inline test.
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub fn current_unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_a_nonzero_time() {
        std::thread::sleep(Duration::from_millis(1));
        assert!(current_unix_seconds() > 0);
    }
}
