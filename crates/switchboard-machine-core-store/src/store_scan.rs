//! Operator results for storage scans that can encounter damaged rows.

/// Successful work and any corrupt rows encountered while selecting it.
#[derive(Debug, Eq, PartialEq)]
#[must_use]
pub struct StoreScan<T> {
    pub healthy: T,
    pub corrupt_rows: Vec<RowCorruption>,
}

impl<T> StoreScan<T> {
    pub(crate) fn map_healthy<U>(self, project: impl FnOnce(T) -> U) -> StoreScan<U> {
        StoreScan {
            healthy: project(self.healthy),
            corrupt_rows: self.corrupt_rows,
        }
    }

    #[cfg(test)]
    #[track_caller]
    pub(crate) fn expect_clean(self) -> T {
        assert!(self.corrupt_rows.is_empty(), "{:?}", self.corrupt_rows);
        self.healthy
    }
}

impl<T> FromIterator<Result<T, RowCorruption>> for StoreScan<Vec<T>> {
    fn from_iter<I: IntoIterator<Item = Result<T, RowCorruption>>>(rows: I) -> Self {
        let mut scan = Self {
            healthy: Vec::new(),
            corrupt_rows: Vec::new(),
        };
        for row in rows {
            match row {
                Ok(value) => scan.healthy.push(value),
                Err(problem) => scan.corrupt_rows.push(problem),
            }
        }
        scan
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct RowCorruption {
    /// The raw key is retained even if it fails identifier validation.
    pub key: String,
    pub field: &'static str,
    pub reason: RowCorruptionReason,
}

#[derive(Debug, Eq, PartialEq)]
pub enum RowCorruptionReason {
    InvalidIdentifier,
    InvalidStorageType,
    TimestampOutOfRange,
    DeadlineMismatch { expected_ms: i64, stored_ms: i64 },
    InvalidJson,
    UnknownState,
    KeyMismatch,
    UnsupportedOperation,
    InconsistentState,
    IncompatibleOutcome,
}

/// Keep invalid UTF-8 keys printable without discarding any of their bytes.
pub(crate) fn diagnostic_key(raw: &[u8]) -> String {
    std::str::from_utf8(raw)
        .map(str::to_owned)
        .unwrap_or_else(|_| format!("bytes:{raw:02x?}"))
}
