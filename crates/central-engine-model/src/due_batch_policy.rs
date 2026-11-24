#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DueBatchPolicy {
    pub max_batches_per_wake: usize,
    pub look_ahead: chrono::Duration,
    pub per_kind_cap: usize,
}

impl DueBatchPolicy {
    pub const DEFAULT: Self = Self {
        max_batches_per_wake: 4,
        look_ahead: chrono::Duration::seconds(50),
        per_kind_cap: 250,
    };
}

impl Default for DueBatchPolicy {
    fn default() -> Self {
        Self::DEFAULT
    }
}
