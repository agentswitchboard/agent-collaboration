//! Identities, times, and transaction-scoped inputs for the Engine due table.
use central_engine_model::{AlarmDecisionError, EngineTime, InstanceKey, supported_time};
use central_storage::StoreFailure;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DueKind {
    WakeTimer,
    AutomationTimer,
    SystemMessageExpiry,
    DirectMessage,
    LegacyNoticeWindows,
    LegacyWorkspaceDrains,
}

impl DueKind {
    pub const GRID: [Self; 4] = [
        Self::WakeTimer,
        Self::AutomationTimer,
        Self::SystemMessageExpiry,
        Self::DirectMessage,
    ];

    pub const fn tag(self) -> &'static str {
        match self {
            Self::WakeTimer => "wake_timer",
            Self::AutomationTimer => "automation_timer",
            Self::SystemMessageExpiry => "system_message_expiry",
            Self::DirectMessage => "direct_message",
            Self::LegacyNoticeWindows => "legacy_notice_windows",
            Self::LegacyWorkspaceDrains => "legacy_workspace_drains",
        }
    }

    pub fn from_tag(tag: &str) -> Option<Self> {
        match tag {
            "wake_timer" => Some(Self::WakeTimer),
            "automation_timer" => Some(Self::AutomationTimer),
            "system_message_expiry" => Some(Self::SystemMessageExpiry),
            "direct_message" => Some(Self::DirectMessage),
            "legacy_notice_windows" => Some(Self::LegacyNoticeWindows),
            "legacy_workspace_drains" => Some(Self::LegacyWorkspaceDrains),
            _ => None,
        }
    }

    pub const fn lane(self) -> DueLane {
        match self {
            Self::WakeTimer
            | Self::AutomationTimer
            | Self::SystemMessageExpiry
            | Self::DirectMessage => DueLane::OnGrid,
            Self::LegacyNoticeWindows | Self::LegacyWorkspaceDrains => DueLane::Legacy,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DueLane {
    OnGrid,
    Legacy,
}

/// The same textual key the dispatcher passes to the owner, scoped by due kind.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DueItemKey {
    pub kind: DueKind,
    pub subject: InstanceKey,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DueEntryKey {
    Item(DueItemKey),
    SourceControl(DueKind),
}

/// Portable milliseconds after the reference query's storage normalisation.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct DueMillis(i64);

impl DueMillis {
    pub const fn get(self) -> i64 {
        self.0
    }
}

impl TryFrom<i64> for DueMillis {
    type Error = AlarmDecisionError;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        EngineTime::from_timestamp_millis(value)
            .filter(|time| supported_time(*time))
            .map(|_| Self(value))
            .ok_or(AlarmDecisionError::OutOfRange)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DueOwnerTiming {
    pub key: DueItemKey,
    pub occurrence: DueMillis,
    pub base_due: DueMillis,
}

impl DueOwnerTiming {
    pub fn at(key: DueItemKey, time: EngineTime) -> Result<Self, StoreFailure> {
        let time = DueMillis::try_from(time.timestamp_millis()).map_err(|_| {
            fleet_runtime_ports::workspace_decode_error::corrupt("engine_due_entries.timestamp")
        })?;
        Ok(Self {
            key,
            occurrence: time,
            base_due: time,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DueTableChange {
    /// Reconcile an attempt change against the already projected owner timing.
    RefreshAttempt(DueEntryKey),
    PublishItem(DueOwnerTiming),
    /// The immediately preceding guarded owner insert may be a conflict no-op.
    PublishInsertedItem(DueOwnerTiming),
    RemoveItem(DueItemKey),
}
