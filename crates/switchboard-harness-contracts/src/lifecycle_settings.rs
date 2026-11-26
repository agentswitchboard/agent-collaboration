//! Settings inputs and observations for held create and restore attempts.

use std::{collections::BTreeMap, sync::Arc};

use crate::{
    CreateAttempt, HarnessSessionId, LifecycleMove, LoadAttempt, OwnerSetting,
    PermissionModeReason, ProviderSettingKind, ProviderSettingsCatalogRecord, RunAttempt,
    SettingApplication, SettingEvidence, SettingValueId,
};

/// The Router's requested name for the harness's auto realization.
pub const AUTO_PERMISSION_MODE: &str = "auto";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AcceptedRunSettings<Catalogue = ProviderSettingsCatalogRecord> {
    pub catalogue: Arc<Catalogue>,
    pub router_auto_requested: bool,
}

pub struct SetupReport<Capabilities, Settings> {
    pub capabilities: Capabilities,
    pub settings: Settings,
    pub observed: BTreeMap<ProviderSettingKind, SettingEvidence>,
}

pub struct CreatedSetupReport<Capabilities, Settings> {
    pub setup: SetupReport<Capabilities, Settings>,
    pub catalogue: Arc<ProviderSettingsCatalogRecord>,
    pub effective_settings: EffectiveProviderSettings,
    pub permission_mode: PermissionModeReport,
    pub router_auto_requested: bool,
    pub keep_fast_off: bool,
    pub failed_setting: Option<InitialSettingFailure>,
}

/// Partial configuration does not undo creation. Initial invalid settings may close the
/// remote session only when the harness separately confirmed that close.
pub enum CreatedSetupFact<Capabilities, Settings, SettingFailureReason> {
    Configured {
        attempt: CreateAttempt,
        report: Box<CreatedSetupReport<Capabilities, Settings>>,
    },
    RemainsCreated {
        attempt: CreateAttempt,
        report: Box<CreatedSetupReport<Capabilities, Settings>>,
        failure: CreatedSettingsFailure<SettingFailureReason>,
    },
    ClosedAfterInvalidSetting {
        attempt: CreateAttempt,
        failure: InvalidRequestedSetting,
    },
}

pub enum CreatedSettingsFailure<Reason> {
    Invalid(InvalidRequestedSetting),
    Partial {
        applied: Box<[AppliedProviderSetting]>,
        failed: FailedProviderSetting<Reason>,
        not_applied: Box<[NotAppliedProviderSetting]>,
    },
}

impl<Reason> CreatedSettingsFailure<Reason> {
    #[must_use]
    pub const fn valid_moves(&self) -> &'static [LifecycleMove] {
        &[
            LifecycleMove::CorrectRequest,
            LifecycleMove::KeepOutcomeUnknown,
        ]
    }
}

pub struct InvalidRequestedSetting {
    pub session: HarnessSessionId,
    pub kind: ProviderSettingKind,
    pub value: String,
    pub advertised: Vec<String>,
}

impl InvalidRequestedSetting {
    #[must_use]
    pub const fn valid_moves(&self) -> &'static [LifecycleMove] {
        &[LifecycleMove::CorrectRequest]
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppliedProviderSetting {
    pub kind: ProviderSettingKind,
    pub value: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FailedProviderSetting<Reason> {
    pub kind: ProviderSettingKind,
    pub value: String,
    pub reason: Reason,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NotAppliedProviderSetting {
    pub kind: ProviderSettingKind,
    pub value: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InitialSettingFailure {
    pub kind: ProviderSettingKind,
    pub attempted: SettingValueId,
    pub application: SettingApplication,
    pub observation: InitialSettingObservation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InitialSettingObservation {
    Known { confirmed_current: bool },
    Unknown,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EffectiveProviderSettings {
    pub mode: Option<String>,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub fast: Option<FastObserved>,
}

/// What the agent last reported for its fast option.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FastObserved {
    Off,
    On,
    /// The agent offers no fast option.
    NoOption,
}

/// The requested and actual mode of a session and why they differ.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PermissionModeReport {
    pub requested: Option<String>,
    pub actual: Option<String>,
    pub reason: Option<PermissionModeReason>,
}

/// An exact restore setting application. The held opening owns its fallback and setter rules.
pub struct SettingAttempt {
    pub restore: RunAttempt<LoadAttempt>,
    pub setting: OwnerSetting,
    pub value: SettingValueId,
    pub catalogue: Arc<ProviderSettingsCatalogRecord>,
}

pub enum SettingFact {
    Mode {
        restore: RunAttempt<LoadAttempt>,
        setting: OwnerSetting,
        outcome: RestoreModeOutcome,
        catalogue: Option<Arc<ProviderSettingsCatalogRecord>>,
    },
    FastOff {
        restore: RunAttempt<LoadAttempt>,
        setting: OwnerSetting,
        observation: SettingEvidence,
        catalogue: Option<Arc<ProviderSettingsCatalogRecord>>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RestoreModeOutcome {
    Confirmed(SettingValueId),
    Refused { current: Option<SettingValueId> },
    ReportedOther(SettingValueId),
    FellBack(SettingValueId),
    Unconfirmed,
}

impl RestoreModeOutcome {
    #[must_use]
    pub const fn refusal_moves(&self) -> Option<&'static [LifecycleMove]> {
        match self {
            Self::Refused { .. } => Some(&[
                LifecycleMove::CorrectRequest,
                LifecycleMove::UseSavedCatalogue,
            ]),
            Self::Confirmed(_) | Self::ReportedOther(_) | Self::FellBack(_) | Self::Unconfirmed => {
                None
            }
        }
    }
}
