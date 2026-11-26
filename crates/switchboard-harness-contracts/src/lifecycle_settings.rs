//! Settings inputs and observations for held create and restore attempts.

use std::{collections::BTreeMap, sync::Arc};

use crate::{
    CreateAttempt, HarnessSessionId, LifecycleMove, LoadAttempt, OwnerSetting,
    PermissionModeReason, ProviderSettingKind, ProviderSettingsCatalogRecord, RunAttempt,
    SettingApplication, SettingEvidence, SettingValueId,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AcceptedRunSettings {
    pub catalogue: Arc<ProviderSettingsCatalogRecord>,
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
        applied: Box<[ConfiguredSetting]>,
        failed: ConfiguredSetting,
        reason: Reason,
        not_applied: Box<[ConfiguredSetting]>,
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
    pub setting: ConfiguredSetting,
    pub advertised: Vec<String>,
}

impl InvalidRequestedSetting {
    #[must_use]
    pub const fn valid_moves(&self) -> &'static [LifecycleMove] {
        &[LifecycleMove::CorrectRequest]
    }
}

pub struct ConfiguredSetting {
    pub kind: ProviderSettingKind,
    pub value: String,
}

pub struct InitialSettingFailure {
    pub kind: ProviderSettingKind,
    pub attempted: SettingValueId,
    pub application: SettingApplication,
    pub observation: InitialSettingObservation,
}

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FastObserved {
    Off,
    On,
    NoOption,
}

#[derive(Clone, Debug, Eq, PartialEq)]
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
