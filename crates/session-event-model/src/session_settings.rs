/// Why a caller is applying a setting; never persisted in Session history.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SettingApplicationIntent {
    ExplicitChange,
    TurnContext,
}

pub use switchboard_harness_contracts::{ConfigValue, ConfigValueState, SessionSettings};
