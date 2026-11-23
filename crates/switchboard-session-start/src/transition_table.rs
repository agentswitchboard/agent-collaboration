use state_machine_core::Transition;

use crate::StartStateKind;

const fn edge(from: StartStateKind, event: &'static str, to: StartStateKind) -> Transition {
    Transition {
        from: from.tag(),
        event,
        to: to.tag(),
        terminal: to.is_terminal(),
    }
}

macro_rules! edges {
    ($($from:ident, $event:literal => $($to:ident)|+;)*) => {
        &[$($(edge(StartStateKind::$from, $event, StartStateKind::$to),)+)*]
    };
}

pub(crate) const TRANSITIONS: &[Transition] = edges![
    NotStarted, "Ask" => NotStarted | SavingCreate;
    SavingCreate, "Ask" => SavingCreate;
    CreateSent, "Ask" => CreateSent;
    CreateUnknown, "Ask" => CreateUnknown | SavingCreate | SavingOutcome;
    StoringSecret, "Ask" => StoringSecret;
    SavingLaunchIntent, "Ask" => SavingLaunchIntent;
    Launching, "Ask" => Launching;
    SavingHeld, "Ask" => SavingHeld;
    SettingUp, "Ask" => SettingUp;
    SavingBind, "Ask" => SavingBind;
    BindSent, "Ask" => BindSent;
    BindUnknown, "Ask" => BindUnknown | SavingBind;
    Opening, "Ask" => Opening;
    SavingCleanUp, "Ask" => SavingCleanUp;
    Ending, "Ask" => Ending;
    Closing, "Ask" => Closing;
    SavingOutcome, "Ask" => SavingOutcome;
    Opened, "Ask" => Opened;
    Failed, "Ask" => Failed;
    Settled, "Ask" => Settled;
    Fenced, "Ask" => Fenced;
    Unknown, "Ask" => Unknown;
    Stopped, "Ask" => Stopped;
    Interrupted, "Ask" => Interrupted;

    SavingCreate, "Save" => CreateSent | Failed | Stopped;
    CreateSent, "SendCreate" => StoringSecret | CreateUnknown | SavingOutcome;
    StoringSecret, "PutSecret" => SavingLaunchIntent | SavingCleanUp | SavingOutcome;
    SavingLaunchIntent, "Save" => Launching | Stopped;
    Launching, "Launch" => SavingHeld | SavingCleanUp | SavingOutcome;
    SavingHeld, "Save" => SettingUp | Closing;
    SettingUp, "SetUp" => SavingBind | SavingCleanUp | Closing;
    SavingBind, "Save" => BindSent | Closing;
    BindSent, "SendBind" => Opening | SavingOutcome | BindUnknown | SavingCleanUp | Closing;
    Opening, "CommitOpening" => Opened | SavingCleanUp | Closing | Unknown;
    SavingCleanUp, "Save" => Ending | Closing | SavingOutcome | Stopped;
    Ending, "SendEnd" => Closing | SavingOutcome;
    Closing, "CloseConversation" => SavingOutcome | Stopped;
    SavingOutcome, "Save" => CreateUnknown | BindUnknown | Failed | Settled | Fenced | Unknown | Stopped | Closing;
];
