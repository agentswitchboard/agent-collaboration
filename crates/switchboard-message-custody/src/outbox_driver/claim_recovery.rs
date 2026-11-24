//! The outbox driver's recovery gate for the current minute drain.

#![deny(clippy::wildcard_enum_match_arm)]

use state_machine_core::{
    EventTag, RefusalTag, StateMachine, StateMachineResult, StateTag, Transition,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ClaimRecovery {
    Clear,
    ScanOwed,
    ParkedForScan,
    ParkedForReturn,
}

#[derive(Clone, Copy, Debug)]
pub(super) enum ClaimRecoveryEvent {
    ClaimFailed,
    ClaimEmpty,
    ScanFinished {
        owes_local_return: bool,
    },
    LocalRetryFinished {
        owes_local_return: bool,
    },
    /// The window closed, the connection was replaced, or the link became unavailable.
    DrainReset,
}

pub(super) enum ClaimRecoveryRefusal {}

impl RefusalTag for ClaimRecoveryRefusal {
    fn refusal_tag(&self) -> &'static str {
        match *self {}
    }
}

const CLAIM_FAILED: &str = "claimFailed";
const CLAIM_EMPTY: &str = "claimEmpty";
const SCAN_FINISHED: &str = "scanFinished";
const LOCAL_RETRY_FINISHED: &str = "localRetryFinished";
const DRAIN_RESET: &str = "drainReset";

impl EventTag for ClaimRecoveryEvent {
    fn event_tag(&self) -> &'static str {
        match self {
            Self::ClaimFailed => CLAIM_FAILED,
            Self::ClaimEmpty => CLAIM_EMPTY,
            Self::ScanFinished { .. } => SCAN_FINISHED,
            Self::LocalRetryFinished { .. } => LOCAL_RETRY_FINISHED,
            Self::DrainReset => DRAIN_RESET,
        }
    }
}

impl ClaimRecovery {
    const fn tag(self) -> &'static str {
        match self {
            Self::Clear => "clear",
            Self::ScanOwed => "scanOwed",
            Self::ParkedForScan => "parkedForScan",
            Self::ParkedForReturn => "parkedForReturn",
        }
    }

    pub(super) fn observe(&mut self, event: ClaimRecoveryEvent) {
        if let Ok((next, _)) = Self::transition(self, event) {
            *self = next;
        }
    }
}

impl StateTag for ClaimRecovery {
    fn state_tag(&self) -> &'static str {
        self.tag()
    }

    fn is_terminal(&self) -> bool {
        false
    }
}

impl StateMachine for ClaimRecovery {
    type State = Self;
    type Event = ClaimRecoveryEvent;
    type Effect = ();
    type Error = ClaimRecoveryRefusal;

    fn transition(state: &Self, event: Self::Event) -> StateMachineResult<Self> {
        use ClaimRecovery::{Clear, ParkedForReturn, ParkedForScan, ScanOwed};
        use ClaimRecoveryEvent::{
            ClaimEmpty, ClaimFailed, DrainReset, LocalRetryFinished, ScanFinished,
        };

        let next = match (*state, event) {
            (_, ClaimFailed) => ScanOwed,
            (_, DrainReset) => Clear,
            (ScanOwed, ClaimEmpty) => ParkedForScan,
            (Clear | ParkedForScan | ParkedForReturn, ClaimEmpty) => *state,
            (Clear | ParkedForReturn, ScanFinished { .. }) => *state,
            (
                ScanOwed | ParkedForScan,
                ScanFinished {
                    owes_local_return: true,
                },
            ) => ParkedForReturn,
            (
                ScanOwed | ParkedForScan,
                ScanFinished {
                    owes_local_return: false,
                },
            )
            | (
                ParkedForReturn,
                LocalRetryFinished {
                    owes_local_return: false,
                },
            ) => Clear,
            (Clear | ScanOwed | ParkedForScan, LocalRetryFinished { .. })
            | (
                ParkedForReturn,
                LocalRetryFinished {
                    owes_local_return: true,
                },
            ) => *state,
        };
        Ok((next, Vec::new()))
    }

    fn transitions() -> &'static [Transition] {
        use ClaimRecovery::{Clear, ParkedForReturn, ParkedForScan, ScanOwed};

        const TABLE: &[Transition] = &[
            edge(Clear, CLAIM_FAILED, ScanOwed),
            edge(Clear, CLAIM_EMPTY, Clear),
            edge(Clear, SCAN_FINISHED, Clear),
            edge(Clear, LOCAL_RETRY_FINISHED, Clear),
            edge(Clear, DRAIN_RESET, Clear),
            edge(ScanOwed, CLAIM_FAILED, ScanOwed),
            edge(ScanOwed, CLAIM_EMPTY, ParkedForScan),
            edge(ScanOwed, SCAN_FINISHED, Clear),
            edge(ScanOwed, SCAN_FINISHED, ParkedForReturn),
            edge(ScanOwed, LOCAL_RETRY_FINISHED, ScanOwed),
            edge(ScanOwed, DRAIN_RESET, Clear),
            edge(ParkedForScan, CLAIM_FAILED, ScanOwed),
            edge(ParkedForScan, CLAIM_EMPTY, ParkedForScan),
            edge(ParkedForScan, SCAN_FINISHED, Clear),
            edge(ParkedForScan, SCAN_FINISHED, ParkedForReturn),
            edge(ParkedForScan, LOCAL_RETRY_FINISHED, ParkedForScan),
            edge(ParkedForScan, DRAIN_RESET, Clear),
            edge(ParkedForReturn, CLAIM_FAILED, ScanOwed),
            edge(ParkedForReturn, CLAIM_EMPTY, ParkedForReturn),
            edge(ParkedForReturn, SCAN_FINISHED, ParkedForReturn),
            edge(ParkedForReturn, LOCAL_RETRY_FINISHED, Clear),
            edge(ParkedForReturn, LOCAL_RETRY_FINISHED, ParkedForReturn),
            edge(ParkedForReturn, DRAIN_RESET, Clear),
        ];
        TABLE
    }
}

const fn edge(from: ClaimRecovery, event: &'static str, to: ClaimRecovery) -> Transition {
    Transition {
        from: from.tag(),
        event,
        to: to.tag(),
        terminal: false,
    }
}

#[cfg(test)]
#[path = "claim_recovery_test.rs"]
mod tests;
