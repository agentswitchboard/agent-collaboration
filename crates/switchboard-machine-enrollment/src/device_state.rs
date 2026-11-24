//! Timing and settlement of a single device sign-in.
#![deny(clippy::wildcard_enum_match_arm)]

use std::{marker::PhantomData, time::Duration};

use router_clock::ClockInstant;
use state_machine_core::{
    EventTag, RefusalTag, StateMachine, StateMachineResult, StateTag, Transition, TransitionError,
};

use crate::{
    ApprovedEnrollmentSaveFailure, EnrollmentEnd, EnrollmentReceipt, PollAnswer, TokenGrant,
};

const FINAL_POLL_GRACE: Duration = Duration::from_secs(2);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct DevicePollWindow {
    interval: Duration,
    next_poll: ClockInstant,
    deadline: ClockInstant,
}

impl DevicePollWindow {
    pub(super) fn wake_at(self) -> ClockInstant {
        self.next_poll.min(self.deadline)
    }

    pub(super) fn dispatch_allowed(self, now: ClockInstant) -> bool {
        now < self.deadline
    }

    pub(super) fn reply_deadline(self) -> ClockInstant {
        self.deadline.after(FINAL_POLL_GRACE)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DeviceSignInState {
    Starting,
    Waiting(DevicePollWindow),
    Polling(DevicePollWindow),
    Saving,
    Enrolled(EnrollmentReceipt),
    Ended(DeviceSignInEnding),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DeviceSignInEnding {
    SignIn(EnrollmentEnd),
    AfterApproval(ApprovedEnrollmentSaveFailure),
}

impl StateTag for DeviceSignInState {
    fn state_tag(&self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Waiting(_) => "waiting",
            Self::Polling(_) => "polling",
            Self::Saving => "saving",
            Self::Enrolled(_) => "enrolled",
            Self::Ended(_) => "ended",
        }
    }

    fn is_terminal(&self) -> bool {
        matches!(self, Self::Enrolled(_) | Self::Ended(_))
    }
}

#[derive(Clone, Copy)]
pub(super) enum DeviceSignInEvent<'reply> {
    Started {
        dispatched: ClockInstant,
        arrived: ClockInstant,
        interval: Duration,
        expires_in: Duration,
    },
    Woke(ClockInstant),
    PollSettled {
        now: ClockInstant,
        reply: Option<&'reply PollAnswer>,
    },
    SaveSettled(Result<EnrollmentReceipt, ApprovedEnrollmentSaveFailure>),
}

impl EventTag for DeviceSignInEvent<'_> {
    fn event_tag(&self) -> &'static str {
        match self {
            Self::Started { .. } => "started",
            Self::Woke(_) => "woke",
            Self::PollSettled { .. } => "pollSettled",
            Self::SaveSettled(_) => "saveSettled",
        }
    }
}

pub(super) struct DeviceSignInMachine<'reply>(PhantomData<&'reply ()>);

#[derive(Debug)]
pub(super) enum DeviceSignInRefusal {}

impl RefusalTag for DeviceSignInRefusal {
    fn refusal_tag(&self) -> &'static str {
        match *self {}
    }
}

const TRANSITIONS: &[Transition] = &[
    edge("starting", "started", "waiting", false),
    edge("waiting", "woke", "waiting", false),
    edge("waiting", "woke", "polling", false),
    edge("waiting", "woke", "ended", true),
    edge("polling", "pollSettled", "waiting", false),
    edge("polling", "pollSettled", "saving", false),
    edge("polling", "pollSettled", "ended", true),
    edge("saving", "saveSettled", "enrolled", true),
    edge("saving", "saveSettled", "ended", true),
];

const fn edge(
    from: &'static str,
    event: &'static str,
    to: &'static str,
    terminal: bool,
) -> Transition {
    Transition {
        from,
        event,
        to,
        terminal,
    }
}

impl<'reply> StateMachine for DeviceSignInMachine<'reply> {
    type State = DeviceSignInState;
    type Event = DeviceSignInEvent<'reply>;
    type Effect = &'reply TokenGrant;
    type Error = DeviceSignInRefusal;

    fn transition(state: &Self::State, event: Self::Event) -> StateMachineResult<Self> {
        use DeviceSignInEvent as Event;
        use DeviceSignInState as State;

        let next = match (*state, event) {
            (
                State::Starting,
                Event::Started {
                    dispatched,
                    arrived,
                    interval,
                    expires_in,
                },
            ) => State::Waiting(DevicePollWindow {
                interval,
                next_poll: arrived.after(interval),
                deadline: dispatched.after(expires_in),
            }),
            (State::Waiting(window), Event::Woke(now)) => {
                if now >= window.deadline {
                    ended(EnrollmentEnd::Expired)
                } else if now >= window.next_poll {
                    State::Polling(window)
                } else {
                    State::Waiting(window)
                }
            }
            (State::Polling(window), Event::PollSettled { now, reply }) => {
                return Ok(settle_poll(window, now, reply));
            }
            (State::Saving, Event::SaveSettled(result)) => match result {
                Ok(receipt) => State::Enrolled(receipt),
                Err(error) => State::Ended(DeviceSignInEnding::AfterApproval(error)),
            },
            (
                State::Enrolled(_) | State::Ended(_),
                Event::Started { .. }
                | Event::Woke(_)
                | Event::PollSettled { .. }
                | Event::SaveSettled(_),
            ) => return Err(TransitionError::Stale),
            (
                State::Starting,
                Event::Woke(_) | Event::PollSettled { .. } | Event::SaveSettled(_),
            )
            | (
                State::Waiting(_),
                Event::Started { .. } | Event::PollSettled { .. } | Event::SaveSettled(_),
            )
            | (State::Polling(_), Event::Started { .. } | Event::Woke(_) | Event::SaveSettled(_))
            | (State::Saving, Event::Started { .. } | Event::Woke(_) | Event::PollSettled { .. }) =>
            {
                return Err(TransitionError::NotAllowed {
                    state: state.state_tag(),
                    event: event.event_tag(),
                });
            }
        };
        Ok((next, Vec::new()))
    }

    fn transitions() -> &'static [Transition] {
        TRANSITIONS
    }
}

fn settle_poll(
    mut window: DevicePollWindow,
    now: ClockInstant,
    reply: Option<&PollAnswer>,
) -> (DeviceSignInState, Vec<&TokenGrant>) {
    let delay = match reply {
        // The request began before expiry. A ready grant wins at expiry and
        // through the inclusive grace boundary. No new poll starts in grace.
        Some(PollAnswer::Enrolled(grant)) => {
            return if now <= window.reply_deadline() {
                (DeviceSignInState::Saving, vec![grant])
            } else {
                (ended(EnrollmentEnd::Expired), Vec::new())
            };
        }
        Some(
            PollAnswer::Pending
            | PollAnswer::SlowDown
            | PollAnswer::Unavailable { .. }
            | PollAnswer::Denied
            | PollAnswer::Expired
            | PollAnswer::Invalid { .. },
        )
        | None
            if now >= window.deadline =>
        {
            return (ended(EnrollmentEnd::Expired), Vec::new());
        }
        Some(PollAnswer::Pending) => window.interval,
        Some(PollAnswer::SlowDown) => {
            window.interval = window.interval.saturating_add(Duration::from_secs(5));
            window.interval
        }
        Some(PollAnswer::Unavailable { retry_after }) => retry_after.unwrap_or(window.interval),
        Some(PollAnswer::Denied) => return (ended(EnrollmentEnd::Denied), Vec::new()),
        Some(PollAnswer::Expired) | None => return (ended(EnrollmentEnd::Expired), Vec::new()),
        Some(PollAnswer::Invalid { reason }) => {
            return (ended(EnrollmentEnd::Invalid(*reason)), Vec::new());
        }
    };
    window.next_poll = now.after(delay);
    (DeviceSignInState::Waiting(window), Vec::new())
}

fn ended(why: EnrollmentEnd) -> DeviceSignInState {
    DeviceSignInState::Ended(DeviceSignInEnding::SignIn(why))
}

#[cfg(test)]
#[path = "device_state_test.rs"]
mod tests;
