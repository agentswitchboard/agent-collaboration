use crate::Transition;

const fn due_transition(from: &'static str, event: &'static str, to: &'static str) -> Transition {
    Transition {
        from,
        event,
        to,
        terminal: false,
    }
}

pub(super) const DUE_DRAIN_TRANSITIONS: &[Transition] = &[
    due_transition("idle", "reply_accepted", "handed_off"),
    due_transition("idle", "alarm_fired", "draining"),
    due_transition("handed_off", "reply_accepted", "handed_off"),
    due_transition("handed_off", "alarm_fired", "handed_off"),
    due_transition("handed_off", "handoff_started", "draining"),
    due_transition("handed_off", "handoff_dropped", "idle"),
    due_transition("handed_off", "handoff_dropped", "handed_off"),
    due_transition("draining", "reply_accepted", "draining"),
    due_transition("draining", "alarm_fired", "draining"),
    due_transition("draining", "full_pass_made_progress", "waiting_for_yield"),
    due_transition("draining", "full_pass_made_progress", "publishing_alarm"),
    due_transition("draining", "full_pass_made_no_progress", "publishing_alarm"),
    due_transition("draining", "short_pass_finished", "waiting_for_yield"),
    due_transition("draining", "short_pass_finished", "publishing_alarm"),
    due_transition("draining", "drain_faulted", "stopped_on_fault"),
    due_transition("draining", "handoff_dropped", "idle"),
    due_transition("draining", "handoff_dropped", "handed_off"),
    due_transition("waiting_for_yield", "reply_accepted", "waiting_for_yield"),
    due_transition("waiting_for_yield", "alarm_fired", "waiting_for_yield"),
    due_transition("waiting_for_yield", "host_yielded", "draining"),
    due_transition("waiting_for_yield", "host_yield_failed", "stopped_on_fault"),
    due_transition("waiting_for_yield", "handoff_dropped", "idle"),
    due_transition("waiting_for_yield", "handoff_dropped", "handed_off"),
    due_transition("publishing_alarm", "reply_accepted", "publishing_alarm"),
    due_transition("publishing_alarm", "alarm_fired", "publishing_alarm"),
    due_transition("publishing_alarm", "alarm_publication_settled", "idle"),
    due_transition(
        "publishing_alarm",
        "alarm_publication_settled",
        "handed_off",
    ),
    due_transition("publishing_alarm", "handoff_dropped", "idle"),
    due_transition("publishing_alarm", "handoff_dropped", "handed_off"),
    due_transition("stopped_on_fault", "reply_accepted", "stopped_on_fault"),
    due_transition("stopped_on_fault", "alarm_fired", "stopped_on_fault"),
    due_transition("stopped_on_fault", "alarm_publication_settled", "idle"),
    due_transition(
        "stopped_on_fault",
        "alarm_publication_settled",
        "handed_off",
    ),
    due_transition("stopped_on_fault", "handoff_dropped", "idle"),
    due_transition("stopped_on_fault", "handoff_dropped", "handed_off"),
];
