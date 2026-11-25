use state_machine_core::Transition;

macro_rules! edges {
    ($($from:literal, $event:ident => $($to:literal)|+;)*) => {
        &[$($(Transition {
            from: $from,
            event: stringify!($event),
            to: $to,
            terminal: is_terminal($to),
        },)+)*]
    };
}

const fn is_terminal(state: &str) -> bool {
    matches!(state.as_bytes(), b"refused" | b"launch_unknown" | b"ended")
}

pub(super) const TRANSITIONS: &[Transition] = edges! {
    "asked", advance => "launching" | "refused";
    "asked", recover => "launching" | "refused";
    "launching", recover => "launch_unknown";
    "launching", launched => "registering_need_create" | "launching_allocated" | "closing" | "ended" | "refused" | "launch_unknown";
    "launching_allocated", advance => "launching_allocated" | "launching_first_input_intended" | "closing";
    "launching_allocated", recover => "launch_unknown";
    "launching_first_input_intended", advance => "launching_first_input_intended" | "closing";
    "launching_first_input_intended", recover => "launching_first_input_intended" | "closing";
    "launching_first_input_intended", first_input_answered => "launching_first_input_intended" | "registering_need_create" | "launch_unknown" | "closing";
    "registering_need_create", advance => "registering_need_create" | "registering_create_intended" | "closing";
    "registering_need_create", recover => "registering_need_create" | "registering_create_intended" | "closing";
    "registering_create_intended", advance => "registering_create_intended" | "registering_blocked" | "closing";
    "registering_create_intended", recover => "registering_create_intended" | "registering_blocked" | "closing";
    "registering_create_intended", create_answered => "registering_create_intended" | "registering_bind_intended" | "registering_blocked" | "closing";
    "registering_bind_intended", advance => "registering_bind_intended" | "closing";
    "registering_bind_intended", recover => "registering_bind_intended" | "closing";
    "registering_bind_intended", bind_answered => "registering_bind_intended" | "open" | "registering_blocked" | "closing";
    "registering_blocked", advance => "registering_blocked" | "closing";
    "registering_blocked", recover => "registering_blocked" | "closing";
    "open", advance => "open" | "closing";
    "open", recover => "open" | "closing";
    "open", end_for_good => "closing";
    "closing", advance => "closing";
    "closing", recover => "closing";
    "closing", harness_close_answered => "closing" | "ended";
    "closing", central_end_answered => "closing" | "ended";
    "ended", advance => "ended";
    "ended", recover => "ended";
    "refused", advance => "refused";
    "refused", recover => "refused";
    "launch_unknown", advance => "launch_unknown";
    "launch_unknown", recover => "launch_unknown";
};
