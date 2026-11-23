use central_engine_model::Transition;

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

pub(crate) const TRANSITIONS: &[Transition] = &[
    edge("missing", "create", "missing", false),
    edge("missing", "create", "reserved", false),
    edge("missing", "bind", "missing", false),
    edge("missing", "end", "missing", false),
    edge("reserved", "create", "reserved", false),
    edge("reserved", "create", "abandoned", true),
    edge("reserved", "bind", "reserved", false),
    edge("reserved", "bind", "active", false),
    edge("reserved", "bind", "abandoned", true),
    edge("reserved", "end", "reserved", false),
    edge("reserved", "end", "abandoned", true),
    edge("active", "create", "active", false),
    edge("active", "create", "ended", true),
    edge("active", "bind", "active", false),
    edge("active", "bind", "ended", true),
    edge("active", "end", "active", false),
    edge("active", "end", "ended", true),
    edge("abandoned", "create", "abandoned", true),
    edge("abandoned", "bind", "abandoned", true),
    edge("abandoned", "end", "abandoned", true),
    edge("ended", "create", "ended", true),
    edge("ended", "bind", "ended", true),
    edge("ended", "end", "ended", true),
];
