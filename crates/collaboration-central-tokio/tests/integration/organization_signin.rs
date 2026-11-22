//! Modern private sign-in routes over the real native Unix listener and SQLite.
mod admission;
mod alarm_recovery;
mod causal_races;
mod deadlines;
mod floors;
mod issuer_order;
mod linearization;
mod list_workspaces;
mod machine_login;
mod observations;
mod post_turn;
mod provisioning;
mod records;
mod replay_repair;
mod rollback;
mod setup;
mod stamp_trace;
mod support;
mod trust_trace;

mod carry_edges;
mod metadata;
mod route_boundaries;

mod engine_admin;
mod engine_admin_support;
mod engine_failing_retry;
mod engine_owner_guards;
mod engine_stateful;
mod engine_stateful_attempts;
mod engine_unit_revival;
