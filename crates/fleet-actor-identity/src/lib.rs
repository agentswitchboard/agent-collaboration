//! Central actor records and session lifecycle decisions.
#![forbid(unsafe_code)]

mod authenticated_session;
mod person_actor_repository;
mod session_edges;
mod session_machine;
mod session_repository;
mod session_row;
mod session_service;

mod session_state;

pub use authenticated_session::{AuthenticatedSession, ScopedSession};
pub use person_actor_repository::PersonActorRepository;
pub use session_machine::SessionLifecycle;
pub use session_state::*;

pub use session_service::*;
