//! Mechanics shared by harnesses while opening a provider session.

mod lifecycle_route;
mod opening_instance;
mod opening_policy;

pub use lifecycle_route::LifecycleRoute;
pub use opening_instance::{OpeningInstance, OpeningOffer};
pub use opening_policy::{
    OpeningEvent, OpeningPhase, OpeningPolicy, WrongOpeningPhase, activation_refusal,
};
