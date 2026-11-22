//! Exact route identity shared by the ordered transport and callback routing.

use provider_session_machines::session_owner::DispatchAuthorization;

pub use switchboard_harness_contracts::TransportRun;

pub(crate) fn authorization_run(authorization: &DispatchAuthorization) -> TransportRun {
    TransportRun {
        incarnation: authorization.incarnation,
        generation: authorization.run,
    }
}
