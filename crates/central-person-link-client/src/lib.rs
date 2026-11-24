//! A person's reusable, authenticated connection to Central's workspace directory.

mod connection_machine;
mod error;
mod fleet_target;

pub use error::{FleetCredentialError, FleetTargetError, PersonLinkError};
pub use fleet_target::FleetTarget;

mod dpop_proof;
mod fleet_credential;
pub use fleet_credential::FleetCredentialStore;
#[cfg(target_os = "macos")]
mod login_keychain;
#[cfg(target_os = "macos")]
pub use login_keychain::LoginKeychainCredential;

fn clock_uuid(millis: i64) -> uuid::Uuid {
    let millis = u64::try_from(millis).unwrap_or_default();
    let nanos = u32::try_from(millis % 1_000).unwrap_or_default() * 1_000_000;
    uuid::Uuid::new_v7(uuid::Timestamp::from_unix(
        uuid::NoContext,
        millis / 1_000,
        nanos,
    ))
}

mod person_socket;
mod workspace_client;
pub use workspace_client::PersonWorkspaceClient;
