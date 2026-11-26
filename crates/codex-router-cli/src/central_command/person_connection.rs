use std::{path::Path, sync::Arc};

use central_person_link_client::PersonLinkClient;
use router_clock::RuntimeClock;

use super::CentralCommandError;

pub(super) async fn build_person_client(
    target: &Path,
    clock: Arc<dyn RuntimeClock>,
) -> Result<PersonLinkClient, CentralCommandError> {
    #[cfg(target_os = "macos")]
    {
        use central_person_link_client::{FleetTarget, LoginKeychainCredential};

        let path = target.to_owned();
        let target = tokio::task::spawn_blocking(move || FleetTarget::read(&path))
            .await
            .map_err(|_| CentralCommandError::LocalTask)??;
        let store = Arc::new(LoginKeychainCredential::for_target(&target));
        Ok(PersonLinkClient::new(target, store, clock)?)
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = (target, clock);
        std::future::ready(Err(central_person_link_client::PersonLinkError::from(
            central_person_link_client::FleetCredentialError::UnsupportedPlatform,
        )
        .into()))
        .await
    }
}
