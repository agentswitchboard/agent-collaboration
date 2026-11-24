use zeroize::Zeroizing;

use crate::{FleetCredentialError, FleetCredentialStore, FleetTarget};

const FLEET_SERVICE: &str = "dev.switchboard.fleet-cli";
const ITEM_NOT_FOUND: i32 = -25300;

pub struct LoginKeychainCredential {
    account: String,
}

impl LoginKeychainCredential {
    pub fn for_target(target: &FleetTarget) -> Self {
        Self {
            account: format!("{}|{}", target.issuer(), target.client_id()),
        }
    }
}

impl FleetCredentialStore for LoginKeychainCredential {
    fn load(&self) -> Result<Zeroizing<Vec<u8>>, FleetCredentialError> {
        let keychain = login_keychain().map_err(|_| FleetCredentialError::Read)?;
        let (secret, _) = keychain
            .find_generic_password(FLEET_SERVICE, &self.account)
            .map_err(|error| {
                if error.code() == ITEM_NOT_FOUND {
                    FleetCredentialError::Missing
                } else {
                    FleetCredentialError::Read
                }
            })?;
        Ok(Zeroizing::new(secret.as_ref().to_vec()))
    }

    fn replace(&self, credential: &[u8]) -> Result<(), FleetCredentialError> {
        let keychain = login_keychain().map_err(|_| FleetCredentialError::Save)?;
        let (_, mut item) = keychain
            .find_generic_password(FLEET_SERVICE, &self.account)
            .map_err(|_| FleetCredentialError::Save)?;
        item.set_password(credential)
            .map_err(|_| FleetCredentialError::Save)
    }
}

fn login_keychain()
-> security_framework::base::Result<security_framework::os::macos::keychain::SecKeychain> {
    use security_framework::os::macos::keychain::{SecKeychain, SecPreferencesDomain};
    SecKeychain::default_for_domain(SecPreferencesDomain::User)
}
