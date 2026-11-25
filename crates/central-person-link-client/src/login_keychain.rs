use core_foundation::data::CFData;
use security_framework::{
    item::{
        ItemAddOptions, ItemAddValue, ItemClass, ItemSearchOptions, ItemUpdateOptions,
        ItemUpdateValue, Location, update_item,
    },
    os::macos::keychain::{SecKeychain, SecPreferencesDomain},
};
use zeroize::Zeroizing;

use crate::{
    FleetCredentialError, FleetCredentialStore, FleetTarget, dpop_proof::sha256_base64url,
};

#[cfg(debug_assertions)]
const FLEET_SERVICE: &str = "dev.shravansunder.codex-router.central-login.debug";
#[cfg(not(debug_assertions))]
const FLEET_SERVICE: &str = "dev.shravansunder.codex-router.central-login";

const ITEM_NOT_FOUND: i32 = -25300;
const DUPLICATE_ITEM: i32 = -25299;

pub struct LoginKeychainCredential {
    account: String,
}

impl LoginKeychainCredential {
    pub fn for_target(target: &FleetTarget) -> Self {
        Self {
            account: format!("{}|{}", target.issuer(), target.client_id()),
        }
    }

    fn load_from(
        &self,
        keychain: &SecKeychain,
    ) -> Result<Option<Zeroizing<Vec<u8>>>, FleetCredentialError> {
        match keychain.find_generic_password(FLEET_SERVICE, &self.account) {
            Ok((secret, _)) => Ok(Some(Zeroizing::new(secret.as_ref().to_vec()))),
            Err(error) if error.code() == ITEM_NOT_FOUND => Ok(None),
            Err(_) => Err(FleetCredentialError::Read),
        }
    }

    fn save_in(
        &self,
        keychain: &SecKeychain,
        expected: Option<&[u8]>,
        replacement: &[u8],
    ) -> Result<(), FleetCredentialError> {
        // The label is an equality key for the item value. Security matches and replaces both
        // attributes in a single operation, including across separately running CLI processes.
        let replacement_label = version_label(replacement);
        let replacement_data = CFData::from_buffer(replacement);
        let result = match expected {
            Some(expected) => {
                let mut search = ItemSearchOptions::new();
                search
                    .keychains(std::slice::from_ref(keychain))
                    .class(ItemClass::generic_password())
                    .service(FLEET_SERVICE)
                    .account(&self.account)
                    .label(&version_label(expected));
                let mut update = ItemUpdateOptions::new();
                update
                    .set_label(replacement_label)
                    .set_value(ItemUpdateValue::Data(replacement_data));
                update_item(&search, &update)
            }
            None => ItemAddOptions::new(ItemAddValue::Data {
                class: ItemClass::generic_password(),
                data: replacement_data,
            })
            .set_location(Location::FileKeychain(keychain.clone()))
            .set_service(FLEET_SERVICE)
            .set_account_name(&self.account)
            .set_label(replacement_label)
            .add(),
        };
        result.map_err(|error| match error.code() {
            ITEM_NOT_FOUND | DUPLICATE_ITEM => FleetCredentialError::Changed,
            _ => FleetCredentialError::Save,
        })
    }
}

impl FleetCredentialStore for LoginKeychainCredential {
    fn load(&self) -> Result<Option<Zeroizing<Vec<u8>>>, FleetCredentialError> {
        self.load_from(&login_keychain().map_err(|_| FleetCredentialError::Read)?)
    }

    fn compare_and_save(
        &self,
        expected: Option<&[u8]>,
        replacement: &[u8],
    ) -> Result<(), FleetCredentialError> {
        self.save_in(
            &login_keychain().map_err(|_| FleetCredentialError::Save)?,
            expected,
            replacement,
        )
    }
}

fn version_label(credential: &[u8]) -> String {
    format!("{FLEET_SERVICE}:{}", sha256_base64url(credential))
}

fn login_keychain() -> security_framework::base::Result<SecKeychain> {
    SecKeychain::default_for_domain(SecPreferencesDomain::User)
}

#[cfg(test)]
#[path = "login_keychain_test.rs"]
mod tests;
