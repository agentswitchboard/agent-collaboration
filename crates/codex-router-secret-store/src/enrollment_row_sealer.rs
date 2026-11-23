//! Enrollment-row encryption using the Router's existing Keychain item.

use std::path::Path;

use switchboard_machine_enrollment::{
    BindingGeneration, EnrollmentId, MachineId, OrganizationId, SecretValue, SignInDeployment,
};
use zeroize::Zeroizing;

use crate::{
    credential_cipher::{self, AES_GCM_NONCE_LENGTH, AES_GCM_TAG_LENGTH},
    credential_store_lock::{CredentialStoreLock, CredentialStoreLockMode},
    file_backend::FileSecretStore,
    keychain_data_key::{
        KeychainAccess, PooledCredentialDataKey, load_enrollment_row_data_key,
        with_platform_keychain_for_production,
    },
    model::SecretStoreError,
};

const FORMAT_VERSION: u8 = 1;
const ASSOCIATED_DATA_DOMAIN: &[u8] = b"switchboard.enrollment-row\0";
const HEADER_LENGTH: usize = 1 + AES_GCM_NONCE_LENGTH;

/// The durable identity of one active enrollment row.
pub struct EnrollmentRowContext {
    pub machine_id: MachineId,
    pub organization_id: OrganizationId,
    pub enrollment_id: EnrollmentId,
    pub deployment: SignInDeployment,
    pub generation: BindingGeneration,
}

/// Key capability restricted to sealing and opening enrollment refresh tokens.
pub struct EnrollmentRowSealer(PooledCredentialDataKey);

impl EnrollmentRowSealer {
    /// Acquires the key using the current row's sealed-byte presence.
    pub fn acquire(
        secret_root: &Path,
        sealed_bytes_present: bool,
    ) -> Result<Self, SecretStoreError> {
        with_platform_keychain_for_production(|keychain| {
            Self::acquire_with_keychain(secret_root, sealed_bytes_present, keychain)
        })
    }

    /// Acquires the same capability through an injected Keychain boundary.
    pub fn acquire_with_keychain(
        secret_root: &Path,
        sealed_bytes_present: bool,
        keychain: &dyn KeychainAccess,
    ) -> Result<Self, SecretStoreError> {
        if sealed_bytes_present {
            let file_store = FileSecretStore::open_read_only(secret_root)?;
            return load_enrollment_row_data_key(&file_store, keychain, true).map(Self);
        }

        let file_store = FileSecretStore::open(secret_root)?;
        // The store-id marker is shared with the older opener. Serialize its
        // first creation using the same lock before addressing the Keychain.
        let _store_lock =
            CredentialStoreLock::acquire(file_store.root(), CredentialStoreLockMode::Exclusive)?;
        load_enrollment_row_data_key(&file_store, keychain, false).map(Self)
    }

    /// Seals the refresh token against its exact row identity and generation.
    pub fn seal(
        &self,
        context: &EnrollmentRowContext,
        credential: &SecretValue,
    ) -> Result<Vec<u8>, SecretStoreError> {
        let (nonce, ciphertext) = credential_cipher::encrypt(
            &self.0,
            &associated_data(context)?,
            credential.expose_secret().as_bytes(),
        )?;
        let mut sealed = Vec::with_capacity(HEADER_LENGTH + ciphertext.len());
        sealed.push(FORMAT_VERSION);
        sealed.extend_from_slice(&nonce);
        sealed.extend_from_slice(&ciphertext);
        Ok(sealed)
    }

    /// Authenticates the row and opens its token into zeroizing memory.
    pub fn open(
        &self,
        context: &EnrollmentRowContext,
        sealed: &[u8],
    ) -> Result<SecretValue, SecretStoreError> {
        let (&version, payload) = sealed
            .split_first()
            .ok_or(SecretStoreError::InvalidCredentialEnvelope)?;
        let (nonce, ciphertext) = payload
            .split_at_checked(AES_GCM_NONCE_LENGTH)
            .ok_or(SecretStoreError::InvalidCredentialEnvelope)?;
        if version != FORMAT_VERSION || ciphertext.len() <= AES_GCM_TAG_LENGTH {
            return Err(SecretStoreError::InvalidCredentialEnvelope);
        }
        let nonce = nonce
            .try_into()
            .map_err(|_| SecretStoreError::InvalidCredentialEnvelope)?;
        let plaintext = Zeroizing::new(credential_cipher::decrypt(
            &self.0,
            &associated_data(context)?,
            nonce,
            ciphertext,
        )?);
        let token = std::str::from_utf8(&plaintext)
            .map_err(|_| SecretStoreError::InvalidCredentialEncoding)?;
        SecretValue::new(token.to_owned()).ok_or(SecretStoreError::InvalidCredentialEncoding)
    }
}

fn associated_data(context: &EnrollmentRowContext) -> Result<Vec<u8>, SecretStoreError> {
    let mut encoded = ASSOCIATED_DATA_DOMAIN.to_vec();
    encoded.push(FORMAT_VERSION);
    for field in [
        context.machine_id.to_string().as_str(),
        context.organization_id.to_string().as_str(),
        context.enrollment_id.to_string().as_str(),
        context.deployment.origin().as_str(),
    ] {
        let length =
            u32::try_from(field.len()).map_err(|_| SecretStoreError::InvalidCredentialEnvelope)?;
        encoded.extend_from_slice(&length.to_be_bytes());
        encoded.extend_from_slice(field.as_bytes());
    }
    encoded.extend_from_slice(&context.generation.get().to_be_bytes());
    Ok(encoded)
}

#[cfg(test)]
#[path = "enrollment_row_sealer_test.rs"]
mod tests;
