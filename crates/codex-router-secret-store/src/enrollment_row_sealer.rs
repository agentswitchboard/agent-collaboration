//! Enrollment-row encryption using the Router's existing Keychain item.

use std::{fmt, path::Path};

use uuid::{Uuid, Variant};
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
    machine_id: Uuid,
    organization_id: Uuid,
    enrollment_id: Uuid,
    deployment_origin: String,
    generation: u64,
}

impl EnrollmentRowContext {
    /// Builds the context from canonical IDs and the adapter's validated origin string.
    pub fn new(
        machine_id: &str,
        organization_id: &str,
        enrollment_id: &str,
        deployment_origin: &str,
        generation: u64,
    ) -> Result<Self, SecretStoreError> {
        if deployment_origin.is_empty() {
            return Err(SecretStoreError::InvalidEnrollmentRowContext);
        }
        Ok(Self {
            machine_id: parse_canonical_id(machine_id)?,
            organization_id: parse_canonical_id(organization_id)?,
            enrollment_id: parse_canonical_id(enrollment_id)?,
            deployment_origin: deployment_origin.to_owned(),
            generation,
        })
    }
}

fn parse_canonical_id(value: &str) -> Result<Uuid, SecretStoreError> {
    let id = Uuid::try_parse(value).map_err(|_| SecretStoreError::InvalidEnrollmentRowContext)?;
    if id.get_version_num() != 7
        || id.get_variant() != Variant::RFC4122
        || id.hyphenated().to_string() != value
    {
        return Err(SecretStoreError::InvalidEnrollmentRowContext);
    }
    Ok(id)
}

/// The enrollment row's nonempty UTF-8 refresh credential, zeroized on drop.
pub struct EnrollmentRowCredential(Zeroizing<Vec<u8>>);

impl EnrollmentRowCredential {
    /// Copies a token directly into zeroizing storage.
    #[must_use]
    pub fn copy_from(token: &str) -> Option<Self> {
        if token.is_empty() {
            return None;
        }
        let mut bytes = Zeroizing::new(Vec::with_capacity(token.len()));
        bytes.extend_from_slice(token.as_bytes());
        Some(Self(bytes))
    }

    /// Exposes the bytes for the enrollment adapter's credential handoff.
    #[must_use]
    pub fn expose_bytes(&self) -> &[u8] {
        self.0.as_slice()
    }
}

impl fmt::Debug for EnrollmentRowCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("[redacted]")
    }
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
        credential: &EnrollmentRowCredential,
    ) -> Result<Vec<u8>, SecretStoreError> {
        let (nonce, ciphertext) = credential_cipher::encrypt(
            &self.0,
            &associated_data(context)?,
            credential.expose_bytes(),
        )?;
        Ok(encode_sealed(&nonce, &ciphertext))
    }

    /// Authenticates the row and opens its token into zeroizing memory.
    pub fn open(
        &self,
        context: &EnrollmentRowContext,
        sealed: &[u8],
    ) -> Result<EnrollmentRowCredential, SecretStoreError> {
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
        std::str::from_utf8(&plaintext).map_err(|_| SecretStoreError::InvalidCredentialEncoding)?;
        Ok(EnrollmentRowCredential(plaintext))
    }
}

fn encode_sealed(nonce: &[u8; AES_GCM_NONCE_LENGTH], ciphertext: &[u8]) -> Vec<u8> {
    let mut sealed = Vec::with_capacity(HEADER_LENGTH + ciphertext.len());
    sealed.push(FORMAT_VERSION);
    sealed.extend_from_slice(nonce);
    sealed.extend_from_slice(ciphertext);
    sealed
}

fn associated_data(context: &EnrollmentRowContext) -> Result<Vec<u8>, SecretStoreError> {
    let mut encoded = ASSOCIATED_DATA_DOMAIN.to_vec();
    encoded.push(FORMAT_VERSION);
    for field in [
        context.machine_id.to_string().as_str(),
        context.organization_id.to_string().as_str(),
        context.enrollment_id.to_string().as_str(),
        context.deployment_origin.as_str(),
    ] {
        let length =
            u32::try_from(field.len()).map_err(|_| SecretStoreError::InvalidCredentialEnvelope)?;
        encoded.extend_from_slice(&length.to_be_bytes());
        encoded.extend_from_slice(field.as_bytes());
    }
    encoded.extend_from_slice(&context.generation.to_be_bytes());
    Ok(encoded)
}

#[cfg(test)]
#[path = "enrollment_row_sealer_test.rs"]
mod tests;
