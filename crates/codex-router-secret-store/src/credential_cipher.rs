//! Internal AES-GCM primitives shared by the two credential formats.

use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, KeyInit, Payload},
};

use crate::{keychain_data_key::PooledCredentialDataKey, model::SecretStoreError};

pub(crate) const AES_GCM_NONCE_LENGTH: usize = 12;
pub(crate) const AES_GCM_TAG_LENGTH: usize = 16;

pub(crate) fn encrypt(
    data_key: &PooledCredentialDataKey,
    associated_data: &[u8],
    plaintext: &[u8],
) -> Result<([u8; AES_GCM_NONCE_LENGTH], Vec<u8>), SecretStoreError> {
    let mut nonce = [0; AES_GCM_NONCE_LENGTH];
    getrandom::fill(&mut nonce).map_err(SecretStoreError::RandomnessUnavailable)?;
    let ciphertext = encrypt_with_nonce(data_key, associated_data, nonce, plaintext)?;
    Ok((nonce, ciphertext))
}

fn encrypt_with_nonce(
    data_key: &PooledCredentialDataKey,
    associated_data: &[u8],
    nonce: [u8; AES_GCM_NONCE_LENGTH],
    plaintext: &[u8],
) -> Result<Vec<u8>, SecretStoreError> {
    cipher(data_key)?
        .encrypt(
            &Nonce::from(nonce),
            Payload {
                msg: plaintext,
                aad: associated_data,
            },
        )
        .map_err(|_| SecretStoreError::CredentialAuthenticationFailed)
}

pub(crate) fn decrypt(
    data_key: &PooledCredentialDataKey,
    associated_data: &[u8],
    nonce: [u8; AES_GCM_NONCE_LENGTH],
    ciphertext: &[u8],
) -> Result<Vec<u8>, SecretStoreError> {
    cipher(data_key)?
        .decrypt(
            &Nonce::from(nonce),
            Payload {
                msg: ciphertext,
                aad: associated_data,
            },
        )
        .map_err(|_| SecretStoreError::CredentialAuthenticationFailed)
}

fn cipher(data_key: &PooledCredentialDataKey) -> Result<Aes256Gcm, SecretStoreError> {
    Aes256Gcm::new_from_slice(data_key.bytes())
        .map_err(|_| SecretStoreError::CipherInitializationFailed)
}

#[cfg(test)]
pub(crate) fn encrypt_for_fixture(
    data_key: &PooledCredentialDataKey,
    associated_data: &[u8],
    nonce: [u8; AES_GCM_NONCE_LENGTH],
    plaintext: &[u8],
) -> Result<Vec<u8>, SecretStoreError> {
    encrypt_with_nonce(data_key, associated_data, nonce, plaintext)
}
