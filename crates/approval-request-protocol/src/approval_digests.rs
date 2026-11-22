//! Exact JSON bytes and digests for approval Raises.
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ApprovalDigestError {
    #[error("approval data could not be encoded as JSON")]
    Serialization,
    #[error("canonical approval JSON accepts only integer number tokens")]
    NonInteger,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("an approval digest must be unpadded base64url for exactly 32 bytes")]
pub struct InvalidApprovalDigest;

/// Canonical JSON for the approval seam, using the signed-intent rules.
pub fn canonical_json(value: &Value) -> Result<String, ApprovalDigestError> {
    let mut output = String::new();
    write_canonical(value, &mut output)?;
    Ok(output)
}

fn write_canonical(value: &Value, output: &mut String) -> Result<(), ApprovalDigestError> {
    match value {
        Value::Null | Value::Bool(_) | Value::String(_) => {
            output.push_str(
                &serde_json::to_string(value).map_err(|_| ApprovalDigestError::Serialization)?,
            );
        }
        Value::Number(number) => {
            if !(number.is_i64() || number.is_u64()) {
                return Err(ApprovalDigestError::NonInteger);
            }
            output.push_str(&number.to_string());
        }
        Value::Array(items) => {
            output.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                write_canonical(item, output)?;
            }
            output.push(']');
        }
        Value::Object(members) => {
            let mut entries = members.iter().collect::<Vec<_>>();
            entries.sort_unstable_by(|(left, _), (right, _)| {
                left.encode_utf16().cmp(right.encode_utf16())
            });
            output.push('{');
            for (index, (key, member)) in entries.into_iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                output.push_str(
                    &serde_json::to_string(key).map_err(|_| ApprovalDigestError::Serialization)?,
                );
                output.push(':');
                write_canonical(member, output)?;
            }
            output.push('}');
        }
    }
    Ok(())
}

pub fn sha256_url(bytes: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(bytes))
}

macro_rules! approval_digest {
    ($(#[$attribute:meta])* $digest:ident) => {
        $(#[$attribute])*
        #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
        pub struct $digest(String);

        impl $digest {
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }

        }

        impl TryFrom<String> for $digest {
            type Error = InvalidApprovalDigest;

            fn try_from(encoded: String) -> Result<Self, Self::Error> {
                if encoded.len() != 43 {
                    return Err(InvalidApprovalDigest);
                }
                let bytes = URL_SAFE_NO_PAD.decode(&encoded).map_err(|_| InvalidApprovalDigest)?;
                if bytes.len() == 32 && URL_SAFE_NO_PAD.encode(&bytes) == encoded {
                    Ok(Self(encoded))
                } else {
                    Err(InvalidApprovalDigest)
                }
            }
        }

        impl From<$digest> for String {
            fn from(digest: $digest) -> Self {
                digest.0
            }
        }
    };
}

approval_digest!(
    /// Identity of the complete admitted Raise body.
    RequestContentsDigest
);
approval_digest!(
    /// Identity of the exact invocation bytes held on the Mac.
    InvocationDigest
);

impl InvocationDigest {
    #[must_use]
    pub fn from_bound_bytes(bytes: &[u8]) -> Self {
        Self(sha256_url(bytes))
    }
}

impl RequestContentsDigest {
    pub(crate) fn from_wire(value: &impl Serialize) -> Result<Self, ApprovalDigestError> {
        digest_wire(value).map(Self)
    }
}

fn digest_wire(value: &impl Serialize) -> Result<String, ApprovalDigestError> {
    let json = serde_json::to_value(value).map_err(|_| ApprovalDigestError::Serialization)?;
    Ok(sha256_url(canonical_json(&json)?.as_bytes()))
}
