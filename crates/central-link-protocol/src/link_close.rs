//! Private WebSocket codes for machine authority loss.

#[derive(
    Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize, schemars::JsonSchema,
)]
#[serde(rename_all = "camelCase")]
#[repr(u16)]
pub enum MachineLinkCloseCode {
    Revoked = 4101,
    Expired = 4102,
    Inactive = 4103,
}

impl MachineLinkCloseCode {
    #[must_use]
    pub const fn from_code(code: u16) -> Option<Self> {
        match code {
            4101 => Some(Self::Revoked),
            4102 => Some(Self::Expired),
            4103 => Some(Self::Inactive),
            _ => None,
        }
    }

    #[must_use]
    pub const fn code(self) -> u16 {
        self as u16
    }
}

#[cfg(test)]
#[path = "link_close_test.rs"]
mod tests;
