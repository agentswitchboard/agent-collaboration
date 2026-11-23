//! Shared input limit for Central's two WebSocket hosts.

pub const LINK_MAX_TEXT_BYTES: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LinkSizeRefusal {
    MessageTooBig,
}

impl LinkSizeRefusal {
    pub const fn check_length(bytes: usize) -> Result<(), Self> {
        if bytes > LINK_MAX_TEXT_BYTES {
            Err(Self::MessageTooBig)
        } else {
            Ok(())
        }
    }

    pub const fn code(self) -> u16 {
        match self {
            Self::MessageTooBig => 1009,
        }
    }

    pub const fn reason(self) -> &'static str {
        match self {
            Self::MessageTooBig => "link message too big",
        }
    }
}
