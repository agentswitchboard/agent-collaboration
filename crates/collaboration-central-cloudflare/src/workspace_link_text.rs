//! The physical Workerd write takes protocol-checked text only.

use central_link_protocol::{CentralEgress, DataFrame, LinkFrame, encode_central_egress};
use worker::{Result, WebSocket};

pub(super) fn send(socket: &WebSocket, frame: DataFrame) -> Result<()> {
    let encoded = encode_central_egress(&LinkFrame::Data(frame))
        .map_err(|failure| worker::Error::RustError(failure.to_string()))?;
    write_central_text(socket, encoded)
}

pub(super) fn write_central_text(socket: &WebSocket, encoded: CentralEgress) -> Result<()> {
    let (text, substituted_bytes) = encoded.into_parts();
    if let Some(bytes) = substituted_bytes {
        worker::console_warn!(
            "central substituted an oversized link reply ({} bytes)",
            bytes
        );
    }
    socket.send_with_str(text)
}
