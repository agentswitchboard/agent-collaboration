//! Install a different run on the same fixture carrier before the driver's write.

use std::{
    io,
    pin::Pin,
    task::{Context, Poll, ready},
};

use central_link_protocol::{CentralCall, DataFrame, LinkFrame};
use tokio::io::AsyncWrite;
use tungstenite::protocol::frame::{
    Frame,
    coding::{Data, OpCode},
};
use workspace_ids::RouterRunId;

use super::packet_flight::decode_segment;

pub(super) enum RegistrationPreface {
    Waiting(RouterRunId),
    Writing(io::Cursor<Vec<u8>>),
}

impl RegistrationPreface {
    /// Write the fixture prefix first. Only the driver's original bytes are
    /// attributed to its production writer when this method completes.
    pub(super) fn poll_before_write(
        &mut self,
        mut writer: Pin<&mut impl AsyncWrite>,
        context: &mut Context<'_>,
        original: &[u8],
    ) -> Poll<io::Result<bool>> {
        if let Self::Waiting(run) = self {
            let frames = decode_segment(original, true);
            let Some(registration) = frames.iter().find_map(|frame| {
                let LinkFrame::Data(DataFrame::CallCentral { request, .. }) = frame else {
                    return None;
                };
                match request.as_ref() {
                    CentralCall::RegisterRun(registration) => Some(registration),
                    _ => None,
                }
            }) else {
                return Poll::Ready(Ok(false));
            };
            let mut frame = Frame::message(
                serde_json::json!({
                    "kind":"callCentral",
                    "correlationId": "019f0000-0000-7000-8000-00000000ce82",
                    "request": { "call":"registerRun", "envelope": {
                        "target": registration.target, "body": { "routerRun": run }
                    }}
                })
                .to_string(),
                OpCode::Data(Data::Text),
                true,
            );
            frame.header_mut().mask = Some([0x91, 0xb3, 0x4f, 0x2d]);
            let mut bytes = Vec::new();
            frame.format(&mut bytes).map_err(io::Error::other)?;
            *self = Self::Writing(io::Cursor::new(bytes));
        }

        if let Self::Writing(frame) = self {
            loop {
                let offset = usize::try_from(frame.position()).map_err(io::Error::other)?;
                let Some(remaining) = frame.get_ref().get(offset..) else {
                    return Poll::Ready(Err(io::Error::other("fixture prefix position")));
                };
                if remaining.is_empty() {
                    return Poll::Ready(Ok(true));
                }
                let wrote = ready!(writer.as_mut().poll_write(context, remaining))?;
                if wrote == 0 {
                    return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
                }
                frame.set_position(
                    frame.position() + u64::try_from(wrote).map_err(io::Error::other)?,
                );
            }
        }
        Poll::Ready(Ok(false))
    }
}
