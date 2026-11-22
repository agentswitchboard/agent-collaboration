//! Dedicated frame readers retain partial input while the connection router handles other work.
use crate::{AcpOutputSender, QueuedAcpFrame, bounded_acp_output, read_acp_frame, write_acp_frame};
use std::io;
use tokio::{
    io::{AsyncRead, AsyncWrite, BufReader},
    sync::mpsc,
};
use tokio_util::sync::CancellationToken;

pub struct AcpWireChannels {
    input: AcpOutputSender,
    output: mpsc::Receiver<QueuedAcpFrame>,
    closed: CancellationToken,
    pub(crate) peer_disconnected: CancellationToken,
}
pub struct AcpRouterChannels {
    pub input: mpsc::Receiver<QueuedAcpFrame>,
    pub output: AcpOutputSender,
    pub closed: CancellationToken,
}
pub fn acp_connection_channels() -> (AcpWireChannels, AcpRouterChannels) {
    let closed = CancellationToken::new();
    let (input, input_receiver) = bounded_acp_output(closed.clone());
    let (output, output_receiver) = bounded_acp_output(closed.clone());
    (
        AcpWireChannels {
            input,
            output: output_receiver,
            closed: closed.clone(),
            peer_disconnected: CancellationToken::new(),
        },
        AcpRouterChannels {
            input: input_receiver,
            output,
            closed,
        },
    )
}
fn peer_disconnected(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::BrokenPipe
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::NotConnected
            | io::ErrorKind::UnexpectedEof
    )
}

pub async fn run_acp_transport<TStream: AsyncRead + AsyncWrite + Unpin>(
    stream: TStream,
    mut channels: AcpWireChannels,
) -> io::Result<()> {
    let (read, mut write) = tokio::io::split(stream);
    let mut read = BufReader::new(read);
    let receive = async {
        loop {
            let frame = match read_acp_frame(&mut read).await {
                Ok(Some(frame)) => frame,
                Ok(None) => {
                    channels.peer_disconnected.cancel();
                    break Ok(());
                }
                Err(error) => {
                    if peer_disconnected(&error) {
                        channels.peer_disconnected.cancel();
                    }
                    break Err(error);
                }
            };
            #[cfg(feature = "test-support")]
            let hold_read = frame
                .get("method")
                .is_some_and(|method| method == "session/prompt");
            channels.input.send(frame).await?;
            #[cfg(feature = "test-support")]
            if hold_read && let Some(probe) = crate::test_support::binding_preservation::current().filter(|probe| probe.phase() == crate::test_support::binding_preservation::PreservationPhase::PeerWriteFailure) {
                // Keep the read half pending after dispatching the prompt so the real write error wins.
                let _wait_for_write = probe.peer_write_failed.cancelled().await;
                std::future::pending::<()>().await;
            }
        }
    };
    let transmit = async {
        while let Some(frame) = channels.output.recv().await {
            // Keep the queued-byte permit until the complete socket write finishes.
            #[cfg(feature = "test-support")]
            let write_probe = crate::test_support::binding_preservation::current().filter(|probe| {
                probe.phase() == crate::test_support::binding_preservation::PreservationPhase::PeerWriteFailure
                    && frame.get("method").is_some_and(|method| method == "session/update")
            });
            #[cfg(feature = "test-support")]
            if let Some(probe) = &write_probe {
                probe.update_waiting_to_write.cancel();
                probe.release_update_write.cancelled().await;
            }
            let result = write_acp_frame(&mut write, &frame).await;
            #[cfg(feature = "test-support")]
            if let Some(probe) = write_probe
                && result
                    .as_ref()
                    .is_err_and(|error| error.kind() == io::ErrorKind::BrokenPipe)
            {
                probe.peer_write_failed.cancel();
            }
            if let Err(error) = result {
                if peer_disconnected(&error) {
                    channels.peer_disconnected.cancel();
                }
                return Err(error);
            }
        }
        Ok::<_, io::Error>(())
    };
    let result = tokio::select! {
        _=channels.closed.cancelled()=>Ok(()),
        result=receive=>result,
        result=transmit=>result,
    };
    channels.closed.cancel();
    result
}

#[cfg(test)]
#[path = "acp_connection_transport_test.rs"]
mod tests;
