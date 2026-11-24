use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn final_body_frame_does_not_handoff_until_the_socket_writer_accepts_bytes() {
    let (server, mut client) = tokio::io::duplex(2);
    let mut socket = SignInReplySocket::new(server);
    let (reply_started, mut drain_runnable) = tokio::sync::oneshot::channel();
    let (_, work) = collect_reply_due_work(async {
        fleet_runtime::queue_reply_due_work(move |scheduler| {
            scheduler.run_after_reply(Box::pin(async move { reply_started.send(()).unwrap() }));
        });
    })
    .await;
    let mut body = SignInReplyBody {
        body: Body::from("wire"),
        writer: socket.writer.clone(),
        pending: Some((WorkspaceBackgroundTasks::default(), work)),
    };
    let mut context = Context::from_waker(std::task::Waker::noop());
    let Poll::Ready(Some(Ok(frame))) =
        http_body::Body::poll_frame(Pin::new(&mut body), &mut context)
    else {
        panic!("the response did not produce its final body frame");
    };
    let bytes = frame.into_data().unwrap();
    assert_eq!(socket.write(&bytes).await.unwrap(), 2);
    assert!(
        Pin::new(&mut socket)
            .poll_write(&mut context, &bytes[2..])
            .is_pending()
    );
    tokio::task::yield_now().await;
    assert!(matches!(
        drain_runnable.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
    ));

    let mut delivered = [0; 4];
    client.read_exact(&mut delivered[..2]).await.unwrap();
    socket.write_all(&bytes[2..]).await.unwrap();
    socket.flush().await.unwrap();
    client.read_exact(&mut delivered[2..]).await.unwrap();
    assert_eq!(&delivered, b"wire");
    drain_runnable.await.unwrap();
}
