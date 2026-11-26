use std::time::Duration;

use central_link_client::{
    ImmediateRequestCommand, LinkLaneDisposition, LinkLaneEvent, ReadyLinkLane, UntouchedCommand,
};
use central_link_protocol::{
    DataFrame, FrameDirection, LinkFrame, ReplyPayload, approval_rpc::RegisterRunReply, decode_text,
};
use futures_util::{FutureExt, SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;

use super::{
    Fixture,
    admitted_commands::{mail_write, send_answer},
    registration_request, reply_registration, text, tick, welcome, welcome_before_registration,
};

#[tokio::test]
async fn exact_admitted_connection_refuses_work_until_registered() {
    let fixture = Fixture::new().await;
    let lane = fixture.inbox_events.lane.clone();
    let mut initial = fixture.accept().await;
    welcome(&mut initial).await;
    let mut registered = lane.watch_registered();
    let prior = registered
        .wait_for(Option::is_some)
        .await
        .unwrap()
        .clone()
        .unwrap();
    assert!(
        matches!(lane.next_event().await, Some(LinkLaneEvent::ConnectionAdmitted(ref admission)) if admission == prior.admitted())
    );
    initial.send(Message::Close(None)).await.unwrap();
    assert_eq!(
        lane.next_event().await,
        Some(LinkLaneEvent::ConnectionNotAdmitted {
            connection: prior.admitted().connection.clone(),
        })
    );
    fixture.clock.not_due_check_now(tick(1)).await;
    fixture.clock.advance(Duration::from_secs(1)).unwrap();

    let mut socket = fixture.accept().await;
    welcome_before_registration(&mut socket).await;
    let (correlation_id, target, _) = registration_request(&mut socket).await;
    let Some(LinkLaneEvent::ConnectionAdmitted(admitted)) = lane.next_event().await else {
        panic!("Welcome must admit while the registration acknowledgement is held");
    };
    assert!(registered.borrow().is_none());
    assert_ne!(admitted.connection, prior.admitted().connection);
    let command = ImmediateRequestCommand {
        connection: admitted.connection.clone(),
        send_before: tick(50),
        request: Box::new(mail_write(1)),
    };

    assert_eq!(
        lane.submit_immediate_request(ImmediateRequestCommand {
            connection: prior.admitted().connection.clone(),
            ..command.clone()
        })
        .now_or_never(),
        Some(LinkLaneDisposition::NotReached {
            cause: UntouchedCommand::ConnectionUnavailable
        })
    );
    assert_eq!(
        lane.submit_immediate_request(command.clone())
            .now_or_never(),
        Some(LinkLaneDisposition::NotReached {
            cause: UntouchedCommand::RunNotRegistered
        }),
        "unregistered work must be refused immediately"
    );
    assert!(socket.next().now_or_never().is_none());

    reply_registration(
        &mut socket,
        correlation_id,
        target,
        RegisterRunReply::Registered {},
    )
    .await;
    registered.wait_for(Option::is_some).await.unwrap();
    let mut result = lane.submit_immediate_request(command.clone());
    let outbound = tokio::select! {
        frame = text(&mut socket) => frame,
        disposition = &mut result => panic!("registered request ended before its frame: {disposition:?}"),
    };
    assert_eq!(
        decode_text(&outbound, FrameDirection::ToCentral).unwrap(),
        LinkFrame::Data(DataFrame::Request {
            request: command.request.clone()
        })
    );
    send_answer(
        &mut socket,
        DataFrame::Reply {
            correlation_id: command.request.correlation_id().clone(),
            result: Box::new(ReplyPayload::central_unavailable(command.request.payload())),
        },
    )
    .await;
    assert!(matches!(result.await, LinkLaneDisposition::Answered { .. }));
    assert!(socket.next().now_or_never().is_none());
    fixture.stop().await;
}
