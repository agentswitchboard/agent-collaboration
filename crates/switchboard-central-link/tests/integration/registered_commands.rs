use central_link_client::{
    ImmediateRequestCommand, LinkLaneDisposition, LinkLaneEvent, ReadyLinkLane, UntouchedCommand,
};
use central_link_protocol::{
    DataFrame, FrameDirection, LinkFrame, ReplyPayload, approval_rpc::RegisterRunReply, decode_text,
};
use futures_util::{FutureExt, StreamExt};

use super::{
    Fixture,
    admitted_commands::{mail_write, send_answer},
    registration_request, reply_registration, text, tick, welcome_before_registration,
};

#[tokio::test]
#[expect(
    clippy::panic,
    reason = "Fail at the missing peer observation or frame."
)]
async fn exact_admitted_connection_refuses_work_until_registered() {
    let fixture = Fixture::new().await;
    let lane = fixture.inbox_events.lane.clone();
    let mut socket = fixture.accept().await;
    welcome_before_registration(&mut socket).await;
    let (correlation_id, target, _) = registration_request(&mut socket).await;
    let Some(LinkLaneEvent::ConnectionAdmitted(admitted)) = lane.next_event().await else {
        panic!("Welcome must admit while the registration acknowledgement is held");
    };
    let mut registered = lane.watch_registered();
    assert!(registered.borrow().is_none());
    let command = ImmediateRequestCommand {
        connection: admitted.connection.clone(),
        send_before: tick(50),
        request: Box::new(mail_write(1)),
    };

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
