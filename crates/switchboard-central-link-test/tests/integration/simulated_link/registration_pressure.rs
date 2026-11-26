use std::time::Duration;

use central_link_client::{LinkLocalFault, ReadyLinkLane};
use central_link_protocol::{DataFrame, LinkFrame};
use central_storage::{
    AlarmUpdate, BatchOutcome, GuardedStatement, RowExpectation, SqlValue, Statement,
    StatementBatch,
};
use collaboration_central_tokio::test_support::MachineLinkFixture;
use router_clock::ClockInstant;
use switchboard_message_custody::{
    custody_operator::CustodyOperatorEvent, custody_task::CustodyTaskEvent,
};
use workspace_ids::MessageId;

use super::super::{
    SimulatedLink,
    packet_flight::{FrameKind, PacketFlight},
    run_link_case,
};
use super::{NetworkCustody, registration_gate_cases::seed_backlog};

fn offers_by_socket(network: &PacketFlight, since: usize, port: u16) -> Vec<MessageId> {
    network
        .frames()
        .iter()
        .skip(since)
        .filter_map(|entry| match &entry.frame {
            LinkFrame::Data(DataFrame::Deliver { message, .. })
                if entry.connection_port == port =>
            {
                Some(message.message_id().clone())
            }
            _ => None,
        })
        .collect()
}

async fn seed_pressure_backlog(central: &MachineLinkFixture) -> Vec<MessageId> {
    let mut queued = seed_backlog(central, 1).await;
    let template = queued.first().unwrap();
    let owner = central
        .host
        .central(&central.organization_id)
        .await
        .unwrap();
    // The live source socket limits outstanding offers. Seed the rest as
    // genuine queued rows on the owning store before opening the test socket.
    let written = owner.runtime().statement_host().run_batch(StatementBatch {
        statements: vec![GuardedStatement {
            statement: Statement {
                sql: "WITH RECURSIVE seed(idx) AS (SELECT 1 UNION ALL SELECT idx+1 FROM seed WHERE idx<63)
                       INSERT INTO mail_messages (message_id,sender,target_machine_id,target_session,message_kind,body,state,created_at,updated_at,cancel_due_at,sender_request_id,delivery_window_ends_at)
                       SELECT printf('019f0000-0000-7000-8001-%012x',idx),sender,target_machine_id,target_session,message_kind,body,state,created_at,updated_at,cancel_due_at,sender_request_id,delivery_window_ends_at
                       FROM mail_messages JOIN seed WHERE message_id=?",
                args: vec![SqlValue::Text(template.to_string())],
            },
            expect: RowExpectation::Any,
        }],
        alarm: AlarmUpdate::Unchanged,
    }).await.unwrap();
    assert!(matches!(written, BatchOutcome::Applied { .. }));
    queued.extend((1..=63).map(|index| {
        format!("019f0000-0000-7000-8001-{index:012x}")
            .parse::<MessageId>()
            .unwrap()
    }));
    queued
}

async fn registration_under_pressure(first: SimulatedLink, hold_through_stall: bool) {
    let central = first.central.clone();
    let clock = first.clock.clone();
    let network = first.network.clone();
    first.close().await;
    let queued = seed_pressure_backlog(&central).await;
    let since = network.frames().len();
    let link = SimulatedLink::start(clock, network.clone(), central).await;
    let attempt = link.admission.connection.clone();
    let (mut case, mut held) =
        NetworkCustody::start_custody(link, |inbox| inbox.hold_next_commit(), |custody| custody)
            .await;
    held.until_held().await.unwrap();
    let first_ack = network.seen(since, FrameKind::RegisterReply).await;
    let observed = offers_by_socket(&network, since, first_ack.connection_port);
    assert_eq!(observed.len(), queued.len());
    assert!(queued.iter().all(|message| observed.contains(message)));
    let stall_at = ClockInstant::ZERO.after(Duration::from_secs(5));
    case.link.driver_clock.wait_until_pending(stall_at).await;
    case.advance_to(ClockInstant::ZERO.after(Duration::from_secs(4)));
    assert!(case.link.lane.watch_registered().borrow().is_none());
    assert_eq!(
        case.link.lane.watch_admitted().borrow().as_ref(),
        Some(&case.link.admission)
    );

    if hold_through_stall {
        case.advance_to(stall_at);
        case.link.break_socket.closed().await;
        assert!(case.link.lane.watch_admitted().borrow().is_none());
        assert!(case.link.lane.watch_registered().borrow().is_none());
        held.release();
        loop {
            match case.events.recv().await.unwrap() {
                CustodyTaskEvent::Operator(CustodyOperatorEvent::LinkFaulted {
                    connection,
                    cause: LinkLocalFault::DeliveryConsumerStalled,
                }) => {
                    assert_eq!(connection, attempt);
                    break;
                }
                CustodyTaskEvent::Failed(error) => {
                    panic!("custody failed while draining pressure: {error:?}")
                }
                _ => {}
            }
        }
        case.link.reconnect().await;
        let next = network.seen_nth(since, FrameKind::RegisterReply, 1).await;
        assert_ne!(next.connection_port, first_ack.connection_port);
        let offered_again = offers_by_socket(&network, since, next.connection_port);
        assert_eq!(offered_again.len(), queued.len());
        assert!(queued.iter().all(|message| offered_again.contains(message)));
    } else {
        held.release();
        assert_eq!(case.link.admission.connection, attempt);
    }
    let registered = case
        .link
        .lane
        .watch_registered()
        .wait_for(Option::is_some)
        .await
        .unwrap()
        .clone()
        .unwrap();
    assert_eq!(&case.link.admission, registered.admitted());
    assert_eq!(
        registered.admitted().connection == attempt,
        !hold_through_stall
    );
    for message in &queued {
        case.wait_for_kept(message).await;
    }
    assert_eq!(
        case.inbox.pending_receipts().await.unwrap().healthy.len(),
        queued.len()
    );
    case.close().await;
}

#[test]
fn releasing_full_intake_before_the_stall_registers_the_original_socket() {
    run_link_case(|link| registration_under_pressure(link, false));
}

#[test]
fn a_full_intake_stall_preserves_the_actual_backlog_for_native_reoffer() {
    run_link_case(|link| registration_under_pressure(link, true));
}
