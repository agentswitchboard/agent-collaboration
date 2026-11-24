//! Millisecond admission probes with erasure held on the engine grid.
use approval_request_protocol::{
    DecideApprovalAsSessionReply, ListApprovalsForSessionReply, ListApprovalsForSessionRequest,
    PassOnApprovalReply, ReadApprovalForSessionReply,
};
use central_link_protocol::approval_rpc::ListApprovalsForSessionRpc;
use central_storage::{SafeInteger, SqlValue, Statement};
use reqwest_websocket::CloseCode;

use crate::{
    approval_link_scenarios::{
        ApprovalLinkFixture, next_ending, raise, register_run, require_receipt, stop, stop_request,
    },
    approval_provider_scenarios::{ProviderAsk, provider_raise},
};

use super::mac_call_ledger_scenarios::wait_without_frame;
use super::{
    Chain, LedgerFixture, MacDeliveryEvent, ParentSetup, Result, answered, call, option, typed_id,
};

const SIXTY_DAYS_MS: i64 = 5_184_000_000;

pub async fn session_page_and_durable_cutoff(fixture: &impl ApprovalLinkFixture) -> Result<()> {
    let mut setup = ParentSetup::open(fixture, Chain::Direct).await?;
    let id = setup.approval.request_id().clone();
    let organization = setup.target.organization_id.clone();
    let result = stop(
        &mut setup.socket,
        &organization,
        stop_request(&setup.approval)?,
    )
    .await?;
    assert!(matches!(
        result,
        central_link_protocol::TypedCallResult::Answered {
            reply: approval_request_protocol::StopReply::Stands { .. },
        }
    ));
    let cutoff = fixture.now_seconds() * 1000 + SIXTY_DAYS_MS;
    setup.socket.close(CloseCode::Normal, None).await?;
    fixture.set_wall_millis(cutoff - 1).await?;
    setup.socket = fixture.connect_without_capabilities().await?;
    register_run(&mut setup.socket, &organization, &setup.target.router_run).await?;

    for ordinal in 100..200 {
        let request = provider_raise(
            &setup.target,
            &setup.child.session_id,
            ordinal,
            ProviderAsk::Approval,
        )?;
        require_receipt(raise(&mut setup.socket, &organization, request).await?)?;
    }
    let ListApprovalsForSessionReply::Page { items, next } = setup.list().await?;
    assert_eq!(items.len(), 100);
    let cursor = next.ok_or("the visible overflow row requires a cursor before cutoff")?;
    let ListApprovalsForSessionReply::Page { items, next } = answered(
        call::<ListApprovalsForSessionRpc>(
            &mut setup.socket,
            &organization,
            setup.parent.request(ListApprovalsForSessionRequest {
                cursor: Some(cursor),
            }),
        )
        .await?,
    )?;
    assert_eq!(items.len(), 1);
    assert!(items.iter().all(|item| item.request_id == id));
    assert_eq!(next, None);

    let operation = typed_id(0xfa04)?;
    assert_eq!(
        setup
            .decide(id.clone(), operation.clone(), option(1)?)
            .await?,
        DecideApprovalAsSessionReply::TooLate {}
    );

    // A point read alone is the first operation to observe the inclusive cutoff.
    fixture.set_wall_millis(cutoff).await?;
    assert_eq!(
        setup.read(id.clone()).await?,
        ReadApprovalForSessionReply::NotVisible {}
    );
    assert_eq!(floor(fixture).await?, cutoff);
    fixture.set_wall_millis(cutoff - 1).await?;
    assert_eq!(
        setup.read(id.clone()).await?,
        ReadApprovalForSessionReply::NotVisible {}
    );

    assert_eq!(
        setup.decide(id.clone(), operation, option(1)?).await?,
        DecideApprovalAsSessionReply::NotVisible {}
    );

    // Keep the old bytes, including broken data the admitted lookup must never read.
    fixture.fixture_write(Statement {
        sql: "UPDATE approval_requests SET offer_json='broken expired offer', ending_json='broken expired ending' WHERE request_id=?",
        args: vec![SqlValue::Text(id.to_string())],
    }).await?;
    for at in [cutoff, cutoff + 1] {
        fixture.set_wall_millis(at).await?;
        let ListApprovalsForSessionReply::Page { items, next } = setup.list().await?;
        assert_eq!(items.len(), 100);
        assert!(items.iter().all(|item| item.request_id != id));
        assert_eq!(next, None);
    }
    let missing = "019f0000-0000-7000-8000-00000000f901".parse()?;
    assert_eq!(setup.read(id.clone()).await?, setup.read(missing).await?);
    assert_eq!(
        setup.pass(id.clone(), typed_id(0xfa01)?).await?,
        PassOnApprovalReply::NotVisible {}
    );
    assert_eq!(
        setup.decide(id, typed_id(0xfa02)?, option(1)?).await?,
        DecideApprovalAsSessionReply::NotVisible {}
    );
    setup.socket.close(CloseCode::Normal, None).await?;
    Ok(())
}

pub async fn session_page_uses_the_ending_when_stored_cutoff_is_corrupt(
    fixture: &impl ApprovalLinkFixture,
) -> Result<()> {
    let mut setup = ParentSetup::open(fixture, Chain::Direct).await?;
    let organization = setup.target.organization_id.clone();
    setup.socket.close(CloseCode::Normal, None).await?;
    setup.socket = fixture.connect_without_capabilities().await?;
    register_run(&mut setup.socket, &organization, &setup.target.router_run).await?;
    let second = provider_raise(
        &setup.target,
        &setup.child.session_id,
        4,
        ProviderAsk::Approval,
    )?;
    require_receipt(raise(&mut setup.socket, &organization, second.clone()).await?)?;
    let first_id = setup.approval.request_id().clone();
    let second_id = second.request_id().clone();
    let now = fixture.now_seconds() * 1000;
    let cutoff = now + SIXTY_DAYS_MS;
    for (request, ended_at) in [(&setup.approval, now), (&second, now + 1)] {
        fixture.set_wall_millis(ended_at).await?;
        assert!(matches!(
            stop(&mut setup.socket, &organization, stop_request(request)?).await?,
            central_link_protocol::TypedCallResult::Answered {
                reply: approval_request_protocol::StopReply::Stands { .. },
            }
        ));
    }
    setup.socket.close(CloseCode::Normal, None).await?;
    fixture.fixture_write(Statement {
        sql: "UPDATE approval_requests SET erase_after_ms=?1 WHERE request_id=?2 AND erase_after_ms=?3",
        args: vec![
            SqlValue::Integer(SafeInteger::new(cutoff - 1)?),
            SqlValue::Text(second_id.to_string()),
            SqlValue::Integer(SafeInteger::new(cutoff + 1)?),
        ],
    }).await?;
    let stored = fixture
        .read(Statement {
            sql: "SELECT erase_after_ms FROM approval_requests WHERE request_id=?",
            args: vec![SqlValue::Text(second_id.to_string())],
        })
        .await?;
    assert_eq!(
        stored.rows,
        [vec![SqlValue::Integer(SafeInteger::new(cutoff - 1)?)]]
    );

    fixture.set_wall_millis(cutoff - 1).await?;
    setup.socket = fixture.connect_without_capabilities().await?;
    register_run(&mut setup.socket, &organization, &setup.target.router_run).await?;
    fixture.set_wall_millis(cutoff).await?;

    let ListApprovalsForSessionReply::Page { items, next } = setup.list().await?;
    assert_eq!(
        items
            .iter()
            .map(|item| &item.request_id)
            .collect::<Vec<_>>(),
        [&second_id],
        "the stored cutoff must not hide the approval that ended one millisecond later",
    );
    assert_eq!(next, None);
    assert_eq!(floor(fixture).await?, cutoff);
    assert_eq!(
        setup.read(first_id.clone()).await?,
        ReadApprovalForSessionReply::NotVisible {}
    );
    assert!(matches!(
        setup.read(second_id.clone()).await?,
        ReadApprovalForSessionReply::Request { .. }
    ));
    fixture.set_wall_millis(cutoff + 1).await?;
    let ListApprovalsForSessionReply::Page { items, next } = setup.list().await?;
    assert!(items.is_empty());
    assert_eq!(next, None);
    setup.socket.close(CloseCode::Normal, None).await?;
    Ok(())
}

pub async fn queued_ending_uses_persisted_cutoff(fixture: &impl LedgerFixture) -> Result<()> {
    let mut setup = ParentSetup::open(fixture, Chain::Direct).await?;
    let id = setup.approval.request_id().clone();
    let organization = setup.target.organization_id.clone();
    assert!(matches!(
        setup
            .decide(id.clone(), typed_id(0xfa03)?, option(1)?)
            .await?,
        DecideApprovalAsSessionReply::SavedDecision { .. }
    ));
    let (_, earlier) = next_ending(&mut setup.socket, &setup.target).await?;
    assert_eq!(earlier.request_id(), &id);
    let cutoff = fixture.now_seconds() * 1000 + SIXTY_DAYS_MS;
    setup.socket.close(CloseCode::Normal, None).await?;
    fixture.set_wall_millis(cutoff - 1).await?;

    let before = MacDeliveryEvent::BeforeEndingSocketWrite;
    let after = MacDeliveryEvent::AfterEndingOffer;
    fixture.hold_mac_event(before).await?;
    setup.socket = fixture.connect().await?;
    let finished = fixture.mac_event_count(after).await?;
    register_run(&mut setup.socket, &organization, &setup.target.router_run).await?;
    fixture.wait_mac_event_hold(before).await?;

    // The point-read scenario proves the writer of this fact. At the socket boundary
    // inject that durable fact while keeping the already queued offer in place.
    fixture
        .fixture_write(Statement {
            sql: "UPDATE approval_applying_time SET observed_ms=?1 WHERE organization_id=?2",
            args: vec![
                SqlValue::Integer(SafeInteger::new(cutoff)?),
                SqlValue::Text(organization.as_str().to_owned()),
            ],
        })
        .await?;
    fixture.set_wall_millis(cutoff - 2).await?;
    fixture.release_mac_event(before).await?;
    wait_without_frame(
        &mut setup.socket,
        fixture.wait_mac_event_count(after, finished + 1),
    )
    .await?;
    assert_eq!(floor(fixture).await?, cutoff);
    let observed = fixture
        .read(Statement {
            sql: "SELECT delivery_state FROM approval_requests WHERE request_id=?",
            args: vec![SqlValue::Text(id.to_string())],
        })
        .await?;
    assert_eq!(observed.rows, [vec![SqlValue::Text("owed".to_owned())]]);
    setup.socket.close(CloseCode::Normal, None).await?;
    Ok(())
}

async fn floor(fixture: &impl ApprovalLinkFixture) -> Result<i64> {
    let outcome = fixture
        .read(Statement {
            sql: "SELECT observed_ms FROM approval_applying_time",
            args: vec![],
        })
        .await?;
    match outcome.rows.as_slice() {
        [row] => match row.as_slice() {
            [SqlValue::Integer(value)] => Ok(value.get()),
            _ => Err("expected the integer approval floor".into()),
        },
        _ => Err("expected a single durable approval floor".into()),
    }
}
