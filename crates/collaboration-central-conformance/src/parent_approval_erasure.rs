//! Destination replay and delivery visibility do not depend on the erase alarm.
use approval_request_protocol::{
    ApprovalDestination, DecideApprovalAsSessionReply, DeliverEndingReply, PassOnApprovalReply,
    RaiseReply, ReadApprovalForSessionReply, RegisterRunReply,
};
use central_storage::{SafeInteger, SqlValue, Statement};
use reqwest_websocket::CloseCode;

use crate::approval_link_scenarios::{
    ApprovalLinkFixture, answer_ending, held_start, next_ending, raise, read_request, register_run,
    require_receipt,
};

use super::{Chain, ParentSetup, Result, option, typed_id};

pub async fn destination_replays_and_unpaid_ending_at_cutoff(
    fixture: &impl ApprovalLinkFixture,
) -> Result<()> {
    let mut setup = ParentSetup::open(fixture, Chain::Direct).await?;
    let id = setup.approval.request_id().clone();
    let pass = typed_id(0xea01)?;
    let decision = typed_id(0xea02)?;
    let late_pass = typed_id(0xea03)?;
    let late_decision = typed_id(0xea04)?;
    assert_eq!(
        setup.pass(id.clone(), pass).await?,
        PassOnApprovalReply::PassedOn {
            to: ApprovalDestination::People {}
        },
    );
    assert!(matches!(
        setup.decide(id.clone(), decision, option(1)?).await?,
        DecideApprovalAsSessionReply::SavedDecision { .. }
    ));
    let cutoff = fixture.now_seconds() + 5_184_000;
    setup.socket.close(CloseCode::Normal, None).await?;
    fixture.set_wall_seconds(cutoff - 1).await?;
    setup.socket = fixture.connect().await?;
    let organization = setup.target.organization_id.clone();
    assert_eq!(
        register_run(&mut setup.socket, &organization, &setup.target.router_run).await?,
        RegisterRunReply::Registered {},
    );
    let (correlation, owed) = next_ending(&mut setup.socket, &setup.target).await?;
    assert_eq!(owed.request_id(), &id);
    assert!(matches!(
        setup.read(id.clone()).await?,
        ReadApprovalForSessionReply::Request { .. }
    ));
    assert!(matches!(
        setup.pass(id.clone(), late_pass.clone()).await?,
        PassOnApprovalReply::AlreadyDecided { .. }
    ));
    assert!(matches!(
        setup
            .decide(id.clone(), late_decision.clone(), option(1)?)
            .await?,
        DecideApprovalAsSessionReply::AlreadyDecided { .. }
    ));
    for ordinal in 100..355 {
        let queued = held_start(&setup.target.router_run, ordinal)?;
        require_receipt(raise(&mut setup.socket, &organization, queued).await?)?;
    }
    let next = held_start(&setup.target.router_run, 355)?;
    assert_eq!(
        raise(&mut setup.socket, &organization, next.clone()).await?,
        RaiseReply::TooManyOutstanding {},
    );

    fixture.advance(1).await?;
    assert_eq!(
        setup.read(id.clone()).await?,
        ReadApprovalForSessionReply::NotVisible {}
    );
    assert_eq!(
        setup.pass(id.clone(), late_pass).await?,
        PassOnApprovalReply::NotVisible {},
    );
    assert_eq!(
        setup.decide(id.clone(), late_decision, option(1)?).await?,
        DecideApprovalAsSessionReply::NotVisible {},
    );
    require_receipt(raise(&mut setup.socket, &organization, next).await?)?;
    fixture.fixture_write(Statement {
        sql: "UPDATE approval_requests SET ending_json='corrupt expired evidence' WHERE request_id=?",
        args: vec![SqlValue::Text(id.to_string())],
    }).await?;
    fixture.hold_delivery_answer().await?;
    answer_ending(
        &mut setup.socket,
        setup.target.clone(),
        correlation,
        DeliverEndingReply::Received {},
    )
    .await?;
    fixture.observe_delivery_answer().await?;
    assert_eq!(
        read_request(fixture, &setup.approval).await?,
        [
            SqlValue::Text("ended".to_owned()),
            SqlValue::Text("owed".to_owned())
        ],
        "the late holder answer did not change the stored delivery",
    );

    fixture
        .fixture_write(Statement {
            sql: "UPDATE approval_requests SET ending_json=?1 WHERE request_id=?2",
            args: vec![
                SqlValue::Text(serde_json::to_string(&owed)?),
                SqlValue::Text(id.to_string()),
            ],
        })
        .await?;

    let ledger = Statement {
        sql: "SELECT count(*) FROM runtime_request_log WHERE approval_row_id=(SELECT approval_row_id FROM approval_requests WHERE request_id=?)",
        args: vec![SqlValue::Text(id.to_string())],
    };
    assert_eq!(
        fixture.read(ledger).await?.rows,
        [vec![SqlValue::Integer(SafeInteger::new(2)?)]]
    );
    let later = 300 - fixture.now_seconds().rem_euclid(300);
    fixture.advance(u64::try_from(later)?).await?;
    fixture.observe_due_alarm().await?;
    let data = fixture.read(Statement {
        sql: "SELECT (SELECT count(*) FROM approval_requests WHERE request_id=?), (SELECT count(*) FROM runtime_request_log WHERE approval_row_id IS NOT NULL), (SELECT count(*) FROM approval_request_destinations WHERE request_id=?), (SELECT count(*) FROM approval_erased_requests WHERE request_id=?)",
        args: vec![SqlValue::Text(id.to_string()); 3],
    }).await?;
    let expected = [0, 0, 0, 1]
        .map(SafeInteger::new)
        .into_iter()
        .collect::<std::result::Result<Vec<_>, _>>()?;
    assert_eq!(
        data.rows,
        [expected
            .into_iter()
            .map(SqlValue::Integer)
            .collect::<Vec<_>>()]
    );
    assert_eq!(
        setup.read(id).await?,
        ReadApprovalForSessionReply::NotVisible {}
    );
    setup.socket.close(CloseCode::Normal, None).await?;
    Ok(())
}
