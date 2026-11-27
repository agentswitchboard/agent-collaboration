use super::{
    Cell, NativeApprovalFixture, RefCell, Result, signed_clock, start_session_link_fixture,
};
use collaboration_central_conformance::approval_erasure_scenarios::{
    MissedApprovalCleanupFixture, cutoff_and_correlated_marker,
    erase_batches_isolate_corruption_and_retain_only_live_markers,
    missed_cleanup_is_recovered_by_the_next_mark,
};
use collaboration_central_conformance::approval_link_scenarios::ApprovalLinkFixture;

impl MissedApprovalCleanupFixture for NativeApprovalFixture {
    async fn miss_cleanup_at(&self, mark: i64) -> Result<()> {
        use central_storage::{AlarmUpdate, BatchOutcome, Statement, StatementBatch};

        let owner = self.link.host.central(&self.link.organization_id).await?;
        let runtime = owner.runtime();
        let store = runtime.statement_host();
        assert_eq!(store.alarm().map(|at| at.unix_millis()), Some(mark * 1000));
        let completed = runtime.alarm_completion_count();
        // Lose the physical alarm while retaining the persisted due-work entry.
        assert!(matches!(
            store
                .run_batch(StatementBatch {
                    statements: vec![],
                    alarm: AlarmUpdate::SetFromQuery(Statement {
                        sql: "SELECT NULL AS earliest_due",
                        args: vec![],
                    }),
                })
                .await?,
            BatchOutcome::Applied { .. }
        ));
        assert_eq!(store.alarm(), None);
        // Jump UTC only. A stale native sleep cannot become due before the driver
        // has observed the cleared publication and the subsequent rearm.
        self.set_wall_seconds(mark + 1).await?;
        assert_eq!(runtime.alarm_completion_count(), completed);
        Ok(())
    }

    async fn prepare_catch_up(
        &self,
        socket: reqwest_websocket::WebSocket,
        mark: i64,
    ) -> Result<reqwest_websocket::WebSocket> {
        use collaboration_central_conformance::approval_link_scenarios::register_run;
        use reqwest_websocket::CloseCode;
        use router_clock::RuntimeClock;
        use runtime_primitives::SyncTime;

        socket.close(CloseCode::Normal, None).await?;
        // Welcome fences the production rearm of the physically lost alarm.
        let mut socket = self.connect_without_capabilities().await?;
        let target = self.target()?;
        assert_eq!(
            register_run(&mut socket, &target.organization_id, &target.router_run).await?,
            super::RegisterRunReply::Registered {}
        );
        let owner = self.link.host.central(&self.link.organization_id).await?;
        let at = SyncTime::from_unix_millis(mark * 1000);
        assert_eq!(owner.runtime().statement_host().alarm(), Some(at));
        let clock = &self.link.clock;
        clock.not_due_check_now(clock.wall_deadline(at)).await;
        Ok(socket)
    }
}

#[tokio::test]
async fn tokio_missed_approval_cleanup_is_recovered_by_the_next_mark() -> Result<()> {
    let fixture = NativeApprovalFixture {
        link: start_session_link_fixture(signed_clock()?).await?,
        before_alarm: Cell::new(0),
        delivery_answer: RefCell::default(),
        before_write: RefCell::default(),
        notice_recovery: RefCell::default(),
    };
    let result = missed_cleanup_is_recovered_by_the_next_mark(&fixture).await;
    fixture.link.close().await?;
    result
}

#[tokio::test]
async fn tokio_approval_cutoff_and_marker_on_the_real_link() -> Result<()> {
    let fixture = NativeApprovalFixture {
        link: start_session_link_fixture(signed_clock()?).await?,
        before_alarm: Cell::new(0),
        delivery_answer: RefCell::default(),
        before_write: RefCell::default(),
        notice_recovery: RefCell::default(),
    };
    let result = cutoff_and_correlated_marker(&fixture).await;
    fixture.link.close().await?;
    result
}

#[tokio::test]
async fn tokio_approval_erasure_batches_and_retired_markers() -> Result<()> {
    let fixture = NativeApprovalFixture {
        link: start_session_link_fixture(signed_clock()?).await?,
        before_alarm: Cell::new(0),
        delivery_answer: RefCell::default(),
        before_write: RefCell::default(),
        notice_recovery: RefCell::default(),
    };
    let result = erase_batches_isolate_corruption_and_retain_only_live_markers(&fixture).await;
    fixture.link.close().await?;
    result
}

#[tokio::test]
async fn tokio_approval_erasure_hides_replays_and_releases_owed_capacity() -> Result<()> {
    let fixture = NativeApprovalFixture {
        link: start_session_link_fixture(signed_clock()?).await?,
        before_alarm: Cell::new(0),
        delivery_answer: RefCell::default(),
        before_write: RefCell::default(),
        notice_recovery: RefCell::default(),
    };
    let result =
        collaboration_central_conformance::parent_approval_scenarios::destination_replays_and_unpaid_ending_at_cutoff(&fixture).await;
    fixture.link.close().await?;
    result
}

#[tokio::test]
async fn tokio_session_page_and_durable_cutoff() -> Result<()> {
    let fixture = NativeApprovalFixture {
        link: start_session_link_fixture(signed_clock()?).await?,
        before_alarm: Cell::new(0),
        delivery_answer: RefCell::default(),
        before_write: RefCell::default(),
        notice_recovery: RefCell::default(),
    };
    let observed = collaboration_central_conformance::parent_approval_scenarios::session_page_and_durable_cutoff(&fixture).await;
    fixture.link.close().await?;
    observed
}

#[tokio::test]
async fn tokio_queued_ending_uses_persisted_cutoff() -> Result<()> {
    let fixture = NativeApprovalFixture {
        link: start_session_link_fixture(signed_clock()?).await?,
        before_alarm: Cell::new(0),
        delivery_answer: RefCell::default(),
        before_write: RefCell::default(),
        notice_recovery: RefCell::default(),
    };
    let observed = collaboration_central_conformance::parent_approval_scenarios::queued_ending_uses_persisted_cutoff(&fixture).await;
    fixture.link.close().await?;
    observed
}

#[tokio::test]
async fn tokio_tied_cleanup_work_yields_the_approval_lane() -> Result<()> {
    let fixture = NativeApprovalFixture {
        link: start_session_link_fixture(signed_clock()?).await?,
        before_alarm: Cell::new(0),
        delivery_answer: RefCell::default(),
        before_write: RefCell::default(),
        notice_recovery: RefCell::default(),
    };
    let observed = collaboration_central_conformance::approval_cleanup_fairness::tied_cleanup_work_yields_the_approval_lane(&fixture).await;
    fixture.link.close().await?;
    observed
}

#[tokio::test]
async fn tokio_corrupt_cleanup_is_charged_without_stopping_healthy_work() -> Result<()> {
    let fixture = NativeApprovalFixture {
        link: start_session_link_fixture(signed_clock()?).await?,
        before_alarm: Cell::new(0),
        delivery_answer: RefCell::default(),
        before_write: RefCell::default(),
        notice_recovery: RefCell::default(),
    };
    let observed = collaboration_central_conformance::approval_cleanup_fairness::corrupt_cleanup_is_charged_without_stopping_healthy_work(&fixture).await;
    fixture.link.close().await?;
    observed
}

#[tokio::test]
async fn tokio_session_page_uses_the_ending_when_stored_cutoff_is_corrupt() -> Result<()> {
    let fixture = NativeApprovalFixture {
        link: start_session_link_fixture(signed_clock()?).await?,
        before_alarm: Cell::new(0),
        delivery_answer: RefCell::default(),
        before_write: RefCell::default(),
        notice_recovery: RefCell::default(),
    };
    let observed = collaboration_central_conformance::parent_approval_scenarios::session_page_uses_the_ending_when_stored_cutoff_is_corrupt(&fixture).await;
    fixture.link.close().await?;
    observed
}

#[tokio::test]
async fn tokio_corrupt_parked_anchor_does_not_undo_the_marker_page() -> Result<()> {
    let fixture = NativeApprovalFixture {
        link: start_session_link_fixture(signed_clock()?).await?,
        before_alarm: Cell::new(0),
        delivery_answer: RefCell::default(),
        before_write: RefCell::default(),
        notice_recovery: RefCell::default(),
    };
    let observed = collaboration_central_conformance::approval_cleanup_fairness::corrupt_parked_anchor_does_not_undo_the_marker_page(&fixture).await;
    fixture.link.close().await?;
    observed
}
