use super::*;

pub(super) struct ApprovalFixture {
    pub(super) central: MachineLinkFixture,
    pub(super) signin: ApprovalSignIn,
    person_revision: Arc<AtomicI64>,
}

impl ApprovalFixture {
    pub(super) async fn start() -> Self {
        Self::with_first_snapshot(300).await
    }

    pub(super) async fn with_first_snapshot(first_snapshot_seconds: i64) -> Self {
        Self::with_person_claims(first_snapshot_seconds, |_| {}).await
    }

    pub(super) async fn with_person_claims(
        first_snapshot_seconds: i64,
        configure: fn(&mut Person),
    ) -> Self {
        let clock = Arc::new(ManualClock::new(
            chrono::DateTime::from_timestamp(INITIAL_SECONDS, 0).unwrap(),
        ));
        let central = start_session_link_fixture(clock.clone()).await.unwrap();
        Self::publish(
            &central,
            json!({
                "kind":"organization", "organizationId":ORGANIZATION, "revision":3,
                "state":"active", "permissionPolicyDigest":POLICY,
            }),
        )
        .await;
        let person_revision = Arc::new(AtomicI64::new(4));
        let revision_for_upgrade = person_revision.clone();
        let signin = ApprovalSignIn::start(clock, central.origin(), PERSON, move |now, socket| {
            let mut person = Person::owner(ORGANIZATION, now).unwrap();
            person.revision = revision_for_upgrade.load(Ordering::SeqCst);
            person.second_factor_at = None;
            configure(&mut person);
            let snapshot = if socket == 1 {
                first_snapshot_seconds
            } else {
                300
            };
            person_link_bearer(&person, now, snapshot, 600).unwrap()
        })
        .await;
        let fixture = Self {
            central,
            signin,
            person_revision,
        };
        fixture.publish_person_revision(4).await;
        fixture
    }

    async fn publish(central: &MachineLinkFixture, revision: Value) {
        let runtime = central
            .host
            .central(&central.organization_id)
            .await
            .unwrap();
        runtime
            .deliver_authorization_revision(&serde_json::from_value(revision).unwrap())
            .await
            .unwrap();
    }

    pub(super) async fn publish_person_revision(&self, revision: i64) {
        Self::publish(
            &self.central,
            json!({
                "kind":"person", "organizationId":ORGANIZATION, "personId":OWNER,
                "subjectId":OWNER_SUBJECT, "revision":revision, "state":"active",
            }),
        )
        .await;
        self.person_revision.store(revision, Ordering::SeqCst);
    }

    pub(super) async fn holder(&self) -> (ApprovalHolderTarget, WebSocket) {
        let mut socket = self.central.connect().await.unwrap();
        assert!(matches!(
            receive_frame(&mut socket).await.unwrap(),
            LinkFrame::Data(DataFrame::Welcome { .. })
        ));
        announce_mac_calls(&mut socket, vec![MacCallKind::DeliverEnding])
            .await
            .unwrap();
        let target = ApprovalHolderTarget {
            organization_id: self.central.organization_id.clone(),
            machine_id: MACHINE_LINK_FIXTURE_MACHINE.parse().unwrap(),
            enrollment_id: self.central.enrollment_id.clone(),
            router_run: ROUTER_RUN.parse().unwrap(),
        };
        assert_eq!(
            register_run(&mut socket, &target.organization_id, &target.router_run)
                .await
                .unwrap(),
            RegisterRunReply::Registered {}
        );
        (target, socket)
    }

    pub(super) async fn rows(&self, sql: &'static str, args: Vec<SqlValue>) -> Vec<Vec<SqlValue>> {
        let runtime = self
            .central
            .host
            .central(&self.central.organization_id)
            .await
            .unwrap();
        runtime
            .runtime()
            .statement_host()
            .read(Statement { sql, args })
            .await
            .unwrap()
            .rows
    }

    pub(super) async fn operation_rows(&self) -> Vec<Vec<SqlValue>> {
        self.rows(
            "SELECT request_id FROM runtime_request_log ORDER BY request_id",
            vec![],
        )
        .await
    }

    pub(super) async fn ending(
        &self,
        holder: &mut WebSocket,
        target: &ApprovalHolderTarget,
        request: &Raise,
        expected: Ending,
    ) {
        let (correlation, offered) = next_ending(holder, target).await.unwrap();
        assert_eq!(offered.request_id(), request.request_id());
        assert_eq!(
            offered.contents_digest(),
            &request.contents_digest().unwrap()
        );
        assert_eq!(offered.into_ending(), expected);
        answer_ending(
            holder,
            target.clone(),
            correlation,
            DeliverEndingReply::Received {},
        )
        .await
        .unwrap();
        let received = [vec![
            SqlValue::Text("ended".into()),
            SqlValue::Text("received_by_holder".into()),
        ]];
        let mut observed = Vec::new();
        for _ in 0..128 {
            observed = self.rows(
                "SELECT record_state,delivery_state FROM approval_requests WHERE request_id = ?1",
                vec![SqlValue::Text(request.request_id().to_string())],
            ).await;
            if observed == received {
                break;
            }
        }
        assert_eq!(
            observed, received,
            "owning store did not receive the holder receipt"
        );
        heartbeat(holder).await.unwrap();
    }

    pub(super) async fn close(self) {
        self.signin.close().await;
        self.central.close().await.unwrap();
    }
}
