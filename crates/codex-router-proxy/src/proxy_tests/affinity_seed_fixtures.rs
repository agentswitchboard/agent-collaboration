use super::*;

pub(super) fn wait_for_session_affinity(
    database_path: &Path,
    session_id: &str,
    predicate: impl Fn(&SessionAccountAffinity) -> bool,
) -> SessionAccountAffinity {
    let observation_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("observation runtime should build");
    observation_runtime.block_on(async {
        let state = AsyncSqliteStateStore::open(database_path)
            .await
            .expect("async state should open");
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Some(affinity) = state
                    .load_session_account_affinity(Provider::Openai, session_id)
                    .await
                    .expect("session affinity should load")
                    && predicate(&affinity)
                {
                    return affinity;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("session affinity observation should arrive")
    })
}

pub(super) fn seed_completed_active_session(
    database_path: &Path,
    account_id: &AccountId,
    reservation_id: &str,
    started_unix_seconds: u64,
    ended_unix_seconds: u64,
) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("active-session fixture runtime should build");
    runtime.block_on(async {
        let state = AsyncSqliteStateStore::open(database_path)
            .await
            .expect("active-session fixture state should open");
        let reservation_id = ReservationId::new(reservation_id);
        state
            .record_active_client_acquired(
                "responses",
                "retention-fixture",
                &reservation_id,
                account_id,
                started_unix_seconds,
                8,
            )
            .await
            .expect("active-session acquisition should persist");
        state
            .record_active_client_released(
                "responses",
                "retention-fixture",
                &reservation_id,
                ended_unix_seconds,
            )
            .await
            .expect("active-session release should persist");
        state.close().await.expect("fixture state should close");
    });
}

pub(super) fn wait_for_responses_history_compaction(
    completion_receiver: &mpsc::Receiver<MaintenanceCompletion>,
) {
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        let completion = completion_receiver
            .recv_timeout(remaining)
            .unwrap_or_else(|error| {
                panic!("responses history compaction should complete within one second: {error}")
            });
        if completion.maintenance_class() == "active_session_history_compaction"
            && completion.route_band() == "responses"
        {
            return;
        }
    }
}

pub(super) fn observed_active_session_reservation_ids(
    runtime: &tokio::runtime::Runtime,
    state: &AsyncSqliteStateStore,
) -> Vec<String> {
    runtime.block_on(async {
        state
            .active_session_events_for_route_band("responses")
            .await
            .expect("active-session events should load")
            .into_iter()
            .map(|event| event.reservation_id().as_str().to_owned())
            .collect()
    })
}

pub(super) fn assert_active_session_absent(
    runtime: &tokio::runtime::Runtime,
    state: &AsyncSqliteStateStore,
    reservation_id: &str,
) {
    assert!(
        !observed_active_session_reservation_ids(runtime, state)
            .iter()
            .any(|candidate| candidate == reservation_id),
        "maintenance should delete completed session {reservation_id}"
    );
}

pub(super) fn wait_for_previous_response_owner(
    state: &SqliteStateStore,
    owner_hash: &AffinityKeyHash,
) -> PreviousResponseAffinityOwnerLookup {
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        let owner_lookup = must_ok(AffinityRepository::load_previous_response_owner(
            state,
            owner_hash,
            RouteBand::Responses.as_str(),
        ));
        if !matches!(owner_lookup, PreviousResponseAffinityOwnerLookup::Missing)
            || Instant::now() >= deadline
        {
            return owner_lookup;
        }
        thread::sleep(Duration::from_millis(10));
    }
}

pub(super) async fn selected_account_after_provider_error_observation(
    temp_name: &str,
    observation_body: &[u8],
) -> AccountId {
    let temp_dir = ProxyTestTempDir::new(temp_name);
    let database_path = temp_dir.path().join("state.sqlite");
    let state = match SqliteStateStore::open(&database_path) {
        Ok(state) => state,
        Err(error) => panic!("state store should open: {error}"),
    };
    let observed = AccountRecord::new(
        codex_router_core::provider::Provider::Openai,
        account_id("acct_observed"),
        "observed",
        AccountStatus::Enabled,
    );
    let fallback = AccountRecord::new(
        codex_router_core::provider::Provider::Openai,
        account_id("acct_fallback"),
        "fallback",
        AccountStatus::Enabled,
    );
    persist_account_with_selector_window_specs(
        &state,
        &observed,
        "responses",
        &[(18_000, 90, true), (604_800, 90, false)],
    );
    persist_account_with_selector_window_specs(
        &state,
        &fallback,
        "responses",
        &[(18_000, 80, true), (604_800, 80, false)],
    );
    let async_state = match AsyncSqliteStateStore::open(&database_path).await {
        Ok(state) => state,
        Err(error) => panic!("async state store should open: {error}"),
    };

    if let Err(error) = record_provider_error_observation(
        &async_state,
        observed.account_id(),
        "responses",
        classify_provider_error_envelope(observation_body),
        2_000,
    )
    .await
    {
        panic!("provider error observation should persist: {error}");
    }

    let selector = AsyncRepositoryBackedAccountSelector::new_with_runtime(
        &async_state,
        Arc::new(Mutex::new(HashMap::new())),
        Arc::new(Mutex::new(HashMap::new())),
        0,
        Arc::new(|| 2_000),
    );
    let selected = match selector
        .select_upstream_account(
            &HttpProxyRequest::new(Method::Post, "/v1/responses"),
            TokenGeneration::new(1),
            None,
        )
        .await
    {
        Ok(selected) => selected,
        Err(error) => panic!("observed account should remain selectable: {error}"),
    };

    selected.account_id().clone()
}

pub(super) fn persist_previous_response_owner(
    state: &SqliteStateStore,
    previous_response_id: &str,
    affinity_secret: &RouterAffinityHashSecret,
    account_id: &codex_router_core::ids::AccountId,
) -> Result<(), codex_router_state::sqlite::StateStoreError> {
    persist_previous_response_owner_for_route(
        state,
        previous_response_id,
        affinity_secret,
        account_id,
        RouteBand::Responses,
    )
}

#[expect(
    clippy::panic_in_result_fn,
    reason = "Assertions must fail this test while fixture errors propagate through Result."
)]
pub(super) fn persist_previous_response_owner_for_route(
    state: &SqliteStateStore,
    previous_response_id: &str,
    affinity_secret: &RouterAffinityHashSecret,
    account_id: &codex_router_core::ids::AccountId,
    route_band: RouteBand,
) -> Result<(), codex_router_state::sqlite::StateStoreError> {
    let previous_response_id = match PreviousResponseId::new(previous_response_id) {
        Ok(previous_response_id) => previous_response_id,
        Err(error) => panic!("previous response id should parse: {error}"),
    };
    let affinity_key_hash = match hash_previous_response_id(affinity_secret, &previous_response_id)
    {
        Ok(affinity_key_hash) => affinity_key_hash,
        Err(error) => panic!("affinity hash should compute: {error}"),
    };
    let owner = PreviousResponseAffinityOwnerRecord::new(
        affinity_key_hash,
        account_id.clone(),
        1,
        route_band,
        AffinitySourceTransport::HttpSse,
        test_unix_seconds(),
    );

    AffinityRepository::write_previous_response_owner(state, &owner)
}

pub(super) fn test_affinity_secret() -> RouterAffinityHashSecret {
    match RouterAffinityHashSecret::new(
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    ) {
        Ok(secret) => secret,
        Err(error) => panic!("test affinity secret should parse: {error}"),
    }
}

pub(super) fn replacement_affinity_secret() -> RouterAffinityHashSecret {
    match RouterAffinityHashSecret::new(
        "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210",
    ) {
        Ok(secret) => secret,
        Err(error) => panic!("replacement affinity secret should parse: {error}"),
    }
}

pub(super) fn assert_repository_selected_account(
    state: &SqliteStateStore,
    expected_account_id: &AccountId,
    context: &str,
) {
    let selected = RepositoryBackedAccountSelector::new(state)
        .select_upstream_account(
            &HttpProxyRequest::new(Method::Post, "/v1/responses"),
            TokenGeneration::new(1),
            None,
        )
        .unwrap_or_else(|error| panic!("{context}: {error}"));
    assert_eq!(selected.account_id(), expected_account_id, "{context}");
}

pub(super) fn wait_for_durable_quota_exhaustion(
    state: &SqliteStateStore,
    completions: &mut tokio::sync::mpsc::UnboundedReceiver<QuotaWriteCompletion>,
    accounts: &[&AccountId],
) {
    must_ok(tokio::runtime::Builder::new_current_thread().build()).block_on(
        assert_quota_exhaustion_after_write(state, completions, accounts),
    );
}

pub(super) async fn assert_quota_exhaustion_after_write(
    state: &SqliteStateStore,
    completions: &mut tokio::sync::mpsc::UnboundedReceiver<QuotaWriteCompletion>,
    accounts: &[&AccountId],
) {
    for &account_id in accounts {
        let completion = completions
            .recv()
            .await
            .expect("quota writer should report completion");
        assert_eq!(&completion.account_id, account_id, "quota write account");
        assert_eq!(
            completion.route_band,
            RouteBand::Responses,
            "quota write band"
        );
        completion.result.unwrap_or_else(|error| {
            panic!(
                "provider quota write failed after enqueue for {} (responses): {error:?}",
                account_id.as_str()
            );
        });
        let snapshot = must_ok(state.load_quota_snapshot_for_route_band(account_id, "responses"));
        assert_eq!(
            snapshot.map(|snapshot| (snapshot.source(), snapshot.remaining_headroom())),
            Some((QuotaSnapshotSource::OpenAiEndpoint, 0)),
            "committed quota exhaustion should be visible for {}",
            account_id.as_str(),
        );
    }
}
