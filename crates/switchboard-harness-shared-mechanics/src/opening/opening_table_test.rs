//! Prove the admission and exact-row rules on the shared table.
use switchboard_harness_contracts::{
    HarnessSessionId, LoadAttempt, OpenSetup, RestoreVia, RunAttempt, WriterState,
};

use super::*;
use crate::opening::opening_test_adapter::{
    DisposalProbe, HeldTable, TestOpenedSession, TestOpening,
};

fn owner() -> OwnerAddress {
    use switchboard_harness_contracts::{ConnectionEpoch, OwnerIncarnation};
    OwnerAddress {
        incarnation: OwnerIncarnation("019f0000-0000-7000-8000-000000000001".parse().unwrap()),
        connection_epoch: ConnectionEpoch("019f0000-0000-7000-8000-000000000002".parse().unwrap()),
    }
}

fn create_attempt(suffix: u16) -> CreateAttempt {
    CreateAttempt(
        format!("019f0000-0000-7000-8000-{suffix:012x}")
            .parse()
            .unwrap(),
    )
}

fn setup() -> OpenSetup {
    OpenSetup {
        cwd: "/".into(),
        work_roots: Default::default(),
        scratch_folder: None,
    }
}

fn create_request() -> OpenCreateRequest {
    OpenCreateRequest {
        setup: setup(),
        requested: Default::default(),
        stopped: CancellationToken::new(),
    }
}

fn restore_request(generation: SessionGeneration) -> RestoreOpening {
    RestoreOpening {
        run: RunAttempt {
            generation,
            attempt: LoadAttempt("019f0000-0000-7000-8000-000000000011".parse().unwrap()),
        },
        session: HarnessSessionId("bound-session".into()),
        via: RestoreVia::Resume,
        setup: setup(),
        requested_mode: None,
    }
}

fn enqueue_restore(
    table: &mut OpeningTable<TestOpening>,
    generation: SessionGeneration,
) -> (OpeningInstance, LifecycleRoute) {
    let (instance, effect) = table
        .begin_restore(
            Some("bound-session"),
            owner(),
            restore_request(generation),
            CancellationToken::new(),
        )
        .unwrap_or_else(|_| panic!("restore admission"));
    let Effect::OfferRestore(start) = effect else {
        panic!("restore must offer its route");
    };
    assert!(matches!(
        table.apply(Slot::Run(generation), &instance, Command::WriterAccepted),
        Ok(Effect::None)
    ));
    (instance, start.resources.route)
}

fn seed_mixed_create(
    table: &mut OpeningTable<TestOpening>,
    create: CreateAttempt,
) -> OpeningInstance {
    // The stop sweep has a defined order even though normal admission never mixes the slot kinds.
    let (instance, effect) = table
        .begin_vacant(
            false,
            OpenAttempt::Create(create),
            Command::OpenCreate(Box::new(create_request())),
        )
        .unwrap_or_else(|_| panic!("table-local create seed"));
    assert!(matches!(effect, Effect::OfferCreate(_)));
    assert!(matches!(
        table.apply(Slot::Create(create), &instance, Command::WriterAccepted),
        Ok(Effect::None)
    ));
    instance
}

#[test]
fn stop_all_applies_in_slot_order_and_only_upgrades_to_retirement() {
    let mut table = OpeningTable::default();
    let first_run = SessionGeneration::from_number(1);
    let later_run = SessionGeneration::from_number(2);
    let (later, _) = enqueue_restore(&mut table, later_run);
    let (first, _) = enqueue_restore(&mut table, first_run);
    let later_create = seed_mixed_create(&mut table, create_attempt(32));
    let first_create = seed_mixed_create(&mut table, create_attempt(31));
    let stopped = LifecycleRefusal::Writer(WriterState::Stopped);

    let effects = table
        .stop_all(stopped)
        .expect("first stop reaches all slots");
    assert_eq!(effects.len(), 4);
    for (result, instance) in effects
        .into_iter()
        .zip([first_create, later_create, first, later])
    {
        let Ok(Effect::Release(release)) = result else {
            panic!("stopping a pending writer retains its exact receipt");
        };
        assert!(release.is_for_test(&instance));
    }
    assert_eq!(table.port_refusal(), Some(stopped));
    assert!(table.stop_all(stopped).is_none());

    let retired = LifecycleRefusal::ConnectionRetired;
    let upgraded = table
        .stop_all(retired)
        .expect("retirement upgrades the stored refusal");
    assert_eq!(upgraded.len(), 4);
    assert!(upgraded.iter().all(Result::is_ok));
    assert_eq!(table.port_refusal(), Some(retired));
    assert!(table.stop_all(stopped).is_none());
    assert!(table.stop_all(retired).is_none());
}

#[test]
fn successful_reader_start_rekeys_an_exact_create_to_its_run() {
    let mut table: OpeningTable<TestOpening> = OpeningTable::default();
    let create = create_attempt(21);
    let generation = SessionGeneration::from_number(3);
    let (instance, _) = table
        .begin_create(false, create, create_request())
        .unwrap_or_else(|_| panic!("create admission"));
    let create_slot = Slot::Create(create);
    assert!(matches!(
        table.apply(create_slot, &instance, Command::WriterAccepted),
        Ok(Effect::None)
    ));
    let held_table = HeldTable::default();
    let session = || {
        Box::new(TestOpenedSession::new(
            DisposalProbe::default().bind(&held_table),
        ))
    };
    assert!(matches!(
        table.apply(create_slot, &instance, Command::Created(session())),
        Ok(Effect::None)
    ));
    let input = || ActivationSetup::Created {
        attempt: create,
        generation,
    };
    let stopped = CancellationToken::new();
    assert!(
        table
            .begin_reader(true, owner(), input(), &stopped)
            .is_err()
    );
    assert!(table.exact(create_slot, &instance).is_some());
    assert!(table.runs_is_empty());
    assert!(matches!(
        table.apply(
            create_slot,
            &instance,
            Command::Configure(stopped.child_token())
        ),
        Ok(Effect::Configure { .. })
    ));
    assert!(matches!(
        table.apply(create_slot, &instance, Command::Configured(session())),
        Ok(Effect::None)
    ));

    let (started, run, effect) = table
        .begin_reader(true, owner(), input(), &stopped)
        .unwrap_or_else(|_| panic!("the configured create may start its reader"));

    assert!(started.is_same_as(&instance));
    assert_eq!(run, generation);
    assert!(matches!(effect, Effect::StartReader(_)));
    assert!(table.get(create_slot).is_none());
    assert!(table.exact(Slot::Run(generation), &instance).is_some());
    assert_eq!(
        table
            .targeted(OpenAttempt::Create(create))
            .map(OpeningRow::phase),
        Some(OpeningPhase::StartingReader)
    );
}

#[test]
fn close_target_preserves_idle_create_mismatch_and_exact_run_answers() {
    let mut table: OpeningTable<TestOpening> = OpeningTable::default();
    let generation = SessionGeneration::from_number(1);
    assert_eq!(
        table.close_target(generation, false).err(),
        Some(OpeningPhase::UnboundIdle.refusal())
    );
    assert_eq!(
        table.close_target(generation, true).err(),
        Some(OpeningPhase::BoundIdle.refusal())
    );
    let mut create_only = OpeningTable::default();
    seed_mixed_create(&mut create_only, create_attempt(17));
    assert_eq!(
        create_only.close_target(generation, false).err(),
        Some(OpeningPhase::AwaitingCreate.refusal())
    );
    let (instance, _) = enqueue_restore(&mut table, generation);
    assert_eq!(
        table.close_target(generation, false).err(),
        Some(OpeningPhase::AwaitingRestore.refusal())
    );
    assert_eq!(
        table
            .close_target(SessionGeneration::from_number(2), true)
            .err(),
        Some(LifecycleRefusal::AttemptMismatch)
    );
    assert!(
        table
            .close_target(generation, true)
            .expect("exact bound row")
            .is_same_as(&instance)
    );
    let refusal = LifecycleRefusal::Writer(WriterState::Stopped);
    assert!(table.stop_all(refusal).is_some());
    assert_eq!(table.close_target(generation, true).err(), Some(refusal));
}

#[test]
fn removed_routes_report_in_generation_order_and_skip_route_loss_already_seen() {
    let mut table = OpeningTable::default();
    let first_generation = SessionGeneration::from_number(1);
    let missing_generation = SessionGeneration::from_number(2);
    let last_generation = SessionGeneration::from_number(3);
    let (last, last_route) = enqueue_restore(&mut table, last_generation);
    let (removed, removed_route) = enqueue_restore(&mut table, missing_generation);
    let (first, first_route) = enqueue_restore(&mut table, first_generation);
    for route in [&first_route, &removed_route, &last_route] {
        route.mark_removed();
    }
    assert!(
        table
            .apply(
                Slot::Run(missing_generation),
                &removed,
                Command::RouteRemoved(removed_route)
            )
            .is_ok()
    );
    assert_eq!(
        table.phase_of(missing_generation),
        Some(OpeningPhase::RouteLost)
    );
    let sticky = LifecycleRefusal::Writer(WriterState::Stopped);
    table.port_refusal = Some(sticky);

    let (routes, refusal) = table.removed_routes();

    assert_eq!(refusal, Some(sticky));
    let [before, after] = routes.as_slice() else {
        panic!("only two unobserved removals: {}", routes.len());
    };
    assert!(
        before.0 == Slot::Run(first_generation)
            && before.1.is_same_as(&first)
            && before.2.is_same_as(&first_route)
    );
    assert!(
        after.0 == Slot::Run(last_generation)
            && after.1.is_same_as(&last)
            && after.2.is_same_as(&last_route)
    );
}
