use super::super::support::{Fixture, id};
use fleet_identity_protocol::OrganizationId;
use serde_json::Value;

#[tokio::test]
async fn production_upgrade_reopens_and_an_old_fixture_ledger_is_preserved() {
    let fixture = Fixture::new().await;
    let organization = |number| {
        serde_json::from_value::<OrganizationId>(serde_json::json!(
            id::<workspace_ids::WorkspaceId>(number)
        ))
        .unwrap()
    };
    let upgrade = fixture.host.cold_runtime(&organization(701));
    let fresh = fixture.host.cold_runtime(&organization(702));
    let old_fixture = fixture.host.cold_runtime(&organization(703));
    let endpoint = "raw-engine-due-migration";
    let before = upgrade
        .control::<Result<Value, String>>(endpoint, &"seedMain")
        .await
        .unwrap()
        .unwrap();
    let old_ledger = before["ledger"]["rows"].as_array().unwrap();
    assert_eq!(old_ledger.len(), 4);
    let completed = upgrade
        .control::<Result<Value, String>>(endpoint, &"upgradeDueViews")
        .await
        .unwrap()
        .unwrap();
    let fresh_snapshot = fresh
        .control::<Result<Value, String>>(endpoint, &"seedDueViews")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        &completed["ledger"]["rows"].as_array().unwrap()[..4],
        old_ledger
    );
    assert_eq!(completed, fresh_snapshot);
    assert_eq!(completed["alarm"], serde_json::json!([300_000, null]));
    let _ = upgrade.control::<Value>(endpoint, &"reset").await;
    assert_eq!(
        upgrade
            .control::<Result<Value, String>>(endpoint, &"inspect")
            .await
            .unwrap()
            .unwrap(),
        completed,
    );
    let saved = old_fixture
        .control::<Result<Value, String>>(endpoint, &"seedOldFixture")
        .await
        .unwrap()
        .unwrap();
    let failure = old_fixture
        .control::<Result<Value, String>>(endpoint, &"upgradeDueViews")
        .await
        .unwrap()
        .unwrap_err();
    assert!(
        failure.contains("migration 5") && failure.contains("modified"),
        "{failure}"
    );
    assert_eq!(
        old_fixture
            .control::<Result<Value, String>>(endpoint, &"inspect")
            .await
            .unwrap()
            .unwrap(),
        saved,
    );
    fixture.host.close().await;
}

#[tokio::test]
async fn fresh_production_catalog_contains_machine_allowances_and_an_empty_due_table() {
    let fixture = Fixture::new().await;
    let org = serde_json::from_value::<OrganizationId>(serde_json::json!(id::<
        workspace_ids::WorkspaceId,
    >(704)))
    .unwrap();
    let current = fixture.host.cold_runtime(&org);
    let catalog = current
        .control::<Result<Value, String>>("raw-engine-due-migration", &"seedCurrent")
        .await
        .unwrap()
        .unwrap();
    let ledger = catalog["ledger"]["rows"].as_array().unwrap();
    assert_eq!(ledger.len(), 11);
    let tables = current.read(central_storage::Statement {
        sql: "SELECT COUNT(*) AS allowances, (SELECT COUNT(*) FROM engine_due_entries) AS due_entries FROM login_machine_allowances",
        args: vec![],
    }).await.unwrap();
    let zero = central_storage::SqlValue::Integer(0.try_into().unwrap());
    assert_eq!(tables.rows, [[zero.clone(), zero]]);
    assert_eq!(catalog["alarm"], serde_json::json!([null, null]));
    fixture.host.close().await;
}
