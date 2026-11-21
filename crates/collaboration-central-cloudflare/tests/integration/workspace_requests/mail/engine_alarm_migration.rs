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
        .control::<Result<Value, String>>(endpoint, &"upgrade")
        .await
        .unwrap()
        .unwrap();
    let fresh_snapshot = fresh
        .control::<Result<Value, String>>(endpoint, &"seedCurrent")
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
        .control::<Result<Value, String>>(endpoint, &"upgrade")
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
