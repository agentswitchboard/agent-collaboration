use super::support::*;

async fn change_page(fixture: &MachineFixture) -> Result<(u16, Value)> {
    let owner = signed::Person::owner(ORG, fixture.now())?;
    let reply = fixture
        .inner
        .person(
            signed::RECORDS,
            &json!({"organizationId":ORG,"after":null,"limit":100}),
            &owner,
            &signed::records_intent(ORG, None, 100),
            &signed::fresh_request_id(fixture.now())?,
        )
        .await?;
    Ok((
        reply.status,
        if reply.body.is_empty() {
            Value::Null
        } else {
            reply.value()?
        },
    ))
}

#[tokio::test]
async fn signed_change_page_decodes_machine_allowances_and_rejects_corrupt_rows() -> Result<()> {
    let fixture = MachineFixture::new().await?;
    verify_change_page(fixture, None).await
}

#[tokio::test]
async fn allowance_audit_upgrade_preserves_the_preexisting_record_and_uses_the_same_page_contract()
-> Result<()> {
    use sqlx::{SqlitePool, sqlite::SqliteConnectOptions};

    let root = tempfile::Builder::new()
        .prefix("signin-upgrade-")
        .tempdir()?;
    let central = root.path().join("central");
    std::fs::create_dir_all(&central)?;
    let pool = SqlitePool::connect_with(
        SqliteConnectOptions::new()
            .filename(central.join(format!("{ORG}.sqlite")))
            .create_if_missing(true),
    )
    .await?;
    fleet_storage_schema::workspace_migrations::central_migrator()
        .run_to(7, &pool)
        .await?;
    sqlx::query("INSERT INTO signin_change_records(id,request_id,kind,origin,target_id,created_at,updated_at) VALUES ('01890f2e-7b4c-7cc0-98c4-000000000023','audit-before-upgrade','workspaceCreated','automatic','019f0000-0000-7000-8000-00000000bc03',1000,1000)")
        .execute(&pool)
        .await?;
    pool.close().await;
    let fixture = MachineFixture {
        inner: super::super::support::Fixture::open_signin_store(root, signed::ISSUER).await?,
    };
    fixture.inner.ready().await?;
    seed_workspaces(&fixture).await?;
    verify_change_page(fixture, Some("audit-before-upgrade")).await
}

async fn verify_change_page(fixture: MachineFixture, legacy_request: Option<&str>) -> Result<()> {
    fixture.inner.ready().await?;
    let request = approval(
        0xcf41,
        INSTALLATION,
        "Allowance audit",
        &[WORKSPACE],
        fixture.now(),
    );
    let (status, approved) = fixture.post(DEVICE, &request).await?;
    assert_eq!(status, 200);
    assert_eq!(
        approved.get("result").and_then(Value::as_str),
        Some("approved")
    );

    let (status, reply) = change_page(&fixture).await?;
    assert_eq!(status, 200);
    let records = reply
        .get("records")
        .and_then(Value::as_array)
        .ok_or("records missing")?;
    let provisioning = records
        .iter()
        .find(|record| {
            record.get("kind").and_then(Value::as_str) == Some("organizationProvisioned")
        })
        .ok_or("flat provisioning record missing")?;
    assert_eq!(
        provisioning.get("targetId").and_then(Value::as_str),
        Some(ORG)
    );
    if let Some(request_id) = legacy_request {
        assert!(records.contains(&json!({
            "id":"01890f2e-7b4c-7cc0-98c4-000000000023",
            "requestId":request_id,"kind":"workspaceCreated",
            "origin":{"kind":"automatic"},"targetId":WORKSPACE,"createdAtMillis":1000
        })));
    }
    let allowance = records
        .iter()
        .find(|record| {
            record.get("kind").and_then(Value::as_str) == Some("machineAllowanceChanged")
        })
        .ok_or("typed allowance record missing")?;
    assert_eq!(
        allowance.get("machineId").and_then(Value::as_str),
        Some(INSTALLATION)
    );
    assert_eq!(
        allowance.get("workspaceId").and_then(Value::as_str),
        Some(WORKSPACE)
    );
    assert_eq!(
        allowance.get("state").and_then(Value::as_str),
        Some("active")
    );
    assert_eq!(
        allowance.get("requestId"),
        Some(&json!(format!(
            "{}:machineAllowanceChanged:{WORKSPACE}",
            id(0xcf41)
        )))
    );
    assert!(allowance.get("targetId").is_none());

    for (corrupt, restore, restored) in [
        (
            "UPDATE signin_change_records SET workspace_id=NULL WHERE kind='machineAllowanceChanged'",
            "UPDATE signin_change_records SET workspace_id=? WHERE kind='machineAllowanceChanged'",
            central_storage::SqlValue::Text(WORKSPACE.to_owned()),
        ),
        (
            "UPDATE signin_change_records SET allowance_state=NULL WHERE kind='machineAllowanceChanged'",
            "UPDATE signin_change_records SET allowance_state=? WHERE kind='machineAllowanceChanged'",
            central_storage::SqlValue::Text("active".to_owned()),
        ),
        (
            "UPDATE signin_change_records SET allowance_state='unknown' WHERE kind='machineAllowanceChanged'",
            "UPDATE signin_change_records SET allowance_state=? WHERE kind='machineAllowanceChanged'",
            central_storage::SqlValue::Text("active".to_owned()),
        ),
        (
            "UPDATE signin_change_records SET allowance_state='active' WHERE kind='organizationProvisioned'",
            "UPDATE signin_change_records SET allowance_state=? WHERE kind='organizationProvisioned'",
            central_storage::SqlValue::Null,
        ),
    ] {
        fixture.execute(corrupt, vec![]).await?;
        assert_eq!(
            change_page(&fixture).await?,
            (503, Value::Null),
            "{corrupt}"
        );
        fixture.execute(restore, vec![restored]).await?;
    }
    assert_eq!(change_page(&fixture).await?, (200, reply));
    fixture.close().await
}
