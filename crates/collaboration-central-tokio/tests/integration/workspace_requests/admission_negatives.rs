use super::*;
use central_storage::{
    AlarmUpdate, BatchOutcome, GuardedStatement, RowExpectation, StatementBatch,
};
#[tokio::test]
async fn live_admission_refuses_revoked_expired_wrong_credential_and_archived_directory_without_domain_writes()
 {
    let f = Fixture::new().await;
    let runtime = f.host.central(f.caller.organization_id()).await.unwrap();
    let schema = runtime
        .runtime().statement_host()
        .read(Statement {
            sql: "SELECT name, pk FROM pragma_table_info('login_machines') WHERE name IN ('machine_id', 'installation_id') ORDER BY cid",
            args: vec![],
        })
        .await
        .unwrap();
    assert_eq!(
        schema.rows,
        vec![vec![
            text("machine_id"),
            central_storage::SqlValue::Integer(central_storage::SafeInteger::new(1).unwrap())
        ]]
    );
    let link = runtime
        .runtime().statement_host()
        .read(Statement {
            sql: "SELECT \"table\", \"to\" FROM pragma_foreign_key_list('mail_messages') WHERE \"from\"='target_machine_id'",
            args: vec![],
        })
        .await
        .unwrap();
    assert_eq!(
        link.rows,
        vec![vec![text("login_machines"), text("machine_id")]]
    );
    let legacy = runtime
        .runtime()
        .statement_host()
        .read(Statement {
            sql: "SELECT name FROM sqlite_schema WHERE type='table' AND name='machine_credentials'",
            args: vec![],
        })
        .await
        .unwrap();
    assert!(legacy.rows.is_empty());
    let operation = m::WorkspaceOperation::CreateArea {
        name: name("Must not be written"),
        description: "Test description".to_owned().try_into().unwrap(),
    };
    let now = router_clock::RuntimeClock::utc_now(f.clock.as_ref()).timestamp();
    let valid_token = signed::machine_token(
        organization().as_str(),
        &id::<workspace_ids::MachineId>(2).to_string(),
        &id::<m::WorkspaceId>(4).to_string(),
        now,
        now + 600,
    )
    .unwrap();
    let mut mismatched = session(&f.caller);
    mismatched.machine_id = id(999);
    assert!(
        collaboration_central_core::WorkspaceCaller::new(
            f.caller.connection().clone(),
            m::Caller::Session(mismatched)
        )
        .is_err()
    );
    let wrong = signed::tamper_signature(&valid_token).unwrap();
    assert!(
        runtime
            .admit_machine_connection(&wrong)
            .await
            .unwrap()
            .is_err()
    );
    // The original socket deadline remains binding after activity from another grant.
    f.advance(std::time::Duration::from_secs(600)).await;
    let renewal = runtime.decide_machine_token(&serde_json::from_value(serde_json::json!({
        "organizationId":organization(),"enrollmentId":id::<m::WorkspaceId>(4),"clientId":id::<m::WorkspaceId>(4)
    })).unwrap()).await.unwrap();
    assert!(matches!(
        renewal,
        fleet_contracts::MachineTokenDecision::Grant { .. }
    ));
    assert_eq!(
        call(
            &f.host,
            &f.caller,
            Some(f.workspace.clone()),
            960,
            operation.clone()
        )
        .await
        .result,
        m::WorkspaceResult::CentralUnavailable
    );
    let renewed = f.fresh_caller(&f.caller).await;
    let machine_id = text(f.caller.caller().machine_id());
    let workspace_id = text(&f.workspace);
    let edit = |sql, args| GuardedStatement {
        statement: Statement { sql, args },
        expect: RowExpectation::Exactly(1),
    };
    let run = |statements| StatementBatch {
        statements,
        alarm: AlarmUpdate::Unchanged,
    };
    let withdraw_allowance = || {
        edit(
            "UPDATE login_machine_allowances SET state='withdrawn' WHERE machine_id=? AND state='active'",
            vec![machine_id.clone()],
        )
    };
    let restore_allowance = || {
        edit(
            collaboration_central_conformance::REPLACE_FIXTURE_ALLOWANCES_SQL,
            vec![
                text(serde_json::to_string(&[&f.workspace]).unwrap()),
                machine_id.clone(),
            ],
        )
    };
    let edits = [
        (
            m::RefusalReason::MachineRevoked,
            vec![
                edit(
                    "UPDATE login_machine_approvals SET state='ended',end_reason='owner',ended_at=last_grant_at,last_grant_at=NULL WHERE machine_id=? AND state='enrolled'",
                    vec![machine_id.clone()],
                ),
                edit(
                    "UPDATE login_machines SET state='revoked',enrollment_generation=enrollment_generation+1 WHERE machine_id=?",
                    vec![machine_id.clone()],
                ),
                withdraw_allowance(),
            ],
            vec![
                edit(
                    "UPDATE login_machines SET state='enrolled',enrollment_generation=enrollment_generation-1 WHERE machine_id=?",
                    vec![machine_id.clone()],
                ),
                edit(
                    "UPDATE login_machine_approvals SET state='enrolled',end_reason=NULL,last_grant_at=ended_at,ended_at=NULL WHERE machine_id=? AND end_reason='owner'",
                    vec![machine_id.clone()],
                ),
                restore_allowance(),
            ],
        ),
        (
            m::RefusalReason::WorkspaceNotAllowed,
            vec![edit(
                "UPDATE directory_workspaces SET state='archived' WHERE workspace_id=?",
                vec![workspace_id.clone()],
            )],
            vec![edit(
                "UPDATE directory_workspaces SET state='active' WHERE workspace_id=?",
                vec![workspace_id],
            )],
        ),
        (
            m::RefusalReason::WorkspaceNotAllowed,
            vec![withdraw_allowance()],
            vec![restore_allowance()],
        ),
    ];
    for (index, (refusal, change, restore)) in edits.into_iter().enumerate() {
        assert!(matches!(
            runtime
                .runtime()
                .statement_host()
                .run_batch(run(change))
                .await
                .unwrap(),
            BatchOutcome::Applied { .. }
        ));
        assert_eq!(
            call(
                &f.host,
                &renewed,
                Some(f.workspace.clone()),
                961 + u32::try_from(index).unwrap(),
                operation.clone()
            )
            .await
            .result,
            m::WorkspaceResult::Refused(refusal)
        );
        assert!(matches!(
            runtime
                .runtime()
                .statement_host()
                .run_batch(run(restore))
                .await
                .unwrap(),
            BatchOutcome::Applied { .. }
        ));
    }
    let counts=runtime.runtime().inspect_workspace(&f.workspace,Statement{sql:"SELECT (SELECT COUNT(*) FROM workspace_areas) AS areas,(SELECT COUNT(*) FROM workspace_changes) AS changes,(SELECT COUNT(*) FROM workspace_request_log) AS log",args:vec![]}).await.unwrap();
    for cell in &counts.rows[0] {
        assert_eq!(
            *cell,
            central_storage::SqlValue::Integer(central_storage::SafeInteger::new(0).unwrap())
        );
    }
    assert!(matches!(
        call(&f.host, &renewed, Some(f.workspace.clone()), 970, operation)
            .await
            .result,
        m::WorkspaceResult::Done(m::WorkspaceAnswer::Created { .. })
    ));
    f.host.close().await;
}

#[tokio::test]
async fn fresh_organization_and_workspace_store_use_the_approved_table_names() {
    use collaboration_central_conformance::workspace_schema_fixture as schema;

    let f = Fixture::new().await;
    let runtime = f.host.central(f.caller.organization_id()).await.unwrap();
    let names = || Statement {
        sql: schema::APPLICATION_TABLE_NAMES,
        args: vec![],
    };
    let organization = runtime
        .runtime()
        .statement_host()
        .read(names())
        .await
        .unwrap();
    let workspace = runtime
        .runtime()
        .inspect_workspace(&f.workspace, names())
        .await
        .unwrap();
    let expected_organization = schema::organization_test_tables();
    for (observed, expected) in [
        (organization, expected_organization.as_slice()),
        (workspace, schema::WORKSPACE_TABLES),
    ] {
        let expected: Vec<_> = expected.iter().map(|name| vec![text(name)]).collect();
        assert_eq!(observed.rows, expected);
    }
    let mail_columns = runtime
        .runtime()
        .statement_host()
        .read(Statement {
            sql: schema::MAIL_MESSAGE_COLUMN_NAMES,
            args: vec![],
        })
        .await
        .unwrap();
    let expected_mail_columns: Vec<_> = schema::MAIL_MESSAGE_COLUMNS
        .iter()
        .map(|name| vec![text(name)])
        .collect();
    assert_eq!(mail_columns.rows, expected_mail_columns);
    f.host.close().await;
}
