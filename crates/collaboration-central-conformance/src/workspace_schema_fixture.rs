//! Expected application tables in a freshly migrated organization and workspace.

pub const APPLICATION_TABLE_NAMES: &str = "SELECT name FROM sqlite_schema WHERE type='table' AND substr(name,1,7)<>'sqlite_' AND name<>'_sqlx_migrations' ORDER BY name";

pub const ORGANIZATION_TABLES: &[&str] = &[
    "access_actor_identities",
    "actor_delegates",
    "actor_identities",
    "actor_persons",
    "actor_sessions",
    "admin_used_requests",
    "approval_applying_time",
    "approval_request_destinations",
    "approval_requests",
    "approval_router_runs",
    "automation_schedules",
    "automation_wakes",
    "directory_workspaces",
    "engine_due_entries",
    "engine_work_attempts",
    "login_machine_allowances",
    "login_machine_approvals",
    "login_machines",
    "mail_messages",
    "mail_notification_window_items",
    "mail_notification_windows",
    "mail_workspace_drains",
    "runtime_request_log",
    "signin_authorization_floors",
    "signin_change_records",
    "signin_organization_identity",
    "signin_person_authorization_floors",
];

/// The owning-host fixtures install the due-engine probe migration as well.
pub fn organization_test_tables() -> Vec<&'static str> {
    let mut tables = ORGANIZATION_TABLES.to_vec();
    tables.extend([
        "engine_alarm_only_items",
        "engine_probe_groups",
        "engine_probe_items",
    ]);
    tables.sort_unstable();
    tables
}

pub const WORKSPACE_TABLES: &[&str] = &[
    "workspace_areas",
    "workspace_backlog_items",
    "workspace_changes",
    "workspace_deliverable_dependencies",
    "workspace_deliverable_roles",
    "workspace_deliverables",
    "workspace_initiatives",
    "workspace_issues",
    "workspace_item_references",
    "workspace_links",
    "workspace_posts",
    "workspace_project_roles",
    "workspace_projects",
    "workspace_read_marks",
    "workspace_request_log",
    "workspace_thread_members",
    "workspace_threads",
    "workspace_work_log_entries",
    "workspace_work_log_tags",
];

/// Inspect named and inline uniqueness using the database's catalog.
pub const BUSINESS_UNIQUENESS: &str = r#"
SELECT tables.name, indexes.name, indexes.origin, CAST(indexes.partial AS TEXT),
       (SELECT group_concat(name) FROM
           (SELECT name FROM pragma_index_info(indexes.name) ORDER BY seqno))
FROM sqlite_schema tables, pragma_index_list(tables.name) indexes
WHERE tables.type = 'table'
  AND tables.name IN (
      'actor_persons', 'actor_sessions', 'approval_request_destinations',
      'engine_due_entries', 'login_machine_allowances'
  )
  AND indexes."unique" = 1 AND indexes.origin <> 'pk'
ORDER BY tables.name, indexes.name"#;

pub fn central_business_uniqueness() -> Vec<Vec<central_storage::SqlValue>> {
    use central_storage::SqlValue;
    [
        ("actor_persons", "actor_persons_person", "person_id", false),
        (
            "actor_sessions",
            "actor_sessions_machine_harness",
            "machine_id,harness_session_id",
            false,
        ),
        (
            "actor_sessions",
            "actor_sessions_machine_operation",
            "machine_id,create_operation_id",
            false,
        ),
        (
            "approval_request_destinations",
            "approval_destinations_one_people",
            "approval_row_id",
            true,
        ),
        (
            "approval_request_destinations",
            "approval_destinations_people_lookup",
            "organization_id,request_id",
            true,
        ),
        (
            "approval_request_destinations",
            "approval_destinations_position",
            "approval_row_id,position",
            false,
        ),
        (
            "approval_request_destinations",
            "approval_destinations_session",
            "approval_row_id,session_id",
            true,
        ),
        (
            "engine_due_entries",
            "engine_due_entries_kind_role_subject",
            "work_kind,entry_role,subject_key",
            false,
        ),
        (
            "login_machine_allowances",
            "login_machine_allowances_machine_workspace",
            "machine_id,workspace_id",
            false,
        ),
    ]
    .into_iter()
    .map(|(table, index, columns, is_partial)| {
        vec![
            SqlValue::Text(table.to_owned()),
            SqlValue::Text(index.to_owned()),
            SqlValue::Text("c".to_owned()),
            SqlValue::Text(u8::from(is_partial).to_string()),
            SqlValue::Text(columns.to_owned()),
        ]
    })
    .collect()
}

pub const MAIL_MESSAGE_COLUMN_NAMES: &str =
    "SELECT name FROM pragma_table_info('mail_messages') ORDER BY cid";

pub const MAIL_MESSAGE_COLUMNS: &[&str] = &[
    "message_id",
    "sender",
    "target_machine_id",
    "target_session",
    "message_kind",
    "body",
    "state",
    "failure_reason",
    "source_change_id",
    "recipient_actor",
    "source_workspace_id",
    "source_thread_id",
    "created_at",
    "updated_at",
    "cancel_due_at",
    "sender_request_id",
    "delivery_window_ends_at",
    "delivered_arrived_at",
    "seen_completed_at",
    "deadline_settled_at",
];
