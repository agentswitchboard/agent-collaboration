use super::support::*;
use central_storage::SqlValue;
use collaboration_central_conformance::signin_fixtures::{self as signed, NOW_SECONDS, Person};
use serde_json::{Value, json};

const FIRST: &str = "019f0000-0000-7000-8000-00000000d110";
const ARCHIVED: &str = "019f0000-0000-7000-8000-00000000d120";
const LAST: &str = "019f0000-0000-7000-8000-00000000d130";
const INSERTED_BEHIND: &str = "019f0000-0000-7000-8000-00000000d105";
const INSERTED_AHEAD: &str = "019f0000-0000-7000-8000-00000000d125";
const OTHER_ORG: &str = "019f0000-0000-7000-8000-00000000b2b1";

async fn insert_workspace(fixture: &Fixture, id: &str, name: &str, state: &str) {
    fixture.execute(
        "INSERT INTO directory_workspaces VALUES (?,?,'description',?,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')",
        [id, name, state].into_iter().map(|value| SqlValue::Text(value.to_owned())).collect(),
    ).await;
}

fn workspace_row(id: &str, name: &str, state: &str) -> Value {
    json!({"workspaceId":id,"name":name,"description":"description","state":state})
}

async fn list_page(
    fixture: &Fixture,
    person: &Person,
    after: Option<&str>,
    limit: Option<u32>,
) -> (u16, Value) {
    let organization = &person.organization;
    let body = signed::list_workspaces_body(organization, after, limit);
    let headers = person
        .headers(
            signed::LIST_WORKSPACES,
            &signed::list_workspaces_intent(organization, after, limit.unwrap_or(100)),
            &signed::fresh_request_id(NOW_SECONDS).unwrap(),
            NOW_SECONDS,
        )
        .unwrap();
    fixture
        .post_with(signed::LIST_WORKSPACES, &body, &headers)
        .await
}

#[tokio::test]
async fn list_workspaces_walks_by_id_without_duplicates_and_preserves_the_lookup_row() {
    let fixture = Fixture::new().await;
    assert_eq!(fixture.provision().await.0, 200);
    fixture.floors().await;
    let mut reader = fixture.owner();
    reader.second_factor_at = None;
    reader.permissions = vec!["organization:view".to_owned()];

    assert_eq!(
        list_page(&fixture, &reader, None, None).await,
        (
            200,
            json!({"result":"workspaces","workspaces":[],"next":null})
        )
    );

    insert_workspace(&fixture, FIRST, "First", "active").await;
    assert_eq!(
        list_page(&fixture, &reader, None, Some(1)).await,
        (
            200,
            json!({"result":"workspaces","workspaces":[workspace_row(FIRST,"First","active")],"next":null})
        )
    );

    insert_workspace(&fixture, LAST, "Last", "active").await;
    insert_workspace(&fixture, ARCHIVED, "Archived", "archived").await;
    let (status, first_page) = list_page(&fixture, &reader, None, Some(1)).await;
    assert_eq!(status, 200);
    assert_eq!(
        first_page,
        json!({"result":"workspaces","workspaces":[workspace_row(FIRST,"First","active")],"next":FIRST})
    );
    let cursor = first_page["next"].as_str().unwrap();

    insert_workspace(&fixture, INSERTED_AHEAD, "Ahead", "active").await;
    insert_workspace(&fixture, INSERTED_BEHIND, "Behind", "active").await;
    let (status, last_page) = list_page(&fixture, &reader, Some(cursor), Some(3)).await;
    assert_eq!(status, 200);
    assert_eq!(
        last_page,
        json!({"result":"workspaces","workspaces":[workspace_row(ARCHIVED,"Archived","archived"),workspace_row(INSERTED_AHEAD,"Ahead","active"),workspace_row(LAST,"Last","active")],"next":null})
    );
    let lookup_headers = reader
        .headers(
            signed::LOOKUP,
            &signed::lookup_intent(ORG, ARCHIVED),
            &signed::fresh_request_id(NOW_SECONDS).unwrap(),
            NOW_SECONDS,
        )
        .unwrap();
    assert_eq!(
        fixture
            .post_with(
                signed::LOOKUP,
                &json!({"organizationId":ORG,"workspaceId":ARCHIVED}),
                &lookup_headers,
            )
            .await,
        (
            200,
            json!({"result":"found","workspace":last_page["workspaces"][0]})
        )
    );
    fixture.close().await;
}

#[tokio::test]
async fn list_workspaces_refuses_wrong_permission_invalid_pages_and_corrupt_state_and_scopes_to_org()
 {
    let fixture = Fixture::new().await;
    assert_eq!(fixture.provision().await.0, 200);
    fixture.floors().await;
    insert_workspace(&fixture, FIRST, "Private A", "active").await;
    let reader = fixture.owner();
    let mut denied = fixture.owner();
    denied
        .permissions
        .retain(|permission| permission != "organization:view");
    assert_eq!(
        list_page(&fixture, &denied, None, None).await,
        (200, json!({"result":"refused","reason":"notPermitted"}))
    );
    for limit in [0, 101] {
        assert_eq!(
            list_page(&fixture, &reader, None, Some(limit)).await,
            (401, Value::Null)
        );
    }
    fixture.host.central(&OTHER_ORG.to_owned().try_into().unwrap()).await;
    let stamp = signed::stamp(OTHER_ORG, NOW_SECONDS).unwrap();
    assert_eq!(
        fixture
            .post(
                PROVISION,
                &json!({"organizationId":OTHER_ORG,"organizationStatement":stamp})
            )
            .await
            .0,
        200
    );
    for mut floor in [org_revision(3, "active"), person_revision(4, "active")] {
        floor.as_object_mut().unwrap().insert("organizationId".to_owned(), json!(OTHER_ORG));
        assert_eq!(fixture.post(REVISIONS, &floor).await.0, 200);
    }
    let other_reader = Person::owner(OTHER_ORG, NOW_SECONDS).unwrap();
    assert_eq!(
        list_page(&fixture, &other_reader, None, Some(100)).await,
        (
            200,
            json!({"result":"workspaces","workspaces":[],"next":null})
        )
    );

    fixture
        .execute(
            "UPDATE directory_workspaces SET state='unrecognized' WHERE workspace_id=?",
            vec![SqlValue::Text(FIRST.to_owned())],
        )
        .await;
    assert_eq!(
        list_page(&fixture, &reader, None, None).await,
        (503, Value::Null)
    );
    fixture.close().await;
}

#[tokio::test]
async fn list_workspaces_default_page_and_late_cursor_bound_durable_object_rows_read() {
    use central_storage::{
        AlarmUpdate, BatchOutcome, GuardedStatement, RowExpectation, Statement, StatementBatch,
    };
    let fixture = Fixture::new().await;
    assert_eq!(fixture.provision().await.0, 200);
    fixture.floors().await;
    let directory = fixture.runtime().await;
    let seeded = directory.run_batch(StatementBatch {
        statements: vec![GuardedStatement {
            statement: Statement {
                sql: "WITH RECURSIVE fixture_rows(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM fixture_rows WHERE n<160) INSERT INTO directory_workspaces SELECT printf('019f0000-0000-7000-8000-%012d',n), printf('Workspace %d',n), '', 'active', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z' FROM fixture_rows",
                args: vec![],
            },
            expect: RowExpectation::Exactly(160),
        }],
        alarm: AlarmUpdate::Unchanged,
    }).await.unwrap();
    assert!(matches!(seeded, BatchOutcome::Applied { .. }));
    let reader = fixture.owner();
    let (status, page) = list_page(&fixture, &reader, None, None).await;
    assert_eq!(status, 200);
    assert_eq!(page["workspaces"].as_array().unwrap().len(), 100);
    assert_eq!(page["next"], "019f0000-0000-7000-8000-000000000100");
    let after = page["next"].as_str().unwrap();
    let (status, next_page) = list_page(&fixture, &reader, Some(after), Some(1)).await;
    assert_eq!(status, 200);
    assert_eq!(
        next_page["workspaces"][0]["workspaceId"],
        "019f0000-0000-7000-8000-000000000101"
    );
    assert_eq!(next_page["next"], "019f0000-0000-7000-8000-000000000101");

    for (bookmark, read_limit) in [("", 101_u16), (after, 2)] {
        let (returned, rows_read): (usize, usize) = directory
            .control("raw-workspace-page-rows-read", &(bookmark, read_limit))
            .await
            .unwrap();
        println!("workspace list after={bookmark:?}, fetched={returned}, rows_read={rows_read}");
        assert_eq!(returned, usize::from(read_limit));
        assert!(
            rows_read <= usize::from(read_limit),
            "range query read {rows_read} rows with a fetch limit of {read_limit}"
        );
    }
    fixture.close().await;
}
