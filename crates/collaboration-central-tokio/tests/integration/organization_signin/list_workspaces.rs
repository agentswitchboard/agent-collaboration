use super::support::*;
use serde_json::{Value, json};

const FIRST: &str = "019f0000-0000-7000-8000-00000000d110";
const ARCHIVED: &str = "019f0000-0000-7000-8000-00000000d120";
const LAST: &str = "019f0000-0000-7000-8000-00000000d130";
const INSERTED_BEHIND: &str = "019f0000-0000-7000-8000-00000000d105";
const INSERTED_AHEAD: &str = "019f0000-0000-7000-8000-00000000d125";

async fn insert_workspace(fixture: &Fixture, id: &str, name: &str, state: &str) -> Result {
    fixture.apply(
        A,
        "INSERT INTO directory_workspaces VALUES (?,?,'description',?,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')",
        vec![text(id), text(name), text(state)],
    ).await
}

fn workspace_row(id: &str, name: &str, state: &str) -> Value {
    json!({"workspaceId":id,"name":name,"description":"description","state":state})
}

async fn list_page(
    fixture: &Fixture,
    person: &signed::Person,
    after: Option<&str>,
    limit: Option<u32>,
) -> Result<Reply> {
    let organization = &person.organization;
    let body = signed::list_workspaces_body(organization, after, limit);
    fixture
        .person(
            signed::LIST_WORKSPACES,
            &body,
            person,
            &signed::list_workspaces_intent(organization, after, limit.unwrap_or(100)),
            &signed::fresh_request_id(signed::NOW_SECONDS)?,
        )
        .await
}

#[tokio::test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "Signed HTTP responses are exact fixture oracles"
)]
async fn list_workspaces_walks_by_id_without_duplicates_and_preserves_the_lookup_row() -> Result {
    let fixture = Fixture::new().await?;
    fixture.ready().await?;
    let mut reader = signed::Person::member(A, signed::NOW_SECONDS)?;
    reader.second_factor_at = None;

    let empty = list_page(&fixture, &reader, None, None).await?;
    assert_eq!(empty.status, 200);
    assert_eq!(
        empty.value()?,
        json!({"result":"workspaces","workspaces":[],"next":null})
    );

    insert_workspace(&fixture, FIRST, "First", "active").await?;
    let only = list_page(&fixture, &reader, None, Some(1)).await?;
    assert_eq!(only.status, 200);
    assert_eq!(
        only.value()?,
        json!({"result":"workspaces","workspaces":[workspace_row(FIRST,"First","active")],"next":null})
    );

    insert_workspace(&fixture, LAST, "Last", "active").await?;
    insert_workspace(&fixture, ARCHIVED, "Archived", "archived").await?;
    let first_page = list_page(&fixture, &reader, None, Some(1)).await?;
    assert_eq!(first_page.status, 200);
    let first_page = first_page.value()?;
    assert_eq!(
        first_page,
        json!({"result":"workspaces","workspaces":[workspace_row(FIRST,"First","active")],"next":FIRST})
    );
    let cursor = first_page["next"]
        .as_str()
        .ok_or("workspace continuation missing")?;

    insert_workspace(&fixture, INSERTED_AHEAD, "Ahead", "active").await?;
    insert_workspace(&fixture, INSERTED_BEHIND, "Behind", "active").await?;
    let last_page = list_page(&fixture, &reader, Some(cursor), Some(3)).await?;
    assert_eq!(last_page.status, 200);
    let last_page = last_page.value()?;
    assert_eq!(
        last_page,
        json!({"result":"workspaces","workspaces":[workspace_row(ARCHIVED,"Archived","archived"),workspace_row(INSERTED_AHEAD,"Ahead","active"),workspace_row(LAST,"Last","active")],"next":null})
    );
    let lookup = fixture
        .person(
            signed::LOOKUP,
            &json!({"organizationId":A,"workspaceId":ARCHIVED}),
            &reader,
            &signed::lookup_intent(A, ARCHIVED),
            &signed::fresh_request_id(signed::NOW_SECONDS)?,
        )
        .await?;
    assert_eq!(lookup.status, 200);
    assert_eq!(
        lookup.value()?,
        json!({"result":"found","workspace":last_page["workspaces"][0]})
    );
    fixture.close().await
}

#[tokio::test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "Signed HTTP refusals and store-corruption status are exact oracles"
)]
async fn list_workspaces_refuses_wrong_permission_invalid_pages_and_corrupt_state_and_scopes_to_org()
-> Result {
    let fixture = Fixture::new().await?;
    fixture.ready().await?;
    insert_workspace(&fixture, FIRST, "Private A", "active").await?;
    let reader = signed::Person::owner(A, signed::NOW_SECONDS)?;
    let mut denied = signed::Person::owner(A, signed::NOW_SECONDS)?;
    denied
        .permissions
        .retain(|permission| permission != "organization:view");
    let refused = list_page(&fixture, &denied, None, None).await?;
    assert_eq!(refused.status, 200);
    assert_eq!(
        refused.value()?,
        json!({"result":"refused","reason":"notPermitted"})
    );

    for limit in [0, 101] {
        let refused = list_page(&fixture, &reader, None, Some(limit)).await?;
        assert_eq!(refused.status, 401);
        assert!(refused.body.is_empty());
    }
    assert_eq!(
        fixture
            .provision(B, &signed::stamp(B, signed::NOW_SECONDS)?)
            .await?
            .status,
        200
    );
    for floor in [
        organization_floor(B, 3, "active"),
        person_floor(B, signed::OWNER, signed::OWNER_SUBJECT, 4, "active"),
    ] {
        assert_eq!(fixture.post(REVISIONS, &floor).await?.status, 200);
    }
    let other_reader = signed::Person::owner(B, signed::NOW_SECONDS)?;
    let other = list_page(&fixture, &other_reader, None, Some(100)).await?;
    assert_eq!(other.status, 200);
    assert_eq!(
        other.value()?,
        json!({"result":"workspaces","workspaces":[],"next":null})
    );

    fixture
        .apply(
            A,
            "UPDATE directory_workspaces SET state='unrecognized' WHERE workspace_id=?",
            vec![text(FIRST)],
        )
        .await?;
    let corrupt = list_page(&fixture, &reader, None, None).await?;
    assert_eq!(corrupt.status, 503);
    assert!(corrupt.body.is_empty());
    fixture.close().await
}
