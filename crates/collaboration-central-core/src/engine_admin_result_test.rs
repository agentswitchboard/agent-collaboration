use super::EngineAdminResult;

#[test]
fn retired_page_requires_a_cursor_member_and_accepts_null() {
    let missing_cursor = r#"{"result":"engineRetired","items":[]}"#;
    let terminal_page = r#"{"result":"engineRetired","items":[],"next":null}"#;
    let continuing_page =
        r#"{"result":"engineRetired","items":[],"next":"019f0000-0000-7000-8000-000000000101"}"#;

    let missing_cursor_error = serde_json::from_str::<EngineAdminResult>(missing_cursor)
        .expect_err("a retired page must contain next");
    assert!(
        missing_cursor_error
            .to_string()
            .contains("missing field `next`")
    );
    for page in [terminal_page, continuing_page] {
        let decoded = serde_json::from_str::<EngineAdminResult>(page).unwrap();
        assert_eq!(serde_json::to_string(&decoded).unwrap(), page);
    }
}

#[test]
fn previously_retained_engine_replies_keep_their_bytes() {
    for retained in [
        r#"{"result":"engineRetried"}"#,
        r#"{"result":"engineDiscarded"}"#,
        r#"{"result":"refused","reason":"stale"}"#,
    ] {
        let decoded = serde_json::from_str::<EngineAdminResult>(retained).unwrap();
        assert_eq!(serde_json::to_string(&decoded).unwrap(), retained);
    }
}
