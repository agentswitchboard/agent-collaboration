use super::OrganizationAdminResult;

#[test]
fn retired_page_requires_a_cursor_member_and_accepts_null() {
    let missing_cursor = r#"{"result":"engineRetired","items":[]}"#;
    let terminal_page = r#"{"result":"engineRetired","items":[],"next":null}"#;
    let continuing_page =
        r#"{"result":"engineRetired","items":[],"next":"019f0000-0000-7000-8000-000000000101"}"#;

    let missing_cursor_error = serde_json::from_str::<OrganizationAdminResult>(missing_cursor)
        .expect_err("a retired page must contain next");
    assert!(
        missing_cursor_error
            .to_string()
            .contains("missing field `next`")
    );
    for page in [terminal_page, continuing_page] {
        let decoded = serde_json::from_str::<OrganizationAdminResult>(page).unwrap();
        assert_eq!(serde_json::to_string(&decoded).unwrap(), page);
    }
}
