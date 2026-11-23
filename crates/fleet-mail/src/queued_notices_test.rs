#[test]
fn queued_notice_edit_is_checked_against_the_mail_schema() {
    macro_rules! checked_update {
        ($file:literal, $($argument:expr),+) => {
            sqlx::query_file!($file, $($argument),+)
        };
    }
    let _ = update_notice!(
        checked_update,
        "body",
        "state",
        "change",
        "workspace",
        None::<String>,
        "now",
        "id",
        "previous"
    );
}
