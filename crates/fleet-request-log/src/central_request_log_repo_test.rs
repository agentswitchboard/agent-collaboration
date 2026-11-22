use super::*;

#[test]
fn the_retention_queries_match_the_central_ledger() {
    let request_id = "019f0000-0000-7000-8000-00000000cf02".parse().unwrap();
    let caller = "019f0000-0000-7000-8000-00000000cf03".parse().unwrap();
    let now = "2030-01-01T00:00:00Z".parse::<Timestamp>().unwrap();
    let entry = RetainRequest {
        request_id: &request_id,
        caller: &caller,
        digest: "digest",
        answer: "{}".into(),
        at: now,
    };
    macro_rules! read {
        ($file:literal, $($arg:expr),+) => {
            sqlx::query_file_as!(StoredRequestRow, $file, $($arg),+)
        };
    }
    macro_rules! write {
        ($file:literal, $($arg:expr),+ $(,)?) => {
            sqlx::query_file!($file, $($arg),+)
        };
    }
    let _ = read_binds!(read, &request_id, now.to_rfc3339());
    let _ = retain_binds!(
        write,
        entry.request_id,
        entry.caller,
        entry.digest,
        entry.answer,
        entry.at.to_rfc3339()
    );
    let _ = prune_binds!(write, now.to_rfc3339());
}
