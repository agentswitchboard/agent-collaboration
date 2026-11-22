use super::*;

#[test]
fn the_applying_mark_reads_the_same_indexed_subject_due_projection() {
    macro_rules! checked {
        ($file:literal, $argument:expr) => {
            sqlx::query_file_as!(StoredMarkGuard, $file, $argument)
        };
    }
    let _ = mark_guard!(checked, "019f0000-0000-7000-8000-000000000006");
}
