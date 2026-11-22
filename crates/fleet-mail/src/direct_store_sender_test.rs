use super::*;

#[test]
fn the_sender_and_label_reads_match_the_central_schema() {
    macro_rules! actor {
        ($file:literal, $argument:expr) => {
            sqlx::query_file_as!(StoredSender, $file, $argument)
        };
    }
    macro_rules! label {
        ($file:literal, $argument:expr) => {
            sqlx::query_file_as!(StoredLabel, $file, $argument)
        };
    }
    let _ = actor_query!(actor, "actor");
    let _ = label_query!(label, "machine");
}
