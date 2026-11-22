use super::*;

#[test]
fn direct_recipient_enrollment_query_matches_the_central_schema() {
    let machine: MachineId = "019f0000-0000-7000-8000-00000000cf02".parse().unwrap();
    macro_rules! checked_query {
        ($file:literal, $machine:expr) => {
            sqlx::query_file_as!(StoredEnrolledRecipient, $file, $machine)
        };
    }
    let _ = enrolled_direct_recipient_query!(checked_query, machine);
}
