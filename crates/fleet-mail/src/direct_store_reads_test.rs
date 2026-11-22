use super::*;

#[test]
fn direct_reads_are_checked_on_their_owning_schema() {
    let lookup = DirectOfferLookup {
        message_id: "019f0000-0000-7000-8000-000000000001".parse().unwrap(),
        recipient: "019f0000-0000-7000-8000-000000000002".parse().unwrap(),
        now: "2030-01-01T00:00:00Z".parse().unwrap(),
    };
    macro_rules! state {
        ($file:literal, $($arg:expr),+ $(,)?) => {
            sqlx::query_file_as!(StoredDirectState, $file, $($arg),+)
        };
    }
    macro_rules! offer {
        ($file:literal, $($arg:expr),+ $(,)?) => {
            sqlx::query_file_as!(StoredOffer, $file, $($arg),+)
        };
    }
    let page = OfferPageLookup {
        recipient: lookup.recipient.clone(),
        after: None,
        now: lookup.now,
    };
    let _ = offer_page_query!(offer, page, 250_i64);
    let _ = offer_query!(offer, lookup);
    let _ = state_query!(state, lookup.message_id);
    let _ = status_query!(state, lookup.message_id, "sender".to_owned());
}
