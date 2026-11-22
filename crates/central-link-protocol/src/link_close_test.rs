use super::MachineLinkCloseCode;

#[test]
fn authority_codes_round_trip_and_other_closes_stay_unclassified() {
    for (cause, expected) in [
        (MachineLinkCloseCode::Revoked, 4101),
        (MachineLinkCloseCode::Expired, 4102),
        (MachineLinkCloseCode::Inactive, 4103),
    ] {
        assert_eq!(cause.code(), expected);
        assert_eq!(MachineLinkCloseCode::from_code(expected), Some(cause));
    }
    for code in [1000, 1008, 1011, 4100, 4104] {
        assert_eq!(MachineLinkCloseCode::from_code(code), None);
    }
}
