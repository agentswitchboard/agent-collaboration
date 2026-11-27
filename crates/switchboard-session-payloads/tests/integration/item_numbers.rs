use std::{fmt::Debug, str::FromStr};

use serde::{Serialize, de::DeserializeOwned};
use switchboard_session_payloads::{
    GroupId, Item, ItemId, ItemIdentifierError, ItemOrder, ItemOrderError, TurnId, UsageAmount,
    UsageReading,
};

use super::{assert_refused, assert_wire};

#[test]
fn orders_and_both_count_fields_keep_exact_decimal_text() {
    for (decimal, value, count_wire) in [
        (
            r#""0""#,
            0,
            r#"{"kind":"count","value":"0","capacity":"0"}"#,
        ),
        (
            r#""9007199254740993""#,
            9_007_199_254_740_993,
            r#"{"kind":"count","value":"9007199254740993","capacity":"9007199254740993"}"#,
        ),
        (
            r#""18446744073709551615""#,
            u64::MAX,
            r#"{"kind":"count","value":"18446744073709551615","capacity":"18446744073709551615"}"#,
        ),
    ] {
        assert_wire(decimal, ItemOrder::from(value));
        assert_wire(
            count_wire,
            UsageAmount::Count {
                value,
                capacity: Some(value),
            },
        );
    }
    assert_wire(
        r#"{"kind":"count","value":"0"}"#,
        UsageAmount::Count {
            value: 0,
            capacity: None,
        },
    );
}

#[test]
fn malformed_decimal_text_is_refused_for_order_and_both_usage_counts() {
    for (decimal, reason) in [
        ("", ItemOrderError::Empty),
        ("007", ItemOrderError::LeadingZero),
        ("+7", ItemOrderError::NonDecimal),
        ("-7", ItemOrderError::NonDecimal),
        ("1.5", ItemOrderError::NonDecimal),
        ("７", ItemOrderError::NonDecimal),
        ("18446744073709551616", ItemOrderError::OutOfRange),
    ] {
        assert_eq!(decimal.parse::<ItemOrder>(), Err(reason));
        let wire = serde_json::to_string(decimal).unwrap();
        assert_decimal_refused(&wire, &reason.to_string());
    }
}

#[test]
fn a_bare_json_number_is_refused_for_order_and_both_usage_counts() {
    assert_decimal_refused("7", "integer `7`, expected a string");
}

fn assert_decimal_refused(invalid: &str, reason: &str) {
    let item = r#"{"id":"fixture","order":"8","body":{"kind":"user","body":{"kind":"message","text":"ok"}}}"#;
    serde_json::from_str::<Item>(item).unwrap();
    assert_refused::<Item>(&item.replace(r#""8""#, invalid), "order", reason);

    let reading = r#"{"metric":{"kind":"inputTokens"},"scope":{"kind":"turn"},"amount":{"kind":"count","value":"8","capacity":"9"}}"#;
    serde_json::from_str::<UsageReading>(reading).unwrap();
    // Serde buffers an internally tagged object before reading its fields.
    // The path ends at amount, and only one of the two valid fields is varied.
    for valid in [r#""8""#, r#""9""#] {
        assert_refused::<UsageReading>(&reading.replace(valid, invalid), "amount", reason);
    }
}

#[test]
fn every_identifier_enforces_the_same_byte_and_control_limits() {
    assert_identifier_limits::<ItemId>();
    assert_identifier_limits::<TurnId>();
    assert_identifier_limits::<GroupId>();
}

fn assert_identifier_limits<T>()
where
    T: FromStr<Err = ItemIdentifierError> + DeserializeOwned + Serialize + PartialEq + Debug,
{
    let exact_limit = "é".repeat(128);
    let exact_wire = serde_json::to_string(&exact_limit).unwrap();
    assert_wire(&exact_wire, exact_limit.parse::<T>().unwrap());

    for (value, reason) in [
        (String::new(), ItemIdentifierError::Empty),
        ("a".repeat(257), ItemIdentifierError::TooLong),
        ("é".repeat(129), ItemIdentifierError::TooLong),
        ("a\u{009f}b".into(), ItemIdentifierError::ControlCharacter),
    ] {
        assert_eq!(value.parse::<T>(), Err(reason));
        let wire = serde_json::to_string(&value).unwrap();
        assert_refused::<T>(&wire, ".", &reason.to_string());
    }
}
