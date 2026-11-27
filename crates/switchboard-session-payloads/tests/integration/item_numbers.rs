use std::{fmt::Debug, str::FromStr};

use serde::{Serialize, de::DeserializeOwned};
use switchboard_session_payloads::{
    DecimalCount, DecimalTextError, GroupId, Item, ItemId, ItemIdentifierError, ItemOrder,
    RawInput, ToolCall, TurnId, UsageAmount, UsageReading,
};

use super::{assert_refused, assert_wire};

#[test]
fn orders_usage_counts_and_byte_counts_keep_exact_decimal_text() {
    for (decimal, value, count_wire, byte_wire) in [
        (
            r#""0""#,
            0,
            r#"{"kind":"count","value":"0","capacity":"0"}"#,
            r#"{"kind":"tooLarge","bytes":"0"}"#,
        ),
        (
            r#""9007199254740993""#,
            9_007_199_254_740_993,
            r#"{"kind":"count","value":"9007199254740993","capacity":"9007199254740993"}"#,
            r#"{"kind":"tooLarge","bytes":"9007199254740993"}"#,
        ),
        (
            r#""18446744073709551615""#,
            u64::MAX,
            r#"{"kind":"count","value":"18446744073709551615","capacity":"18446744073709551615"}"#,
            r#"{"kind":"tooLarge","bytes":"18446744073709551615"}"#,
        ),
    ] {
        assert_wire(decimal, ItemOrder::from(value));
        assert_wire(
            count_wire,
            UsageAmount::Count {
                value: value.into(),
                capacity: Some(value.into()),
            },
        );
        assert_wire(
            byte_wire,
            RawInput::TooLarge {
                bytes: value.into(),
            },
        );
    }
    assert_wire(
        r#"{"kind":"count","value":"0"}"#,
        UsageAmount::Count {
            value: 0.into(),
            capacity: None,
        },
    );
}

#[test]
fn malformed_decimal_text_is_refused_for_orders_and_counts() {
    for (decimal, reason) in [
        ("", DecimalTextError::Empty),
        ("007", DecimalTextError::LeadingZero),
        ("+7", DecimalTextError::NonDecimal),
        ("-7", DecimalTextError::NonDecimal),
        ("1.5", DecimalTextError::NonDecimal),
        ("７", DecimalTextError::NonDecimal),
        ("18446744073709551616", DecimalTextError::OutOfRange),
    ] {
        assert_eq!(decimal.parse::<ItemOrder>(), Err(reason));
        assert_eq!(decimal.parse::<DecimalCount>(), Err(reason));
        let wire = serde_json::to_string(decimal).unwrap();
        assert_decimal_refused(&wire, &reason.to_string());
    }
}

#[test]
fn a_bare_json_number_is_refused_for_orders_and_counts() {
    assert_decimal_refused("7", "integer `7`, expected a string");
}

fn assert_decimal_refused(invalid: &str, reason: &str) {
    let item = r#"{"id":"fixture","order":"8","body":{"kind":"user","body":{"kind":"message","text":"ok"}}}"#;
    serde_json::from_str::<Item>(item).unwrap();
    assert_refused::<Item>(&item.replace(r#""8""#, invalid), "order", reason);

    let reading = r#"{"metric":{"kind":"inputTokens"},"scope":"turn","amount":{"kind":"count","value":"8","capacity":"9"}}"#;
    serde_json::from_str::<UsageReading>(reading).unwrap();
    // Serde buffers an internally tagged object before reading its fields.
    // The path ends at amount, and only one of the two valid fields is varied.
    for valid in [r#""8""#, r#""9""#] {
        assert_refused::<UsageReading>(&reading.replace(valid, invalid), "amount", reason);
    }

    let call = r#"{"kind":"other","action":{"kind":"other","input":{"kind":"tooLarge","bytes":"8"}},"state":{"kind":"pending"},"locations":[]}"#;
    serde_json::from_str::<ToolCall>(call).unwrap();
    assert_refused::<ToolCall>(&call.replace(r#""8""#, invalid), "action", reason);
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
