use switchboard_session_payloads::{
    MoneyAmount, MoneyAmountError, MoneyCurrency, MoneyCurrencyError, UsageAmount, UsageReading,
};

use super::{assert_refused, assert_wire};

const VALID_MONEY: &str = r#"{"metric":{"kind":"cost"},"scope":{"kind":"session"},"amount":{"kind":"money","amount":"0.0400","currency":"USD"}}"#;

#[test]
fn money_round_trips_amount_and_currency_at_their_byte_limits() {
    let amount: MoneyAmount = "123456789012345678901234567890123456.000".parse().unwrap();
    let currency: MoneyCurrency = "éééééééé".parse().unwrap();
    assert_eq!(amount.as_str().len(), 40);
    assert_eq!(currency.as_str().len(), 16);
    assert_wire(
        r#"{"kind":"money","amount":"123456789012345678901234567890123456.000","currency":"éééééééé"}"#,
        UsageAmount::Money { amount, currency },
    );
    assert_wire(r#""0""#, "0".parse::<MoneyAmount>().unwrap());
}

#[test]
fn money_amount_rejects_invalid_decimal_text_with_typed_reasons() {
    serde_json::from_str::<UsageReading>(VALID_MONEY).unwrap();
    for (amount, reason) in [
        ("not-money", MoneyAmountError::InvalidDecimal),
        ("", MoneyAmountError::Empty),
        ("-1.00", MoneyAmountError::InvalidDecimal),
        ("+1.00", MoneyAmountError::InvalidDecimal),
        ("1e3", MoneyAmountError::InvalidDecimal),
        ("01.5", MoneyAmountError::LeadingZero),
        ("1.", MoneyAmountError::InvalidDecimal),
        (".5", MoneyAmountError::InvalidDecimal),
        (" 1", MoneyAmountError::InvalidDecimal),
        ("1 ", MoneyAmountError::InvalidDecimal),
        ("1.1.1", MoneyAmountError::InvalidDecimal),
        ("１", MoneyAmountError::InvalidDecimal),
        (
            "123456789012345678901234567890123456.0000",
            MoneyAmountError::TooLong,
        ),
    ] {
        assert_eq!(amount.parse::<MoneyAmount>(), Err(reason));
        let wire = serde_json::to_string(amount).unwrap();
        assert_refused::<UsageReading>(
            &VALID_MONEY.replace(r#""0.0400""#, &wire),
            "amount",
            &reason.to_string(),
        );
    }
}

#[test]
fn money_amount_refuses_a_bare_json_number() {
    serde_json::from_str::<UsageReading>(VALID_MONEY).unwrap();
    assert_refused::<UsageReading>(
        &VALID_MONEY.replace(r#""0.0400""#, "7"),
        "amount",
        "integer `7`, expected a string",
    );
}

#[test]
fn money_currency_rejects_empty_long_and_control_text_with_typed_reasons() {
    serde_json::from_str::<UsageReading>(VALID_MONEY).unwrap();
    for (currency, reason) in [
        ("", MoneyCurrencyError::Empty),
        ("USD00000000000000", MoneyCurrencyError::TooLong),
        ("ééééééééé", MoneyCurrencyError::TooLong),
        ("US\u{009f}D", MoneyCurrencyError::ControlCharacter),
    ] {
        assert_eq!(currency.parse::<MoneyCurrency>(), Err(reason));
        let wire = serde_json::to_string(currency).unwrap();
        assert_refused::<UsageReading>(
            &VALID_MONEY.replace(r#""USD""#, &wire),
            "amount",
            &reason.to_string(),
        );
    }
}
