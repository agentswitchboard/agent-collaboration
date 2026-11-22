use super::{POSITION, assert_wire_refused, position};
use switchboard_session_calls::{
    Continuation, DecimalPositionError, FrameOrdinal, FrameOrdinalError, FrameOrdinalExhausted,
    HistoryEpoch, IssuedPosition,
};

#[test]
fn continuation_uses_exact_position_text() {
    let value: Continuation = serde_json::from_str(POSITION).unwrap();
    assert_eq!(value, position());
    assert_eq!(serde_json::to_string(&value).unwrap(), POSITION);

    for (text, error) in [
        ("01", DecimalPositionError::LeadingZero),
        ("", DecimalPositionError::Empty),
        ("18446744073709551616", DecimalPositionError::OutOfRange),
        ("+1", DecimalPositionError::NonDecimal),
    ] {
        assert_eq!(
            text.parse::<HistoryEpoch>(),
            Err(error),
            "epoch must reject {error:?}"
        );
        assert_eq!(
            text.parse::<IssuedPosition>(),
            Err(error),
            "position must reject {error:?}"
        );
        let reason = error.to_string();
        assert_wire_refused::<Continuation>(
            &POSITION.replace(
                r#""historyEpoch":"0""#,
                &format!(r#""historyEpoch":"{text}""#),
            ),
            "historyEpoch",
            &reason,
        );
        assert_wire_refused::<Continuation>(
            &POSITION.replace(
                r#""after":"18446744073709551615""#,
                &format!(r#""after":"{text}""#),
            ),
            "after",
            &reason,
        );
    }

    assert_wire_refused::<Continuation>(
        &POSITION.replace(r#""historyEpoch":"0""#, r#""historyEpoch":0"#),
        "historyEpoch",
        "expected a string",
    );
    assert_wire_refused::<Continuation>(
        &POSITION.replace(r#""after":"18446744073709551615""#, r#""after":7"#),
        "after",
        "expected a string",
    );
    assert_wire_refused::<Continuation>(
        &POSITION.replace(r#""sessionId":"#, r#""endpoint":"legacy","sessionId":"#),
        "endpoint",
        "unknown field",
    );
    assert_wire_refused::<Continuation>(
        &POSITION.replace(r#""routerRunId":"#, r#""extra":null,"routerRunId":"#),
        "extra",
        "unknown field",
    );
    assert_wire_refused::<Continuation>(
        &POSITION.replace(r#""historyEpoch":"0","#, ""),
        "historyEpoch",
        "missing field",
    );
    assert_wire_refused::<Continuation>(
        &POSITION.replace(
            r#""historyEpoch":"#,
            r#""historyEpoch":"0","historyEpoch":"#,
        ),
        "historyEpoch",
        "duplicate field",
    );
}

#[test]
fn the_frame_ordinal_is_positive_and_cannot_wrap() {
    assert_eq!(
        serde_json::from_str::<FrameOrdinal>("1").unwrap(),
        FrameOrdinal::FIRST
    );
    assert_eq!(serde_json::to_string(&FrameOrdinal::FIRST).unwrap(), "1");
    let next = FrameOrdinal::FIRST.checked_next().unwrap();
    assert_eq!(u32::from(next), 2);

    let max = FrameOrdinal::try_from(u64::from(FrameOrdinal::MAX)).unwrap();
    assert_eq!(serde_json::to_string(&max).unwrap(), "4294967295");
    assert_eq!(
        serde_json::from_str::<FrameOrdinal>("4294967295").unwrap(),
        max
    );
    assert_eq!(max.checked_next(), Err(FrameOrdinalExhausted::AtMaximum));

    assert_eq!(
        FrameOrdinal::try_from(0),
        Err(FrameOrdinalError::Zero),
        "frame ordinal must reject zero"
    );
    assert_eq!(
        FrameOrdinal::try_from(u64::from(FrameOrdinal::MAX) + 1),
        Err(FrameOrdinalError::OutOfRange)
    );
    for (literal, error) in [
        ("0", FrameOrdinalError::Zero),
        ("4294967296", FrameOrdinalError::OutOfRange),
    ] {
        let failure = serde_json::from_str::<FrameOrdinal>(literal).unwrap_err();
        assert!(
            failure.to_string().contains(&error.to_string()),
            "{failure}"
        );
    }
}

#[test]
fn position_getters_preserve_the_full_u64_range() {
    for number in [0, 1, 4_294_967_296, u64::MAX] {
        assert_eq!(HistoryEpoch::from(number).get(), number, "epoch");
        assert_eq!(
            IssuedPosition::from(number).get(),
            number,
            "issued position"
        );
    }
}

#[hegel::test(test_cases = 300)]
fn decimal_wire_numbers_have_one_canonical_spelling(case: hegel::TestCase) {
    use hegel::generators as gs;
    let number = case.draw(gs::integers::<u64>());
    let canonical = number.to_string();
    let wire = format!(r#""{canonical}""#);
    let epoch = HistoryEpoch::from(number);
    let position = IssuedPosition::from(number);
    assert_eq!(serde_json::to_string(&epoch).unwrap(), wire);
    assert_eq!(serde_json::to_string(&position).unwrap(), wire);
    assert_eq!(serde_json::from_str::<HistoryEpoch>(&wire).unwrap(), epoch);
    assert_eq!(
        serde_json::from_str::<IssuedPosition>(&wire).unwrap(),
        position
    );

    let alias = format!("0{number}");
    assert_eq!(
        alias.parse::<HistoryEpoch>(),
        Err(DecimalPositionError::LeadingZero),
        "only canonical decimal text may decode"
    );
    assert_eq!(
        alias.parse::<IssuedPosition>(),
        Err(DecimalPositionError::LeadingZero),
        "only canonical decimal text may decode"
    );

    let alphabet = [
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', ' ', '+', '-', 'é', '١',
    ];
    let length = case.draw(gs::integers::<u8>().max_value(24));
    let letters = gs::sampled_from(&alphabet);
    let arbitrary: String = (0..length).map(|_| case.draw(&letters)).collect();
    if let Ok(value) = arbitrary.parse::<HistoryEpoch>() {
        assert_eq!(
            String::from(value),
            arbitrary,
            "accepted epoch text must already be canonical"
        );
    }
    if let Ok(value) = arbitrary.parse::<IssuedPosition>() {
        assert_eq!(
            String::from(value),
            arbitrary,
            "accepted position text must already be canonical"
        );
    }
}
