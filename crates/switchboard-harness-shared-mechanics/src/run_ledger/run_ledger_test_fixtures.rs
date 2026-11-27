use super::*;

pub(super) fn test_run(owner: u8, generation: u64) -> TransportRun {
    TransportRun {
        incarnation: OwnerIncarnation(
            format!("019ac19f-aa00-7000-8000-00000000{owner:04x}")
                .parse()
                .unwrap(),
        ),
        generation: SessionGeneration::from_number(generation),
    }
}

pub(super) fn test_turn(number: u8) -> TurnId {
    TurnId(
        format!("019ac19f-aa00-7000-8000-00000000{number:04x}")
            .parse()
            .unwrap(),
    )
}

pub(super) fn close_attempt(number: u8) -> CloseAttempt {
    CloseAttempt(test_turn(number).0)
}

pub(super) fn test_input(number: u8) -> InputId {
    InputId::new(format!("input-{number}")).unwrap()
}

pub(super) fn authorization(
    run: TransportRun,
    input: u8,
    sequence: u64,
    target: AuthorizationTarget,
) -> DispatchAuthorization {
    DispatchAuthorization {
        incarnation: run.incarnation,
        run: run.generation,
        input_id: test_input(input),
        authorization_seq: sequence,
        target,
    }
}
