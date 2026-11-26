//! Cross creation worker facts with each distinct receipt owner.

use super::*;

#[derive(Clone, Copy)]
enum Outcome {
    Refuse,
    HeldCreate,
    Configured,
    Remove,
}

// Rows: awaiting create, configuring, stopped create, stopped configure,
// stopped restore and stopped close. Columns: created, open failed, worker
// exited, configured and configure closed.
const OUTCOMES: [[Outcome; 5]; 6] = {
    use Outcome::{Configured, HeldCreate, Refuse, Remove};
    [
        [HeldCreate, Remove, Remove, Refuse, Refuse],
        [Refuse, Refuse, Remove, Configured, Remove],
        [Remove, Remove, Remove, Refuse, Refuse],
        [Refuse, Refuse, Remove, Remove, Remove],
        [Refuse; 5],
        [Refuse; 5],
    ]
};

#[tokio::test]
async fn create_and_configure_worker_facts_settle_only_the_receipt_they_own() {
    let fixture = Fixture::new().await;
    let mut visited = [[false; 5]; 6];
    for (owner, expected_facts) in OUTCOMES.into_iter().enumerate() {
        for (fact, expected) in expected_facts.into_iter().enumerate() {
            let stopped = fixture.stopped.clone();
            let state = match owner {
                0 => OpeningState::AwaitingCreate(PendingCreate {
                    requested: Box::default(),
                    stopped,
                }),
                1 => OpeningState::ConfiguringCreate(stopped),
                2 => OpeningState::StoppedAwaitingReply(StoppedReceipt::OpenCreate(stopped)),
                3 => OpeningState::StoppedAwaitingReply(StoppedReceipt::Configure(stopped)),
                4 => OpeningState::StoppedAwaitingReply(StoppedReceipt::OpenRestore(
                    fixture.run.clone(),
                    RestoreReceiptStage::AwaitingPeer,
                )),
                _ => OpeningState::StoppedAwaitingReply(StoppedReceipt::Close(
                    fixture.run.clone(),
                    fixture.close.attempt,
                )),
            };
            let command = match fact {
                0 => Command::Created(fixture.session()),
                1 => Command::OpenFailed,
                2 => Command::WorkerExited,
                3 => Command::Configured(fixture.session()),
                _ => Command::ConfigureClosed,
            };
            let phase = state.phase();
            let attempt = if owner < 4 {
                fixture.create
            } else {
                fixture.attempt
            };
            let instance = OpeningInstance::default();
            let row = super::super::super::OpeningRow {
                attempt,
                instance: instance.clone(),
                state,
            };
            let result = {
                let _table = lock(&fixture.table);
                row.apply(&instance, command)
            };
            match (expected, result) {
                (Outcome::Refuse, Err(refused)) => {
                    assert_eq!(refused.rejection.reason, expectations::refusal(phase));
                    assert_eq!(refused.row.attempt(), attempt);
                    assert!(refused.row.instance().is_same_as(&instance));
                    assert_eq!(refused.row.phase(), phase);
                }
                (Outcome::HeldCreate, Ok((Some(row), Effect::None))) => {
                    assert_eq!(row.phase(), P::HeldCreate);
                    assert_eq!(row.attempt(), attempt);
                    assert!(row.instance().is_same_as(&instance));
                }
                (Outcome::Configured, Ok((Some(row), Effect::None))) => {
                    assert_eq!(row.phase(), P::ConfiguredCreate);
                    assert_eq!(row.attempt(), attempt);
                    assert!(row.instance().is_same_as(&instance));
                }
                (Outcome::Remove, Ok((None, effect))) => {
                    assert!(actual_actions(&effect, &fixture, &instance).contains(&Action::Remove));
                }
                _ => panic!(
                    "worker fact {fact} must settle only its own stopped receipt class {owner}"
                ),
            }
            fixture.assert_disposals();
            visited[owner][fact] = true;
        }
    }
    assert_eq!(visited, [[true; 5]; 6]);
}
