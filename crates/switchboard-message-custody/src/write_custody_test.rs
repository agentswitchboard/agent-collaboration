use super::{WriteCustodyEvent, WriteCustodyMachine, WriteCustodyRefusal, WriteCustodyState};
use central_link_protocol::{ReplyPayload, RequestRefusal};
use hegel::{TestCase, generators};
use state_machine_core::{StateMachine, TransitionError, check_state_machine_transitions};
use workspace_model::{
    RefusalReason, WorkspaceReply, WorkspaceResult,
    central_mail::{CentralMailAnswer, CentralMailResult},
};

type StepResult = Result<WriteCustodyState, TransitionError<WriteCustodyRefusal>>;

fn write_events() -> Vec<WriteCustodyEvent> {
    vec![
        WriteCustodyEvent::ClaimedBeforeDeadline,
        WriteCustodyEvent::FrameNotReached,
        WriteCustodyEvent::FrameAnswerLost,
        WriteCustodyEvent::CentralAccepted(Box::new(ReplyPayload::Workspace(WorkspaceReply {
            result: WorkspaceResult::Conflict,
            actors: Vec::new(),
        }))),
        WriteCustodyEvent::CentralRefused(RequestRefusal::Workspace(
            RefusalReason::WorkspaceNotAllowed,
        )),
        WriteCustodyEvent::DeadlineReached,
        WriteCustodyEvent::DeliveryConfirmed {
            message_id: "019f0000-0000-7000-8000-000000000091".parse().unwrap(),
        },
    ]
}

/// Tracks what the sender can know from the history. A lost answer is a fact
/// about all earlier attempts, independent of what happened to the latest one.
#[derive(Default)]
struct WriteHistory {
    claim_open: bool,
    earlier_answer_lost: bool,
    resolution: Option<WriteCustodyState>,
}

impl WriteHistory {
    fn observe(&mut self, event: &WriteCustodyEvent) -> StepResult {
        use WriteCustodyEvent::{
            CentralAccepted, CentralRefused, ClaimedBeforeDeadline, DeadlineReached,
            DeliveryConfirmed, FrameAnswerLost, FrameNotReached,
        };
        use WriteCustodyState::{Accepted, NotConfirmed, NotSent, Refused};

        let possible_departure = self.claim_open || self.earlier_answer_lost;
        if let DeliveryConfirmed { message_id } = event
            && (self.resolution == Some(NotConfirmed)
                || (self.resolution.is_none() && possible_departure))
        {
            let acceptance =
                ReplyPayload::Mail(CentralMailResult::Done(CentralMailAnswer::Queued {
                    message_id: message_id.clone(),
                }));
            self.resolution = Some(Accepted(Box::new(acceptance)));
            return Ok(self.state());
        }
        if let Some(resolution) = &self.resolution {
            let answer = match event {
                CentralAccepted(reply) => Some(Accepted(reply.clone())),
                CentralRefused(reason) => Some(Refused(reason.clone())),
                ClaimedBeforeDeadline
                | FrameNotReached
                | FrameAnswerLost
                | DeadlineReached
                | DeliveryConfirmed { .. } => None,
            };
            let contradictory_final = matches!(resolution, Accepted(_) | Refused(_))
                && answer.as_ref().is_some_and(|answer| answer != resolution);
            return Err(if contradictory_final {
                TransitionError::Domain(WriteCustodyRefusal::ConflictingFinalOutcome)
            } else {
                TransitionError::Stale
            });
        }
        match event {
            ClaimedBeforeDeadline if !self.claim_open => self.claim_open = true,
            FrameNotReached if self.claim_open => self.claim_open = false,
            FrameAnswerLost if self.claim_open => {
                self.claim_open = false;
                self.earlier_answer_lost = true;
            }
            CentralAccepted(reply) if self.claim_open => {
                self.resolution = Some(Accepted(reply.clone()));
            }
            CentralRefused(reason) if self.claim_open => {
                self.resolution = Some(Refused(reason.clone()));
            }
            DeadlineReached => {
                let ending = if possible_departure {
                    NotConfirmed
                } else {
                    NotSent
                };
                self.resolution = Some(ending);
            }
            ClaimedBeforeDeadline
            | FrameNotReached
            | FrameAnswerLost
            | CentralAccepted(_)
            | CentralRefused(_)
            | DeliveryConfirmed { .. } => {
                let (state, event) = self.invalid_step_tags(event);
                return Err(TransitionError::NotAllowed { state, event });
            }
        }
        Ok(self.state())
    }

    fn state(&self) -> WriteCustodyState {
        use WriteCustodyState::{FirstClaimed, InOutbox, ReplayClaimed, ReplayWaiting};
        self.resolution
            .clone()
            .unwrap_or(match (self.claim_open, self.earlier_answer_lost) {
                (false, false) => InOutbox,
                (true, false) => FirstClaimed,
                (false, true) => ReplayWaiting,
                (true, true) => ReplayClaimed,
            })
    }

    fn invalid_step_tags(&self, event: &WriteCustodyEvent) -> (&'static str, &'static str) {
        use state_machine_core::{EventTag, StateTag};
        (self.state().state_tag(), event.event_tag())
    }
}

fn compare_steps(events: impl IntoIterator<Item = WriteCustodyEvent>) {
    let mut oracle = WriteHistory::default();
    let mut actual = WriteCustodyState::InOutbox;
    for event in events {
        let expected = oracle.observe(&event);
        let step = WriteCustodyMachine::transition(&actual, event.clone()).map(|(state, _)| state);
        assert_eq!(step, expected, "{actual:?} + {event:?}");
        if let Ok(next) = step {
            actual = next;
        }
    }
}

#[hegel::test(test_cases = 128)]
fn generated_write_orders_preserve_earlier_uncertainty_and_only_accept_central_evidence(
    case: TestCase,
) {
    use WriteCustodyEvent::{
        CentralAccepted, CentralRefused, ClaimedBeforeDeadline, DeadlineReached, FrameAnswerLost,
        FrameNotReached,
    };
    compare_steps([
        ClaimedBeforeDeadline,
        FrameAnswerLost,
        ClaimedBeforeDeadline,
        FrameNotReached,
        DeadlineReached,
    ]);
    let acceptance = ReplyPayload::Workspace(WorkspaceReply {
        result: WorkspaceResult::Conflict,
        actors: Vec::new(),
    });
    compare_steps([
        ClaimedBeforeDeadline,
        CentralAccepted(Box::new(acceptance.clone())),
        CentralRefused(RequestRefusal::Workspace(
            RefusalReason::WorkspaceNotAllowed,
        )),
        CentralAccepted(Box::new(acceptance)),
    ]);

    let count = case.draw(generators::integers::<u8>().min_value(1).max_value(36));
    let events = write_events();
    compare_steps((0..count).map(|_| case.draw(generators::sampled_from(events.as_slice()))));
}

#[test]
fn the_write_reducer_and_declared_transition_table_agree() {
    use WriteCustodyState::{
        FirstClaimed, InOutbox, NotConfirmed, NotSent, ReplayClaimed, ReplayWaiting,
    };
    let cases = [
        InOutbox,
        FirstClaimed,
        ReplayWaiting,
        ReplayClaimed,
        NotSent,
        NotConfirmed,
    ];
    let events = write_events();
    let result = check_state_machine_transitions::<WriteCustodyMachine>(
        cases.into_iter().flat_map(|state| {
            events
                .iter()
                .cloned()
                .map(move |event| (state.clone(), event))
        }),
    );
    assert_eq!(result, Ok(()));
}
