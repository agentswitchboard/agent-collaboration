use std::collections::VecDeque;

use hegel::generators as gs;
use state_machine_core::{EventTag, StateMachine, StateTag, Transition};
use switchboard_session_start::{
    CandidateAsker, CentralStanding, ConversationFact, Directed, EndLeg, EnvelopeRefusal,
    LaunchSpecRef, LocalStartDigest, Recording, SnapshotKind, StartDirective, StartEffect,
    StartEnvelope, StartInput, StartMachine, StartRevision, StartSnapshot, StartStateKind,
};

use super::{
    choices,
    executor::{Executor, Timing},
    fixtures,
};

// Each prefix leaves a different directive outstanding. Interrupted histories
// and terminal replays have their own deterministic witnesses.
const PREFIXES: &[&[usize]] = &[
    &[],
    &[0],
    &[0, 0],
    &[0, 0, 0],
    &[0, 0, 0, 0],
    &[0, 0, 0, 0, 0],
    &[0, 0, 0, 0, 0, 0],
    &[0, 0, 0, 0, 0, 0, 0],
    &[0, 0, 0, 0, 0, 0, 0, 0],
    &[0, 0, 0, 0, 0, 0, 0, 0, 0],
    &[0, 0, 0, 0, 0, 0, 3],
    &[0, 0, 0, 0, 0, 0, 3, 0],
    &[0, 0, 0, 0, 0, 0, 4],
    &[0, 0, 0, 0, 0, 0, 4, 0],
    &[0, 13],
    &[0, 13, 0],
    &[0, 0, 0, 0, 0, 0, 0, 0, 10],
    &[0, 0, 0, 0, 0, 0, 0, 0, 10, 0],
    &[1],
    &[2],
    &[0, 12, 0],
    &[0, 3, 0],
    &[0, 15, 0],
];

fn timing(number: u8) -> Timing {
    match number {
        0 => Timing::FenceBeforeTake,
        1 => Timing::FenceAfterTake,
        2 => Timing::RetireConnectionAfterTake,
        _ => Timing::BeforeFence,
    }
}

fn prefix(indices: &[usize]) -> Executor {
    let mut executor = Executor::new();
    executor.feed(fixtures::ask(
        1,
        Ok(fixtures::envelope()),
        CentralStanding::Ready,
    ));
    for index in indices {
        executor.take(*index, Timing::BeforeFence, false);
        executor.deliver();
    }
    executor
}

#[hegel::test(test_cases = 450)]
fn generated_histories_agree_on_every_effect_and_preserve_take_invariants(case: hegel::TestCase) {
    let start = case.draw(gs::integers::<usize>().max_value(PREFIXES.len() + 1));
    let mut executor = match PREFIXES.get(start) {
        Some(indices) => prefix(indices),
        None if start == PREFIXES.len() => Executor::new(),
        None => Executor::interrupted(),
    };
    let steps = case.draw(gs::integers::<u8>().min_value(6).max_value(56));
    for _ in 0..steps {
        let operation = case.draw(gs::integers::<u8>().max_value(9));
        if operation < 4 {
            if executor.result_is_waiting() {
                executor.deliver();
            } else if executor.pending.is_some() {
                let choice =
                    case.draw(gs::integers::<usize>().max_value(executor.selectable_results() - 1));
                let order = timing(case.draw(gs::integers::<u8>().max_value(16)));
                executor.take(choice, order, case.draw(gs::booleans()));
            }
        } else if operation < 6 {
            executor.wrong_id(case.draw(gs::booleans()));
        } else {
            let central = match case.draw(gs::integers::<u8>().max_value(5)) {
                0 => CentralStanding::NotEnrolled,
                1 => CentralStanding::LinkNotReady,
                _ => CentralStanding::Ready,
            };
            let kind = case.draw(gs::integers::<u8>().max_value(7));
            let candidate = varied_candidate(&executor.machine, kind);
            let slot = case.draw(gs::integers::<u64>().max_value(5));
            executor.feed(fixtures::ask(slot, candidate, central));
        }
        executor.assert_terminal_custody();
    }
}

#[test]
fn every_directive_result_and_fence_order_is_exercised_against_the_model() {
    for indices in PREFIXES {
        let initial = prefix(indices);
        if initial.pending.is_none() {
            continue;
        }
        for choice in 0..initial.selectable_results() {
            for order in [
                Timing::BeforeFence,
                Timing::FenceBeforeTake,
                Timing::FenceAfterTake,
                Timing::RetireConnectionAfterTake,
            ] {
                let mut executor = prefix(indices);
                executor.wrong_id(false);
                executor.wrong_id(true);
                executor.take(choice, order, true);
                for kind in 0..=7 {
                    let candidate = varied_candidate(&executor.machine, kind);
                    executor.feed(fixtures::ask(
                        u64::from(kind % 3),
                        candidate,
                        CentralStanding::NotEnrolled,
                    ));
                }
                executor.deliver();
                executor.assert_terminal_custody();
            }
        }
    }
}

#[hegel::test(test_cases = 120)]
fn session_asker_replies_obey_the_model_without_storing_a_secret(case: hegel::TestCase) {
    let mut executor = Executor::new();
    let mut candidate = fixtures::candidate();
    candidate.asker = CandidateAsker::Session {
        session_id: fixtures::central_id(),
    };
    executor.feed(fixtures::ask(
        1,
        StartEnvelope::new(candidate),
        CentralStanding::Ready,
    ));
    executor.take(0, Timing::BeforeFence, false);
    executor.deliver();
    let choice = case.draw(gs::integers::<usize>().max_value(executor.selectable_results() - 1));
    executor.take(choice, Timing::BeforeFence, false);
    executor.deliver();
    assert!(!matches!(
        executor.pending.as_ref().map(|item| &item.directive),
        Some(StartDirective::PutSecret { .. })
    ));
}

fn varied_candidate(machine: &StartMachine, kind: u8) -> Result<StartEnvelope, EnvelopeRefusal> {
    let mut candidate = fixtures::candidate();
    match kind {
        0 => return Err(EnvelopeRefusal::NoProvableAsker),
        1 => return Err(EnvelopeRefusal::UnsupportedAgentProgram),
        2 if machine.envelope().is_some() => {
            candidate.local_digest = LocalStartDigest::new([29; 32])
        }
        3 if machine.envelope().is_some() => candidate.machine = fixtures::machine_id(11),
        4 if machine.envelope().is_some() => {
            candidate.asker = CandidateAsker::Session {
                session_id: fixtures::central_id(),
            }
        }
        5 => candidate.launch = LaunchSpecRef::new(12),
        _ => {}
    }
    if matches!(kind, 5..=7)
        && let Some(held) = machine.envelope()
    {
        // The initial handle may itself have been varied by a generated Ask.
        // Copy only the fixed request contents, then use a fresh handle.
        candidate.scope = held.scope().clone();
        candidate.machine = held.machine().clone();
        candidate.local_digest = held.local_digest();
        candidate.agent_program = held.agent_program().clone();
        candidate.asker = match held.asker() {
            switchboard_session_start::StartAsker::Operator => CandidateAsker::Operator,
            switchboard_session_start::StartAsker::Session { session_id } => {
                CandidateAsker::Session {
                    session_id: session_id.clone(),
                }
            }
        };
    }
    StartEnvelope::new(candidate)
}

#[derive(PartialEq, Eq)]
struct EdgeClass {
    state: StartStateKind,
    first_attempt: bool,
    recorded: Option<Recording>,
    held: bool,
    exposure: Option<switchboard_session_start::Exposure>,
    end_due: bool,
    snapshot: Option<SnapshotKind>,
}

fn edge_class(machine: &StartMachine, directive: Option<&Directed>) -> EdgeClass {
    let facts = machine.facts();
    EdgeClass {
        state: machine.kind(),
        first_attempt: machine.attempt() == Some(fixtures::attempt(1)),
        recorded: machine.recording(),
        held: facts.is_some_and(|facts| matches!(facts.conversation, ConversationFact::Held(_))),
        exposure: facts.map(|facts| facts.bind_exposure),
        end_due: facts.is_some_and(|facts| facts.end == EndLeg::Due),
        snapshot: match directive.map(|directed| &directed.directive) {
            Some(StartDirective::Save { snapshot, .. }) => Some(snapshot.kind()),
            _ => None,
        },
    }
}

#[test]
fn finite_witnesses_reach_each_declared_edge() {
    let boot = StartMachine::new(fixtures::instance(501));
    let interrupted = StartMachine::restored(
        fixtures::instance(502),
        StartRevision::new(1),
        StartSnapshot::CreateIntended(fixtures::snapshot_facts(
            switchboard_session_start::StartFacts::default(),
        )),
    );
    let mut unexplored = VecDeque::from([(boot, None), (interrupted, None)]);
    let mut visited = Vec::new();
    let mut reached = Vec::new();
    while let Some((machine, pending)) = unexplored.pop_front() {
        let class = edge_class(&machine, pending.as_ref());
        if visited.contains(&class) {
            continue;
        }
        visited.push(class);
        let refused = fixtures::ask(
            2,
            Err(EnvelopeRefusal::NoProvableAsker),
            CentralStanding::NotEnrolled,
        );
        let fresh = fixtures::ask(1, Ok(fixtures::envelope()), CentralStanding::Ready);
        let (next, _) = witness(&machine, refused, &mut reached);
        assert_eq!(machine, next);
        if let Some(directed) = pending {
            for result in choices::results(
                &directed.directive,
                StartRevision::new(directed.id.sequence),
            ) {
                let input = StartInput::DirectiveResult {
                    id: directed.id.clone(),
                    result,
                };
                unexplored.push_back(witness(&machine, input, &mut reached));
            }
        } else {
            unexplored.push_back(witness(&machine, fresh, &mut reached));
        }
    }
    let ceiling = Transition {
        from: "CreateUnknown",
        event: "Ask",
        to: "SavingOutcome",
        terminal: false,
    };
    assert!(StartMachine::transitions().contains(&ceiling));
    // The sibling unit test seeds the valid protocol ceiling directly. The
    // public entry point would need 9,007,199,254,740,990 retries to reach it.
    for edge in StartMachine::transitions() {
        assert!(
            *edge == ceiling || reached.contains(edge),
            "no witness for {edge:?}"
        );
    }
}

fn witness(
    machine: &StartMachine,
    input: StartInput,
    reached: &mut Vec<Transition>,
) -> (StartMachine, Option<Directed>) {
    let event = input.event_tag();
    let (next, effects) = StartMachine::transition(machine, input).unwrap();
    let observed = Transition {
        from: machine.state_tag(),
        event,
        to: next.state_tag(),
        terminal: next.is_terminal(),
    };
    assert!(
        StartMachine::transitions().contains(&observed),
        "undeclared {observed:?}"
    );
    if !reached.contains(&observed) {
        reached.push(observed);
    }
    let mut outgoing = effects.into_iter().filter_map(|effect| match effect {
        StartEffect::Directive(directed) => Some(directed),
        StartEffect::Notification(_) => None,
    });
    let pending = outgoing.next();
    assert!(outgoing.next().is_none());
    (next, pending)
}
