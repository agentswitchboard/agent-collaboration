use super::*;

#[test]
fn direct_when_idle_rejects_a_busy_current_owner_without_queueing_or_steering() {
    let mut state = fresh();
    let run = restore_live(&mut state, 1);
    let foreground = input(run.generation, 2);
    send_prompt(&mut state, &foreground);
    let prior = state.snapshot();

    let notice = input(run.generation, 3);
    assert!(matches!(
        decide(
            &state,
            OwnerEvent::Command(SessionOwnerCommand::Prompt {
                input: notice,
                admission: PromptAdmission::DirectWhenIdle
            })
        ),
        Err(SessionOwnerRejection::Refused(
            SessionOwnerRefusal::EffectInProgress(EffectInProgress::TurnInProgress)
        ))
    ));
    assert_eq!(state.snapshot().queue, prior.queue);
}

#[test]
fn direct_when_idle_rejects_loading_and_uses_the_prompt_take_of_an_exact_idle_run() {
    let mut state = fresh();
    let run = open_wire(&mut state, 1);
    let notice = input(run.generation, 2);
    assert!(matches!(
        decide(
            &state,
            OwnerEvent::Command(SessionOwnerCommand::Prompt {
                input: notice.clone(),
                admission: PromptAdmission::DirectWhenIdle
            })
        ),
        Err(SessionOwnerRejection::Refused(
            SessionOwnerRefusal::GenerationChanged
        ))
    ));
    apply(&mut state, restore_reply(run));
    finish_preparation(&mut state, run);
    let other_run = input(
        SessionGeneration::from_number(run.generation.number() + 1),
        3,
    );
    assert!(matches!(
        decide(
            &state,
            OwnerEvent::Command(SessionOwnerCommand::Prompt {
                input: other_run,
                admission: PromptAdmission::DirectWhenIdle
            })
        ),
        Err(SessionOwnerRejection::Refused(
            SessionOwnerRefusal::GenerationChanged
        ))
    ));

    let admitted = command(
        &mut state,
        SessionOwnerCommand::Prompt {
            input: notice.clone(),
            admission: PromptAdmission::DirectWhenIdle,
        },
    );
    assert!(
        admitted.contains(&OwnerEffect::Store(SessionStoreRequest::MarkDispatch(
            notice.clone()
        )))
    );
    assert!(!admitted.iter().any(|effect| matches!(
        effect,
        OwnerEffect::InputQueued(_) | OwnerEffect::PromptTaken(_)
    )));
    let offered = apply(&mut state, mark_prompt(&notice));
    assert!(
        !offered
            .iter()
            .any(|effect| matches!(effect, OwnerEffect::PromptTaken(_)))
    );
    let taken = apply(
        &mut state,
        OwnerEvent::Dispatch(DispatchResult::Taken(authorization(&offered))),
    );
    assert!(taken.contains(&OwnerEffect::PromptTaken(notice)));
}

#[test]
fn pr329_limit_a_notice_offered_but_not_taken_refuses_direct_until_settling_changes_it() {
    let mut state = fresh();
    let run = restore_live(&mut state, 1);
    let notice = input(run.generation, 2);
    command(
        &mut state,
        SessionOwnerCommand::Prompt {
            input: notice.clone(),
            admission: PromptAdmission::DirectWhenIdle,
        },
    );
    let offered = apply(&mut state, mark_prompt(&notice));
    let _offered = authorization(&offered);
    assert!(
        !offered
            .iter()
            .any(|effect| matches!(effect, OwnerEffect::PromptTaken(_)))
    );
    assert_eq!(state.snapshot().turn, SessionTurn::Idle);

    // PR 329 only fixes admission priority. Settling removes the refusal at
    // this exact mark/offer boundary when the human wins before provider take.
    let human = input(run.generation, 3);
    assert!(matches!(
        decide(
            &state,
            OwnerEvent::Command(SessionOwnerCommand::Prompt {
                input: human,
                admission: PromptAdmission::Direct,
            })
        ),
        Err(SessionOwnerRejection::Refused(
            SessionOwnerRefusal::EffectInProgress(_)
        ))
    ));
}
