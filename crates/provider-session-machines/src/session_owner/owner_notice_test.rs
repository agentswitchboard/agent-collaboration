use super::*;

#[test]
fn notice_admission_rejects_a_busy_current_owner_without_queueing_or_steering() {
    let mut state = fresh();
    let run = restore_live(&mut state, 1);
    let foreground = input(run.generation, 2);
    send_prompt(&mut state, &foreground);
    let prior = state.snapshot();

    let notice = input(run.generation, 3);
    assert!(matches!(
        decide(
            &state,
            OwnerEvent::Command(SessionOwnerCommand::NoticePrompt(notice))
        ),
        Err(SessionOwnerRejection::Refused(
            SessionOwnerRefusal::EffectInProgress(EffectInProgress::TurnInProgress)
        ))
    ));
    assert_eq!(state.snapshot().queue, prior.queue);
}

#[test]
fn notice_admission_rejects_loading_and_uses_the_prompt_take_of_an_exact_idle_run() {
    let mut state = fresh();
    let run = open_wire(&mut state, 1);
    let notice = input(run.generation, 2);
    assert!(matches!(
        decide(
            &state,
            OwnerEvent::Command(SessionOwnerCommand::NoticePrompt(notice.clone()))
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
            OwnerEvent::Command(SessionOwnerCommand::NoticePrompt(other_run))
        ),
        Err(SessionOwnerRejection::Refused(
            SessionOwnerRefusal::GenerationChanged
        ))
    ));

    let admitted = command(
        &mut state,
        SessionOwnerCommand::NoticePrompt(notice.clone()),
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
fn a_human_start_can_revoke_an_untaken_notice_and_wait_for_its_exact_undo() {
    let mut state = fresh();
    let run = restore_live(&mut state, 1);
    let notice = input(run.generation, 2);
    command(
        &mut state,
        SessionOwnerCommand::NoticePrompt(notice.clone()),
    );
    let offered = apply(&mut state, mark_prompt(&notice));
    let wire = authorization(&offered);
    assert!(
        !offered
            .iter()
            .any(|effect| matches!(effect, OwnerEffect::PromptTaken(_)))
    );
    assert_eq!(state.snapshot().turn, SessionTurn::Idle);

    let human = input(run.generation, 3);
    assert_eq!(
        state
            .snapshot()
            .prompt_start_disposition(human.operation, true),
        PromptStartDisposition::YieldNotice(notice.clone()),
    );
    let revocation = command(
        &mut state,
        SessionOwnerCommand::YieldNoticeBeforePrompt(notice.clone()),
    );
    assert_eq!(
        state
            .snapshot()
            .prompt_start_disposition(human.operation, true),
        PromptStartDisposition::Hold,
    );
    assert!(revocation.contains(&OwnerEffect::Revoke {
        authorization: wire.clone(),
        reason: RevokeReason::NoticeYieldedToPrompt,
    }));
    let revoked = apply(
        &mut state,
        OwnerEvent::Dispatch(DispatchResult::Revoked(wire)),
    );
    assert_eq!(
        state
            .snapshot()
            .prompt_start_disposition(human.operation, true),
        PromptStartDisposition::Hold,
    );
    assert!(
        revoked.contains(&OwnerEffect::Store(SessionStoreRequest::UndoDispatch(
            notice.clone()
        )))
    );
    let settled = apply(
        &mut state,
        OwnerEvent::Store(SessionStoreResult::UndoDispatch {
            input: notice.clone(),
            outcome: EffectOutcome::Applied,
        }),
    );
    assert!(
        settled.contains(&OwnerEffect::Reply(SessionOwnerReply::InputUnsent {
            input: notice,
            reason: RevokeReason::NoticeYieldedToPrompt,
        }))
    );
    assert_eq!(
        state
            .snapshot()
            .prompt_start_disposition(human.operation, true),
        PromptStartDisposition::Proceed,
    );
    let starts = command(
        &mut state,
        SessionOwnerCommand::Prompt {
            input: human.clone(),
            admission: PromptAdmission::Direct,
        },
    );
    assert!(
        starts.contains(&OwnerEffect::Store(SessionStoreRequest::MarkDispatch(
            human
        )))
    );
}

#[test]
fn a_notice_take_winning_the_revocation_race_keeps_the_ordinary_running_refusal() {
    let mut state = fresh();
    let run = restore_live(&mut state, 1);
    let notice = input(run.generation, 2);
    command(
        &mut state,
        SessionOwnerCommand::NoticePrompt(notice.clone()),
    );
    let offer = apply(&mut state, mark_prompt(&notice));
    let wire = authorization(&offer);
    command(
        &mut state,
        SessionOwnerCommand::YieldNoticeBeforePrompt(notice.clone()),
    );
    let taken = apply(
        &mut state,
        OwnerEvent::Dispatch(DispatchResult::AlreadyTaken(wire)),
    );
    assert!(taken.contains(&OwnerEffect::PromptTaken(notice.clone())));
    assert_eq!(
        state.snapshot().turn,
        SessionTurn::Running { turn: notice.turn }
    );
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
