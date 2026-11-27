use super::*;
use acp_client_runtime::handover_test_control::HandoverPausePoint;
use provider_session_machines::{
    ProviderSettingKind,
    session_lifecycle_machine::SessionLifecycle,
    session_owner::{
        OwnerSetting, RemoteEffect, RunAttempt, SessionOwnerCommand, SessionOwnerReply,
    },
    session_setting_machine::{SettingApplication, SettingValueId},
};
use session_event_model::InputId;
use uuid::Uuid;

fn setting_work(
    owner: &crate::session_owner_door::SessionOwnerAddress,
) -> (Uuid, InputId, SessionOwnerCommand) {
    let SessionLifecycle::Live { generation } = owner.snapshots().borrow().lifecycle else {
        panic!("setting fixture must be live");
    };
    let operation = Uuid::now_v7();
    let input = InputId::generate();
    let command = SessionOwnerCommand::Set {
        input: input.clone(),
        setting: OwnerSetting {
            kind: ProviderSettingKind::Model,
            change: RunAttempt {
                generation,
                attempt: SettingApplication(operation),
            },
        },
        value: SettingValueId("new".into()),
        expected_model: None,
    };
    (operation, input, command)
}

async fn fixture() -> SteeringSession {
    managed_notice_fixture(
        Arc::new(ManualClock::new(chrono::DateTime::UNIX_EPOCH)),
        NoticeScript::SettingWork,
        collaboration_protocol::ProviderKind::ClaudeCode,
    )
    .await
}

async fn assert_no_setting_received(rig: &SteeringSession) {
    rig.supervisor.shutdown().await.expect("fixture shutdown");
    assert!(!rig.receipt.with_extension("setting").exists());
    assert!(!rig.receipt.exists(), "unexpected wire request");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn setting_with_no_waiter_never_reaches_the_offer_record() {
    let rig = fixture().await;
    let owner = rig
        .supervisor
        .session_owner_for(&rig.target)
        .expect("setting owner");
    let (_, input, command) = setting_work(&owner);
    let runtime =
        crate::provider_test_support::fixture_runtime_for(&rig.supervisor, &rig.target.endpoint)
            .expect("fixture runtime");
    let pause = runtime.pause_work_handover_for_test(HandoverPausePoint::BeforeOffer, input);

    tokio::select! {
        accepted = owner.admit(command) => accepted.expect("setting command settled its outbox"),
        reached = pause.reached => {
            pause.release.send(()).expect("release unexpected offer");
            panic!("an unwanted setting reached the offer record: {reached:?}");
        }
    }

    assert_no_setting_received(&rig).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn setting_that_loses_its_waiter_at_the_writer_question_publishes_no_frame() {
    let rig = fixture().await;
    let owner = rig
        .supervisor
        .session_owner_for(&rig.target)
        .expect("setting owner");
    let (operation, input, command) = setting_work(&owner);
    let runtime =
        crate::provider_test_support::fixture_runtime_for(&rig.supervisor, &rig.target.endpoint)
            .expect("fixture runtime");
    let waiter = owner.expect_work_settlement_for_test(operation);
    let before =
        runtime.pause_work_handover_for_test(HandoverPausePoint::BeforeQuestion, input.clone());
    let admission = owner.admit(command);
    tokio::pin!(admission);
    let authorization = tokio::select! {
        accepted = &mut admission => panic!("an offered setting did not reach the question: {accepted:?}"),
        reached = before.reached => reached.expect("exact offer reached the question"),
    };
    assert_eq!(authorization.input_id, input);

    drop(waiter);
    let after = runtime.pause_work_handover_for_test(HandoverPausePoint::BeforePublication, input);
    before.release.send(()).expect("release final question");
    tokio::select! {
        accepted = admission => accepted.expect("setting command settled its outbox"),
        reached = after.reached => {
            after.release.send(()).expect("release unexpected publication");
            panic!("a setting without its waiter would have been written: {reached:?}");
        }
    }

    assert_no_setting_received(&rig).await;
}

#[tokio::test]
async fn setting_that_keeps_its_waiter_reaches_the_peer_once() {
    let rig = fixture().await;
    let owner = rig
        .supervisor
        .session_owner_for(&rig.target)
        .expect("setting owner");
    let (operation, _, command) = setting_work(&owner);

    let settled = owner
        .settle(operation, command)
        .await
        .expect("setting settled");

    assert!(matches!(
        settled.reply,
        SessionOwnerReply::SettingFinished {
            remote: RemoteEffect::Applied,
            ..
        }
    ));
    assert!(rig.receipt.with_extension("setting").is_file());
    assert!(!rig.receipt.exists(), "unexpected wire request");
    rig.supervisor.shutdown().await.expect("fixture shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn an_unrealized_auto_mode_settles_inline_without_an_offer() {
    use provider_session_machines::session_owner::PermissionModeReason;

    let rig = fixture().await;
    let owner = rig
        .supervisor
        .session_owner_for(&rig.target)
        .expect("setting owner");
    let (operation, input, mut command) = setting_work(&owner);
    let SessionOwnerCommand::Set { setting, value, .. } = &mut command else {
        panic!("fixture command must set a value");
    };
    setting.kind = ProviderSettingKind::Mode;
    value.0 = "auto".into();
    let setting = *setting;
    let runtime =
        crate::provider_test_support::fixture_runtime_for(&rig.supervisor, &rig.target.endpoint)
            .expect("fixture runtime");
    let pause = runtime.pause_work_handover_for_test(HandoverPausePoint::BeforeOffer, input);

    let settled = tokio::select! {
        settled = owner.settle(operation, command) => settled.expect("keeping the mode settled"),
        observed = pause.reached => {
            let _released = pause.release.send(());
            panic!("keeping the mode reached the offer record: {observed:?}");
        }
    };

    assert_eq!(settled.reply, SessionOwnerReply::SettingKept { setting });
    assert_eq!(
        owner.snapshots().borrow().mode_reason,
        Some(PermissionModeReason::AutoNotRealized)
    );
    assert_no_setting_received(&rig).await;
}
