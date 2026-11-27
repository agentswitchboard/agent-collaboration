//! The live callback entry point uses the owner's real inline completion carrier.

use super::*;
use crate::session_owner_ingress::SessionOwnerIngress;
use provider_session_machines::{
    SessionGeneration,
    harness_lifecycle::{CallbackAdmission, HarnessCallbackIngress, HarnessSessionId},
};

#[tokio::test]
async fn inline_callback_ingress_distinguishes_admitted_ended_and_unavailable() {
    let setup = ScriptedSession::load_saved(ScriptControls::default()).await;
    let owner = setup.owner();
    let ingress = SessionOwnerIngress::default();
    ingress.attach(&setup.door);
    let session = HarnessSessionId(SCRIPTED_SESSION.to_owned());
    let generation = ingress.current_generation(&session);
    assert_ne!(generation, SessionGeneration::UNTRACKED);
    let board = ProviderIdentity::session_to_board(&setup.target).expect("board session");
    let mut publications = setup
        .hub
        .owner_inbox(board.clone(), NonZeroUsize::MIN)
        .0
        .capacity_returned();
    let observe_texts = |snapshot: collaboration_contracts::SessionEventAttachment| {
        snapshot
            .snapshot
            .into_iter()
            .filter_map(|event| match event.event {
                SessionEvent::ItemStarted { item } => item.text,
                _ => None,
            })
            .collect::<Vec<_>>()
    };

    assert_eq!(
        ingress.report_for_generation(
            &session,
            generation,
            script::item_event("callback-admitted")
        ),
        CallbackAdmission::Admitted,
    );
    loop {
        let heard = observe_texts(
            setup
                .hub
                .attach(board.clone())
                .await
                .expect("attached history"),
        );
        if heard.iter().any(|text| text == "callback-admitted") {
            break;
        }
        publications
            .changed()
            .await
            .expect("admitted callback published");
    }
    let hold = owner.hold_completion_capacity();
    assert!(!hold.is_empty());
    assert_eq!(
        ingress.report_for_generation(
            &session,
            SessionGeneration::UNTRACKED,
            script::item_event("callback-ended")
        ),
        CallbackAdmission::Ended,
        "a stale generation has already ended even while the inline carrier is full",
    );
    assert_eq!(
        ingress.report_for_generation(
            &session,
            generation,
            script::item_event("callback-unavailable")
        ),
        CallbackAdmission::Unavailable,
        "an exact live generation may lose inline admission when all slots are held",
    );
    drop(hold);
    assert_eq!(
        ingress.report_for_generation(
            &session,
            generation,
            script::item_event("callback-after-capacity")
        ),
        CallbackAdmission::Admitted,
    );
    let heard = loop {
        let heard = observe_texts(
            setup
                .hub
                .attach(board.clone())
                .await
                .expect("retained callbacks"),
        );
        if heard.iter().any(|text| text == "callback-after-capacity") {
            break heard;
        }
        publications
            .changed()
            .await
            .expect("publications advanced past the matrix");
    };
    assert_eq!(
        heard
            .iter()
            .filter(|text| text.starts_with("callback-"))
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["callback-admitted", "callback-after-capacity"],
    );
    assert_eq!(ingress.current_generation(&session), generation);
    setup.door.shutdown().await;
}
