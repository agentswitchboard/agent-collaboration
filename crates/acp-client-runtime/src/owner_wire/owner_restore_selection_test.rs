//! An update may pause after choosing its actual restore registration.

use super::super::OwnerAcpWire;
use crate::{
    acp_transport::TransportRun, owner_session_setting::tests::ConnectionCatalogueBehavior,
};
use agent_client_protocol::{Dispatch, UntypedMessage};
use provider_session_machines::{SessionGeneration, session_owner::OwnerIncarnation};
use session_event_model::session_profile_codec::STATE_METHOD;
use std::{num::NonZeroUsize, sync::Arc};
use switchboard_harness_shared_mechanics::opening::{LifecycleRoute, OpeningInstance};
use uuid::Uuid;

#[test]
fn a_pinned_retired_restore_never_delivers_into_the_reused_generation() {
    let (wire, _outgoing) =
        OwnerAcpWire::new(NonZeroUsize::MIN, Arc::new(ConnectionCatalogueBehavior));
    let run = TransportRun {
        incarnation: OwnerIncarnation(Uuid::now_v7()),
        generation: SessionGeneration::from_number(1),
    };
    let session = "same-provider-session";
    let old_route = LifecycleRoute::new(run);
    let old_stamp = OpeningInstance::default();
    wire.install_opening_route(&old_route, session.to_owned())
        .expect("install A's actual route");
    assert!(
        wire.transport
            .attach_restore_registration(run, Some(&old_route), &old_stamp)
    );
    let _a = wire
        .restore_updates
        .open_instance(run, session.to_owned(), &old_stamp);
    wire.restore_updates.reply_settled(run, &old_stamp);

    let selected_before_reuse = wire.select_restore_update(session);
    wire.retire_opening_route(&old_route, &old_stamp);
    let new_route = LifecycleRoute::new(run);
    let new_stamp = OpeningInstance::default();
    wire.install_opening_route(&new_route, session.to_owned())
        .expect("install B at the same run key");
    assert!(
        wire.transport
            .attach_restore_registration(run, Some(&new_route), &new_stamp)
    );
    let mut b = wire
        .restore_updates
        .open_instance(run, session.to_owned(), &new_stamp);

    for method in ["session/update", STATE_METHOD] {
        assert!(
            wire.forward_selected_restore_update(
                session,
                selected_before_reuse.clone(),
                update(session, method, "A")
            )
            .is_none(),
            "a retired restore selection cannot fall back to generic dispatch"
        );
        assert!(
            matches!(
                b.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            ),
            "a marked old update leaked into the replacement tap"
        );
    }

    assert!(
        wire.forward_restored_update(session, update(session, "session/update", "B"))
            .is_none()
    );
    let Dispatch::Notification(fresh) = b
        .try_recv()
        .expect("B's exact tap accepts its own fresh update")
    else {
        panic!("the fresh provider update was not a notification");
    };
    assert_eq!(
        fresh
            .params()
            .get("marker")
            .and_then(serde_json::Value::as_str),
        Some("B")
    );
}

fn update(session: &str, method: &str, marker: &str) -> Dispatch {
    Dispatch::Notification(
        UntypedMessage::new(
            method,
            serde_json::json!({ "sessionId": session, "marker": marker }),
        )
        .expect("construct the raw ACP notification"),
    )
}
