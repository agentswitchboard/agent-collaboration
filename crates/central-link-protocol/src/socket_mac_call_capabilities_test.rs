use super::*;
use workspace_ids::ApprovalRequestId;

fn ending_item(ordinal: usize) -> MacCallDeliveryKey {
    let request = format!("019f0000-0000-7000-8000-{ordinal:012x}")
        .parse::<ApprovalRequestId>()
        .unwrap();
    MacCallDeliveryKey::Ending(
        request,
        "019f0000-0000-7000-8000-ffffffffffff".parse().unwrap(),
    )
}

#[test]
fn empty_announcement_is_final_and_cannot_claim_an_item() {
    let mut socket = SocketMacCallState::default();
    socket
        .announce(MacCapabilities {
            handled_calls: HandledMacCalls::default(),
        })
        .unwrap();
    assert_eq!(
        socket.begin(ending_item(1)),
        Err(MacCallSkip::KindNotAdvertised)
    );
    let ending = HandledMacCalls::try_from(vec![MacCallKind::DeliverEnding]).unwrap();
    assert_eq!(
        socket.announce(MacCapabilities {
            handled_calls: ending
        }),
        Err(DuplicateMacCapabilities)
    );
}

#[test]
fn saturation_waits_for_settled_inflight_and_is_observed_once() {
    let mut socket = SocketMacCallState::default();
    let ending = HandledMacCalls::try_from(vec![MacCallKind::DeliverEnding]).unwrap();
    socket
        .announce(MacCapabilities {
            handled_calls: ending,
        })
        .unwrap();
    for ordinal in 0..MAX_SOCKET_MAC_CALL_ITEMS {
        assert_eq!(socket.begin(ending_item(ordinal)), Ok(()));
        assert_eq!(
            socket.begin(ending_item(ordinal)),
            Err(MacCallSkip::AlreadyOffered)
        );
    }
    let deferred = ending_item(MAX_SOCKET_MAC_CALL_ITEMS);
    assert_eq!(
        socket.begin(deferred.clone()),
        Err(MacCallSkip::FirstSaturation)
    );
    assert_eq!(socket.begin(deferred.clone()), Err(MacCallSkip::Saturated));
    assert!(!socket.reclaim_settled(&ending_item(0)));
    assert_eq!(socket.begin(deferred.clone()), Err(MacCallSkip::Saturated));
    assert!(socket.finish(&ending_item(0)));
    assert_eq!(socket.begin(deferred.clone()), Ok(()));
    assert!(!socket.finish(&deferred));
    assert_eq!(socket.begin(deferred), Err(MacCallSkip::AlreadyOffered));
    assert_eq!(
        socket.begin(ending_item(MAX_SOCKET_MAC_CALL_ITEMS + 1)),
        Err(MacCallSkip::Saturated)
    );
    assert!(serde_json::to_vec(&socket).unwrap().len() < 12 * 1024);
}
