use super::*;
use crate::{
    HandledMacCalls, MAX_SOCKET_MAC_CALL_ITEMS, MacCallDeliveryKey, MacCallKind, MacCapabilities,
    SocketMacCallState,
};

fn message(ordinal: usize) -> MessageId {
    format!("019f0000-0000-7000-8000-{ordinal:012x}")
        .parse()
        .unwrap()
}

#[test]
fn saturation_and_competing_claims_survive_attachment_round_trip() {
    let mut offers = SocketDirectMailOffers::default();
    let now: Timestamp = "2026-11-23T22:00:00Z".parse().unwrap();
    let cutoff: Timestamp = "2026-11-23T22:15:00Z".parse().unwrap();
    for ordinal in 0..SOCKET_DIRECT_MAIL_LIMIT {
        assert_eq!(offers.claim(&message(ordinal), cutoff), Ok(()));
    }
    let waiting = message(SOCKET_DIRECT_MAIL_LIMIT);
    assert_eq!(offers.claim(&waiting, cutoff), Err(NotOffered::Deferred));

    let persisted = serde_json::to_vec(&offers).unwrap();
    let mut reawakened: SocketDirectMailOffers = serde_json::from_slice(&persisted).unwrap();
    assert!(!reawakened.take_deferred());
    assert_eq!(
        reawakened.claim(&message(0), cutoff),
        Err(NotOffered::AlreadyOffered)
    );
    reawakened.release(&message(0));
    assert!(reawakened.take_deferred());
    assert_eq!(reawakened.claim(&waiting, cutoff), Ok(()));

    assert_eq!(
        reawakened.elapsed_claims(cutoff).count(),
        SOCKET_DIRECT_MAIL_LIMIT
    );
    assert_eq!(reawakened.elapsed_claims(now).count(), 0);
    assert_eq!(
        reawakened.claim(&waiting, cutoff),
        Err(NotOffered::AlreadyOffered)
    );
}

#[test]
fn full_ledgers_leave_space_for_socket_authority_under_the_attachment_limit() {
    let mut mail = SocketDirectMailOffers::default();
    let cutoff: Timestamp = "2026-11-23T22:15:00Z".parse().unwrap();
    for ordinal in 0..SOCKET_DIRECT_MAIL_LIMIT {
        assert_eq!(mail.claim(&message(ordinal), cutoff), Ok(()));
    }
    let mut calls = SocketMacCallState::default();
    calls
        .announce(MacCapabilities {
            handled_calls: HandledMacCalls::try_from(vec![MacCallKind::DeliverEnding]).unwrap(),
        })
        .unwrap();
    let run = "019f0000-0000-7000-8000-ffffffffff00"
        .parse::<workspace_ids::RouterRunId>()
        .unwrap();
    for ordinal in 0..MAX_SOCKET_MAC_CALL_ITEMS {
        calls
            .begin(MacCallDeliveryKey::Ending(
                format!("019f0000-0000-7000-8000-{ordinal:012x}")
                    .parse()
                    .unwrap(),
                run.clone(),
            ))
            .unwrap();
    }
    let serialized = serde_json::to_vec(&(mail, calls)).unwrap();
    assert!(
        serialized.len() < 15 * 1024,
        "ledger bytes: {}",
        serialized.len()
    );
}
