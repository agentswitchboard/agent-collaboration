use super::*;

#[test]
fn two_bindings_count_as_one_and_a_late_drop_keeps_the_replacement() {
    let presence = CodexSessionPresence::default();
    let generation = CancellationToken::new();
    let session = SessionId::try_from("lease-target".to_owned()).unwrap();
    let original = presence.claim(session.clone(), generation.clone());
    let replacement = presence.claim(session.clone(), generation);

    assert!(presence.contains(&session));
    drop(original);
    assert!(presence.contains(&session));
    drop(replacement);
    assert!(!presence.contains(&session));
}

#[test]
fn retirement_hides_a_claim_before_the_binding_drops() {
    let presence = CodexSessionPresence::default();
    let generation = CancellationToken::new();
    let frontend = generation.child_token();
    let session = SessionId::try_from("lease-target".to_owned()).unwrap();
    let lease = presence.claim(session.clone(), generation.clone());

    frontend.cancel();
    assert!(presence.contains(&session));
    generation.cancel();
    assert!(!presence.contains(&session));
    drop(lease);
}
