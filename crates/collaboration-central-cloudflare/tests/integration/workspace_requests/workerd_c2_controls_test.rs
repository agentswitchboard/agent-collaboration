use super::{BridgeReply, C2EvictionCause, C2EvictionError};

#[test]
fn an_active_reference_eviction_failure_retains_its_bridge_cause() {
    let target = "019f0000-0000-7000-8000-00000000d299"
        .to_owned()
        .try_into()
        .unwrap();
    let failure: BridgeReply =
        serde_json::from_str(r#"{"answer":"c2EvictionFailed","reason":"activeReferences"}"#)
            .unwrap();

    assert!(matches!(
        failure.eviction_result(&target),
        Err(C2EvictionError::Failed(C2EvictionCause::ActiveReferences))
    ));
}
