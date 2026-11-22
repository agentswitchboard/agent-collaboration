use super::*;
use collaboration_contracts::{InvalidProviderModelEntry, ProviderReasoningChoice};

#[test]
fn bootstrap_delegates_to_provider_without_fabricated_effort() {
    // Program Design Native: bootstrap is an action, never a measured medium.
    let response = render_model_list(&[]);
    assert_eq!(
        response,
        json!({
            "data": [{
                "id": "provider-default", "model": "provider-default",
                "upgrade": null, "upgradeInfo": null, "availabilityNux": null,
                "displayName": "Use provider settings", "description": "Use provider settings",
                "hidden": false,
                "supportedReasoningEfforts": [{"reasoningEffort": "provider", "description": "Use provider settings"}],
                "defaultReasoningEffort": "provider", "multiAgentVersion": null, "isDefault": true
            }],
            "nextCursor": null
        })
    );
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "Contract asserts; invalid fixture constructors propagate."
)]
fn all_models_keep_independent_defaults_and_reserved_collisions_delegate()
-> Result<(), InvalidProviderModelEntry> {
    let first = ProviderModelEntry::try_new("a".into(), "A".into(), String::new())?
        .with_reasoning(
            vec![ProviderReasoningChoice {
                value: "low".into(),
                label: "Low".into(),
            }],
            "low".into(),
        )?;
    let unknown = ProviderModelEntry::try_new("b".into(), "B".into(), String::new())?;
    let collision = ProviderModelEntry::try_new("c".into(), "C".into(), String::new())?
        .with_reasoning(
            vec![ProviderReasoningChoice {
                value: "provider".into(),
                label: "Vendor level".into(),
            }],
            "provider".into(),
        )?;
    let response = render_model_list(&[first, unknown, collision]);
    assert_eq!(
        response,
        json!({
            "data": [
                {
                    "id": "a", "model": "a", "upgrade": null, "upgradeInfo": null, "availabilityNux": null,
                    "displayName": "A", "description": "", "hidden": false,
                    "supportedReasoningEfforts": [{"reasoningEffort": "low", "description": "Low"}],
                    "defaultReasoningEffort": "low", "multiAgentVersion": null, "isDefault": true
                },
                {
                    "id": "b", "model": "b", "upgrade": null, "upgradeInfo": null, "availabilityNux": null,
                    "displayName": "B", "description": "", "hidden": false,
                    "supportedReasoningEfforts": [{"reasoningEffort": "provider", "description": "Use provider settings"}],
                    "defaultReasoningEffort": "provider", "multiAgentVersion": null, "isDefault": false
                },
                {
                    "id": "c", "model": "c", "upgrade": null, "upgradeInfo": null, "availabilityNux": null,
                    "displayName": "C", "description": "", "hidden": false,
                    "supportedReasoningEfforts": [{"reasoningEffort": "provider", "description": "Use provider settings"}],
                    "defaultReasoningEffort": "provider", "multiAgentVersion": null, "isDefault": false
                }
            ],
            "nextCursor": null
        })
    );
    Ok(())
}
