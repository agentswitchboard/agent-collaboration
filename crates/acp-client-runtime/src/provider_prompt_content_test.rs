use super::*;

/// The canonical variants retain their exact ACP type and nested payload
/// shape at the single SDK decoding edge.
#[test]
fn audio_and_embedded_resources_keep_payload_and_capability_gate() {
    let contents = vec![
        PromptContent::audio("audio/wav".to_owned(), "aGVsbG8=".to_owned()).expect("audio"),
        PromptContent::embedded_text(
            "file:///note.txt".to_owned(),
            Some("text/plain".to_owned()),
            "hello".to_owned(),
        )
        .expect("text resource"),
        PromptContent::embedded_blob(
            "file:///data.bin".to_owned(),
            Some("application/octet-stream".to_owned()),
            "aGVsbG8=".to_owned(),
        )
        .expect("blob resource"),
    ];
    let blocks = acp_blocks_from_prompt_content(&contents);
    assert_eq!(
        serde_json::to_value(&blocks).expect("ACP blocks"),
        serde_json::json!([
            {"type":"audio","mimeType":"audio/wav","data":"aGVsbG8="},
            {"type":"resource","resource":{"uri":"file:///note.txt",
                "mimeType":"text/plain","text":"hello"}},
            {"type":"resource","resource":{"uri":"file:///data.bin",
                "mimeType":"application/octet-stream","blob":"aGVsbG8="}}
        ])
    );
    assert!(matches!(
        validate_prompt_content(&contents, &ProviderCapabilityReport::default(),),
        Err(ExternalProviderRuntimeError::UnsupportedContent {
            content_type: "audio"
        })
    ));
    let mut capabilities = ProviderCapabilityReport {
        accepts_audio: true,
        ..ProviderCapabilityReport::default()
    };
    assert!(matches!(
        validate_prompt_content(&contents, &capabilities),
        Err(ExternalProviderRuntimeError::UnsupportedContent {
            content_type: "embeddedResource"
        })
    ));
    capabilities.accepts_embedded_resource = true;
    assert!(validate_prompt_content(&contents, &capabilities).is_ok());
}

#[test]
fn prompt_content_gate_matches_advertised_optional_types() {
    // ACP v1 initialization.mdx:202-217: text and resource links are baseline;
    // image, audio and embedded resources require promptCapabilities.
    let examples = [
        ("text", serde_json::json!({"type": "text", "text": "hello"})),
        (
            "resourceLink",
            serde_json::json!({"type": "resourceLink", "name": "reference", "uri": "file:///tmp/reference"}),
        ),
        (
            "image",
            serde_json::json!({"type": "image", "mimeType": "image/png", "data": "AA=="}),
        ),
        (
            "audio",
            serde_json::json!({"type": "audio", "mimeType": "audio/wav", "data": "AA=="}),
        ),
        (
            "embeddedResource",
            serde_json::json!({"type": "embeddedResource", "uri": "file:///tmp/reference", "text": "content"}),
        ),
    ];
    for advertised_mask in 0_u8..8 {
        let report = ProviderCapabilityReport {
            accepts_image: advertised_mask & 1 != 0,
            accepts_audio: advertised_mask & 2 != 0,
            accepts_embedded_resource: advertised_mask & 4 != 0,
            ..ProviderCapabilityReport::default()
        };
        for (content_type, wire_block) in &examples {
            let block: PromptContent =
                serde_json::from_value(wire_block.clone()).expect("canonical content example");
            let result = validate_prompt_content(&[block], &report);
            let accepted = match *content_type {
                "text" | "resourceLink" => true,
                "image" => report.accepts_image,
                "audio" => report.accepts_audio,
                "embeddedResource" => report.accepts_embedded_resource,
                _ => false,
            };
            if accepted {
                assert!(result.is_ok(), "{content_type} mask {advertised_mask}");
            } else {
                assert_eq!(
                    result
                        .expect_err("unadvertised content is rejected")
                        .to_string(),
                    format!("unsupportedContent{{{content_type}}}")
                );
            }
        }
    }
}
