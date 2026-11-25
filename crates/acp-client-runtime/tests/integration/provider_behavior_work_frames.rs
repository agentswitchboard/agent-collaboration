#![expect(
    clippy::panic_in_result_fn,
    reason = "Fixture I/O errors propagate while assertion failures retain the exact wire diff."
)]

use super::*;
use serde_json::Value;

const ORIGINAL_WORK_REQUESTS: &str = concat!(
    r#"{"jsonrpc":"2.0","id":"<request-id>","method":"session/prompt","params":{"sessionId":"session-1","prompt":["#,
    r#"{"type":"text","text":"Read\tthis\nλ"},"#,
    r#"{"type":"resource_link","mimeType":"text/plain","name":"Context","uri":"https://example.test/context"},"#,
    r#"{"type":"image","data":"eA==","mimeType":"image/png","uri":"file:///fixture.png"},"#,
    r#"{"type":"audio","data":"eA==","mimeType":"audio/wav"},"#,
    r#"{"type":"resource","resource":{"mimeType":"text/plain","text":"note","uri":"resource://note"}},"#,
    r#"{"type":"resource","resource":{"blob":"eA==","uri":"resource://blob"}}]}}"#,
    "\n",
    r#"{"jsonrpc":"2.0","id":"<request-id>","method":"session/prompt","params":{"sessionId":"session-1","prompt":[{"type":"text","text":"/compact"}]}}"#,
);

#[derive(serde::Deserialize)]
struct CapturedLine {
    direction: String,
    line: String,
}

#[tokio::test]
async fn work_frames_preserve_exact_content_and_reply_correlation()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::open(
        ScriptedBehavior {
            skill: "review",
            preparation: Preparation::Preserve,
        },
        "work-frames",
    )
    .await?;
    let contents = vec![
        PromptContent::text("Read\tthis\nλ".into())?,
        PromptContent::resource_link(
            "https://example.test/context".into(),
            "Context".into(),
            Some("text/plain".into()),
        )?,
        PromptContent::image(
            "image/png".into(),
            "eA==".into(),
            Some("file:///fixture.png".into()),
        )?,
        PromptContent::audio("audio/wav".into(), "eA==".into())?,
        PromptContent::embedded_text(
            "resource://note".into(),
            Some("text/plain".into()),
            "note".into(),
        )?,
        PromptContent::embedded_blob("resource://blob".into(), None, "eA==".into())?,
    ];

    let prompt = fixture
        .submit(ProviderTurnInput::Prompt(ProviderSubmission::plain(
            contents,
        )))
        .await?;
    let compact = fixture.submit(ProviderTurnInput::Compact).await?;
    fixture.close().await?;

    assert_eq!(prompt.stop_reason, StopReason::EndTurn);
    assert_eq!(compact.stop_reason, StopReason::EndTurn);
    let capture = std::fs::read_to_string(fixture.root.path().join("wire.jsonl.frames"))?;
    let frames = capture
        .lines()
        .map(|line| {
            let captured: CapturedLine = serde_json::from_str(line)?;
            let envelope: Value = serde_json::from_str(&captured.line)?;
            Ok((captured, envelope))
        })
        .collect::<Result<Vec<_>, serde_json::Error>>()?;
    let mut seen_ids = std::collections::BTreeSet::new();
    let mut received = Vec::new();
    for (capture, frame) in &frames {
        if capture.direction != "received" || frame["method"] != "session/prompt" {
            continue;
        }
        let request_id = frame.get("id").ok_or("prompt id missing")?;
        assert!(request_id.is_string() || request_id.is_number());
        let encoded_id = serde_json::to_string(request_id)?;
        assert!(seen_ids.insert(encoded_id.clone()), "request ID reused");
        let id_field = format!(r#""id":{encoded_id}"#);
        assert_eq!(capture.line.matches(&id_field).count(), 1);
        received.push(
            capture
                .line
                .replacen(&id_field, r#""id":"<request-id>""#, 1),
        );
        let matching = frames.iter().filter(|(capture, reply)| {
            capture.direction == "sent" && reply.get("id") == Some(request_id)
        });
        assert_eq!(
            matching
                .map(|(_, reply)| reply["result"]["stopReason"].clone())
                .collect::<Vec<_>>(),
            [Value::String("end_turn".into())],
            "peer must answer each observed prompt exactly once",
        );
    }
    assert_eq!(received.join("\n"), ORIGINAL_WORK_REQUESTS);
    Ok(())
}

#[tokio::test]
async fn unsupported_audio_is_refused_before_work_is_sent() -> Result<(), Box<dyn std::error::Error>>
{
    let fixture = Fixture::open(
        ScriptedBehavior {
            skill: "review",
            preparation: Preparation::Preserve,
        },
        "manual",
    )
    .await?;
    let contents = vec![
        PromptContent::text("Audio follows".into())?,
        PromptContent::audio("audio/wav".into(), "eA==".into())?,
    ];

    let result = fixture
        .submit(ProviderTurnInput::Prompt(ProviderSubmission::plain(
            contents,
        )))
        .await;
    fixture.close().await?;

    assert!(matches!(
        result,
        Err(ExternalProviderRuntimeError::UnsupportedContent {
            content_type: "audio"
        })
    ));
    assert!(fixture.wire()?.is_empty());
    Ok(())
}
