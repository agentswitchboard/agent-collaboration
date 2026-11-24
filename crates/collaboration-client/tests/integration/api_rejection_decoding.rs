//! Published rejections keep their typed evidence through the client.
use collaboration_client::{ClientError, CollaborationClient};
use serde_json::{Value, json};

use crate::scripted_api;
use scripted_api::SCRIPTED_SERVICE_ID;

#[tokio::test]
async fn rpc_error_requires_matching_call_id_before_preserving_rejection() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // The published availability payload must not qualify another request's answer.
    // Exercise the real Unix HTTP decoder in both supported response carriers.
    for streaming in [false, true] {
        for response_id in [json!(1), json!(9), Value::Null] {
            let directory = tempfile::tempdir().unwrap();
            let listener =
                tokio::net::UnixListener::bind(directory.path().join("control.sock")).unwrap();
            std::fs::write(
                directory.path().join("service.json"),
                json!({
                    "version":3,"serviceId":SCRIPTED_SERVICE_ID,
                    "serviceEpoch":"00000000-0000-4000-8000-000000000002",
                    "machineLabel":"correlation-fixture","serviceVersion":"1",
                    "api":{"transport":"streamableHttpUnix","path":"control.sock"},
                    "mcp":{"transport":"streamableHttp","url":"http://127.0.0.1:0/mcp"}
                })
                .to_string(),
            )
            .unwrap();
            let expected_match = response_id == json!(1);
            let published = json!({"kind":"unavailable","stage":"discovery",
                "message":"private-correlation-sentinel"});
            let expected_data = published.clone();
            let fixture = tokio::spawn(async move {
                loop {
                    let (mut stream, _) = listener.accept().await.unwrap();
                    let mut request = Vec::new();
                    let mut buffer = [0_u8; 4096];
                    let header_end = loop {
                        let count = stream.read(&mut buffer).await.unwrap();
                        if count == 0 {
                            break None;
                        }
                        request.extend_from_slice(buffer.get(..count).unwrap());
                        if let Some(position) =
                            request.windows(4).position(|part| part == b"\r\n\r\n")
                        {
                            break Some(position + 4);
                        }
                        assert!(request.len() < 65536);
                    };
                    let Some(header_end) = header_end else {
                        continue;
                    };
                    let headers = String::from_utf8_lossy(request.get(..header_end).unwrap());
                    let body_length: usize = headers
                        .lines()
                        .filter_map(|line| line.split_once(':'))
                        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                        .unwrap()
                        .1
                        .trim()
                        .parse()
                        .unwrap();
                    while request.len() < header_end + body_length {
                        let count = stream.read(&mut buffer).await.unwrap();
                        assert_ne!(count, 0);
                        request.extend_from_slice(buffer.get(..count).unwrap());
                    }
                    let request: Value =
                        serde_json::from_slice(request.get(header_end..).unwrap()).unwrap();
                    assert_eq!(request.get("id").unwrap(), 1);
                    assert_eq!(request.pointer("/params/name").unwrap(), "endpoints_list");
                    let message = json!({"jsonrpc":"2.0","id":response_id,
                        "error":{"code":-32050,"message":"unavailable","data":published}});
                    let (content_type, body) = if streaming {
                        (
                            "text/event-stream",
                            format!("event: message\ndata: {message}\n\n"),
                        )
                    } else {
                        ("application/json", message.to_string())
                    };
                    let response = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    stream.write_all(response.as_bytes()).await.unwrap();
                    stream.shutdown().await.unwrap();
                    break;
                }
            });
            let client = CollaborationClient::connect(directory.path(), "correlation", "1")
                .await
                .unwrap();
            let result =
                tokio::time::timeout(std::time::Duration::from_secs(5), client.list_endpoints())
                    .await
                    .unwrap()
                    .unwrap_err();
            tokio::time::timeout(std::time::Duration::from_secs(5), fixture)
                .await
                .unwrap()
                .unwrap();
            if expected_match {
                assert!(
                    matches!(result, ClientError::Rejected {code:-32050, data:Some(data)} if data == expected_data)
                );
            } else {
                assert!(
                    matches!(
                        result,
                        ClientError::Protocol("API answer does not match the call")
                    ),
                    "uncorrelated {streaming} error became {result:?}"
                );
                assert!(!format!("{result:?}").contains("private-correlation-sentinel"));
            }
        }
    }
}
