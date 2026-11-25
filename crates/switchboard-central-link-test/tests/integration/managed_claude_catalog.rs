//! Qualifies the installed Claude adapter and SDK against the real managed catalog.

use std::{
    path::Path,
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader},
    net::TcpListener,
    process::Command,
};

use super::{ManagedMcpJourney, TestPhase};

const LOOPBACK_ONLY: &str = r#"(version 1) (allow default) (deny network*) (allow network-outbound (remote ip "localhost:*"))"#;

// Use the real adapter connection. Observe the SDK's effective server status
// before returning the new-session reply. The receipt projects names and states only.
const ADAPTER_RECEIPT: &str = r#"
import fs from 'node:fs';
import path from 'node:path';
import {createRequire} from 'node:module';
import {pathToFileURL} from 'node:url';
const [library, receipt] = process.argv.slice(1);
const installed = createRequire(library);
const manifest = installed('../package.json');
const sdk = installed.resolve('@anthropic-ai/claude-agent-sdk');
const sdkManifest = installed(path.join(path.dirname(sdk), 'package.json'));
if (manifest.version !== '0.81.0' || sdkManifest.version !== '0.3.280') process.exit(42);
const {runAcp} = await import(pathToFileURL(library));
const {agent, connection} = runAcp({log() {}, error() {}});
connection.closed.then(async () => { await agent.dispose(); process.exit(0); });
const create = agent.newSession.bind(agent);
agent.newSession = async (parameters) => {
    const reply = await create(parameters);
    const query = agent.sessions[reply.sessionId].query;
    let servers;
    for (let attempt = 0; attempt < 32; attempt += 1) {
        servers = await query.mcpServerStatus();
        if (servers.some(server => server.name === 'agent-router' && server.tools?.length)) break;
    }
    fs.writeFileSync(receipt, JSON.stringify(servers.map(server => ({
        name: server.name, status: server.status, tools: (server.tools ?? []).map(tool => tool.name).sort(),
    }))));
    return reply;
};
"#;

#[tokio::test]
#[ignore = "requires the locally installed pinned Claude adapter and a macOS loopback-only child sandbox"]
async fn real_claude_gets_one_router_tool_set_fails_without_strict_mcp() {
    let fixture = ManagedMcpJourney::start().await;
    let (_, descriptor) = fixture.prepare(9, 1, TestPhase::Pending);
    let count = Arc::new(AtomicUsize::new(0));
    let count_requests = count.clone();
    let decoy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let decoy_address = decoy_listener.local_addr().unwrap();
    let shutdown = fixture.config.shutdown.clone();
    let decoy = tokio::spawn(async move {
        axum::serve(
            decoy_listener,
            axum::Router::new().fallback(move || {
                count_requests.fetch_add(1, Ordering::Relaxed);
                async { axum::http::StatusCode::SERVICE_UNAVAILABLE }
            }),
        )
        .with_graceful_shutdown(shutdown.cancelled_owned())
        .await
        .unwrap();
    });

    let strict = installed_catalog(&descriptor, &format!("http://{decoy_address}"), true).await;
    assert_eq!(
        count.load(Ordering::Relaxed),
        0,
        "configured MCP endpoints received traffic"
    );
    let tools = ["message_inbox", "message_send", "message_status"];
    assert_eq!(
        strict,
        json!([{"name":"agent-router","status":"connected","tools":tools}])
    );

    // A real counterfactual on the same pin and same tool and config fixtures.
    let non_strict =
        installed_catalog(&descriptor, &format!("http://{decoy_address}"), false).await;
    assert!(
        non_strict
            .as_array()
            .unwrap()
            .iter()
            .any(|server| server["name"] == "configured-router")
    );
    assert!(
        count.load(Ordering::Relaxed) > 0,
        "non-strict control never attempted the configured Router"
    );
    fixture.close().await;
    decoy.await.unwrap();
}

async fn installed_catalog(
    descriptor: &collaboration_protocol::ManagedToolDescriptor,
    decoy: &str,
    strict: bool,
) -> Value {
    let home = tempfile::tempdir().unwrap();
    let receipt = home.path().join("catalog.json");
    std::fs::write(
        home.path().join(".claude.json"),
        serde_json::to_vec(&json!({
            "hasCompletedOnboarding": true,
            "mcpServers": {
                "agent-router": {"type":"http","url":format!("{decoy}/mcp")},
                "configured-router": {"type":"http","url":format!("{decoy}/other")}
            }
        }))
        .unwrap(),
    )
    .unwrap();
    let node = std::env::var_os("ROUTER_TEST_CLAUDE_NODE").unwrap();
    let library = std::env::var_os("ROUTER_TEST_CLAUDE_ADAPTER_LIB").unwrap();
    assert!(Path::new(&node).is_absolute() && Path::new(&library).is_absolute());
    let mut child = Command::new("/usr/bin/sandbox-exec")
        .args(["-p", LOOPBACK_ONLY])
        .arg(node)
        .args(["--input-type=module", "--eval", ADAPTER_RECEIPT, "--"])
        .arg(library)
        .arg(&receipt)
        .current_dir(home.path())
        .env_clear()
        .env("HOME", home.path())
        .env("TMPDIR", home.path())
        .env("PATH", "/usr/bin:/bin")
        .env("CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC", "1")
        .env("DISABLE_AUTOUPDATER", "1")
        .env("ANTHROPIC_API_KEY", "offline-fixture")
        .env("ANTHROPIC_BASE_URL", "http://127.0.0.1:1")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap()).lines();
    for (id, method, params) in [
        (
            1,
            "initialize",
            json!({"protocolVersion":1,"clientCapabilities":{}}),
        ),
        (
            2,
            "session/new",
            json!({
                "cwd":home.path(),"mcpServers":[{"type":"http","name":"agent-router","url":descriptor.url,
                    "headers":[{"name":"Authorization","value":format!("Bearer {}",descriptor.bearer.expose_header_value())}]}],
                "_meta":{"claudeCode":{"options":{"strictMcpConfig":strict}}}
            }),
        ),
    ] {
        let frame = json!({"jsonrpc":"2.0","id":id,"method":method,"params":params});
        stdin
            .write_all(format!("{frame}\n").as_bytes())
            .await
            .unwrap();
        loop {
            let reply: Value =
                serde_json::from_str(&stdout.next_line().await.unwrap().unwrap()).unwrap();
            if reply.get("id").and_then(Value::as_i64) == Some(id) && reply.get("method").is_none()
            {
                assert!(
                    reply.get("result").is_some(),
                    "adapter refused {method} with ACP code {:?}",
                    reply.get("error").and_then(|error| error.get("code"))
                );
                break;
            }
        }
    }
    let catalog = serde_json::from_slice(&std::fs::read(receipt).unwrap()).unwrap();
    drop(stdin);
    assert!(child.wait().await.unwrap().success());
    assert_bearer_absent_from_profile(
        home.path(),
        descriptor.bearer.expose_header_value().as_bytes(),
    );
    catalog
}

fn assert_bearer_absent_from_profile(directory: &Path, bearer: &[u8]) {
    for entry in std::fs::read_dir(directory).unwrap() {
        let entry = entry.unwrap();
        let kind = entry.file_type().unwrap();
        if kind.is_dir() {
            assert_bearer_absent_from_profile(&entry.path(), bearer);
        } else if kind.is_file() {
            let bytes = std::fs::read(entry.path()).unwrap();
            assert!(
                !bytes.windows(bearer.len()).any(|window| window == bearer),
                "Claude wrote the managed bearer to its disposable profile: {:?}",
                entry.file_name()
            );
        }
    }
}
