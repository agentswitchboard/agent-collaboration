use super::{
    ROUTER,
    codex_holder::{initialize_native_fixture, native_reply, native_request, native_schemas},
};
use collaboration_runtime::NativeGenerationGate;
use futures_util::{FutureExt, StreamExt};
use serde_json::{Value, json};
use std::{future::Future, path::Path, pin::pin};
use tokio::net::{UnixListener, UnixStream};
use tokio_tungstenite::WebSocketStream;

pub(super) struct NativeListFixture {
    pub gate: NativeGenerationGate,
    pub listener: UnixListener,
}

impl NativeListFixture {
    pub(super) fn bind(root: &Path) -> Self {
        let listener = UnixListener::bind(root.join("native.sock")).unwrap();
        let fixture = Self {
            gate: NativeGenerationGate::default(),
            listener,
        };
        fixture.activate(root, 1);
        fixture
    }

    pub(super) fn activate(&self, root: &Path, generation: u64) {
        self.gate
            .activate(
                serde_json::from_value(json!({"serviceEpoch": ROUTER, "generation": generation}))
                    .unwrap(),
                root.join("native.sock"),
                Some(native_schemas()),
            )
            .unwrap();
    }

    pub(super) async fn request(&self) -> (WebSocketStream<UnixStream>, Value) {
        let mut wire = initialize_native_fixture(&self.listener).await;
        let request = native_request(&mut wire, "thread/loaded/list").await;
        assert_eq!(request["params"], json!({}), "list must be unpaginated");
        (wire, request)
    }

    pub(super) async fn run<T>(&self, lookup: impl Future<Output = T>, response: Value) -> T {
        let mut lookup = pin!(lookup);
        let (mut wire, request) = tokio::select! {
            _ = &mut lookup => panic!("lookup completed without a native list request"),
            request = self.request() => request,
        };
        native_reply(&mut wire, &request, response).await;
        let no_extra_frame = async {
            let frame = wire.next().await;
            assert!(
                !matches!(
                    frame,
                    Some(Ok(tokio_tungstenite::tungstenite::Message::Text(_)))
                ),
                "lookup sent more than the one list request: {frame:?}"
            );
            std::future::pending::<T>().await
        };
        tokio::select! {
            biased;
            _ = self.listener.accept() => panic!("lookup opened a second native connection"),
            answer = no_extra_frame => answer,
            answer = &mut lookup => answer,
        }
    }

    pub(super) fn assert_no_connection(&self) {
        assert!(
            self.listener.accept().now_or_never().is_none(),
            "unexpected native connection"
        );
    }
}
