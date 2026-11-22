use super::support::*;
use central_link_protocol::{
    DataFrame, FrameDirection, LinkFrame, MachineLinkCloseCode, decode_text,
};
use central_storage::SqlValue;
use futures_util::{SinkExt, StreamExt};
use reqwest_websocket::{Message, RequestBuilderExt};
use signed::machine_lifecycle::{
    GrantRevokeOrder, MachineLifecycleHost, MachineLifecycleTrace, ReplayProbe,
};

#[hegel::test(test_cases = 12, seed = Some(20261122), suppress_health_check = [hegel::HealthCheck::TooSlow])]
fn generated_machine_login_routes_preserve_enrollment_and_allowance_lifecycle(tc: hegel::TestCase) {
    use hegel::generators as gs;

    let workspaces = if tc.draw(gs::booleans()) {
        [WORKSPACE_TWO, WORKSPACE]
    } else {
        [WORKSPACE, WORKSPACE_TWO]
    };
    let lapse_offset = tc.draw(gs::integers::<u64>().min_value(1).max_value(1999));
    let first_grant_elapsed_seconds = tc.draw(gs::integers::<u64>().min_value(0).max_value(4));
    let stale_client_at_deadline = tc.draw(gs::booleans());
    let above_first = tc.draw(gs::booleans());
    let replay_probes = tc.draw(
        gs::vecs(gs::sampled_from(vec![
            ReplayProbe::Equal,
            ReplayProbe::ChangedLabel,
            ReplayProbe::ChangedWorkspaceSet,
            ReplayProbe::ChangedDeadline,
            ReplayProbe::Reopen,
        ]))
        .min_size(0)
        .max_size(4),
    );
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        for (order, lapse_millis_after_deadline) in [
            (
                GrantRevokeOrder::GrantThenRevoke,
                if above_first { lapse_offset } else { 0 },
            ),
            (
                GrantRevokeOrder::RevokeThenGrant,
                if above_first { 0 } else { lapse_offset },
            ),
        ] {
            let trace = MachineLifecycleTrace {
                organization: ORG,
                machine: INSTALLATION,
                workspaces,
                lapse_millis_after_deadline,
                stale_client_at_deadline,
                first_grant_elapsed_seconds,
                replay_probes: replay_probes.clone(),
            };
            let mut fixture = MachineFixture::new().await.unwrap();
            trace.prepare_revoker(&fixture).await.unwrap();
            let result = trace.exercise(&mut fixture, order).await;
            fixture.close().await.unwrap();
            result.unwrap_or_else(|error| panic!("{order:?} {trace:?}: {error}"));
        }
    });
}

impl MachineLifecycleHost for MachineFixture {
    fn now_millis(&self) -> i64 {
        use runtime_primitives::Clock;
        self.inner.clock.now().unix_millis()
    }

    async fn post(
        &self,
        path: &'static str,
        body: &Value,
        headers: &[(&'static str, String)],
    ) -> Result<DecisionReply> {
        let reply = super::super::support::post(
            &self.inner.client(),
            path,
            serde_json::to_string(body)?,
            headers.to_vec(),
        )
        .await?;
        Ok((
            reply.status,
            if reply.body.is_empty() {
                Value::Null
            } else {
                reply.value()?
            },
        ))
    }

    async fn rows(&self, sql: &'static str) -> Result<Vec<Value>> {
        MachineFixture::rows(self, sql)
            .await?
            .into_iter()
            .map(|row| {
                let [SqlValue::Text(value)] = row.as_slice() else {
                    return Err(format!("expected one JSON array per owning row: {row:?}").into());
                };
                Ok(serde_json::from_str(value)?)
            })
            .collect()
    }

    async fn advance(&self, millis: u64) -> Result<()> {
        MachineFixture::advance(self, millis).await
    }

    async fn reopen(&mut self) -> Result<()> {
        MachineFixture::reopen(self).await
    }

    async fn revoke_and_observe_close(
        &self,
        machine: &str,
        bearer: &str,
        path: &'static str,
        body: &Value,
        headers: &[(&'static str, String)],
    ) -> Result<DecisionReply> {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
        let address = listener.local_addr()?;
        let link = collaboration_central_tokio::WorkspaceLink::new(self.inner.host.clone());
        let router = link.router();
        let shutdown = tokio_util::sync::CancellationToken::new();
        let stopped = shutdown.clone();
        let listener_task = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(stopped.cancelled_owned())
                .await
        });
        let url = format!("http://{address}/v1/organizations/{ORG}/link");
        let result =
            signed_revocation_with_live_link(self, &url, machine, bearer, path, body, headers)
                .await;
        link.begin_shutdown();
        shutdown.cancel();
        listener_task.await??;
        link.shutdown().await;
        result
    }
}

async fn signed_revocation_with_live_link(
    fixture: &MachineFixture,
    url: &str,
    machine: &str,
    bearer: &str,
    path: &'static str,
    body: &Value,
    headers: &[(&'static str, String)],
) -> Result<DecisionReply> {
    let client = reqwest::Client::builder().no_proxy().build()?;
    let mut socket = client
        .get(url)
        .bearer_auth(bearer)
        .upgrade()
        .send()
        .await?
        .into_websocket()
        .await?;
    let Some(Ok(Message::Text(welcome))) = socket.next().await else {
        return Err("signed machine did not receive a Welcome".into());
    };
    if !matches!(
        decode_text(&welcome, FrameDirection::ToRouter)?,
        LinkFrame::Data(DataFrame::Welcome { machine_id, .. }) if machine_id.as_str() == machine
    ) {
        return Err("initial Welcome did not identify the machine".into());
    }
    let reply = MachineLifecycleHost::post(fixture, path, body, headers).await?;
    if reply != (200, json!({"result":"revoked"})) {
        return Err(format!("signed revoke did not commit: {reply:?}").into());
    }
    // If the revoke observer is removed, a live link answers this with Pong
    // instead of leaving the test to wait for unsolicited traffic.
    let _ = socket.send(Message::Text("ping".into())).await;
    if !matches!(
        socket.next().await,
        Some(Ok(Message::Close { code, .. }))
            if MachineLinkCloseCode::from_code(code.into()) == Some(MachineLinkCloseCode::Revoked)
    ) {
        return Err("the signed revoke did not close the live machine link with Revoked".into());
    }
    Ok(reply)
}
