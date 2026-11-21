use super::support::{Fixture, ORG as ORGANIZATION, REVISIONS, org_revision};

use collaboration_central_conformance::{
    ConformanceResult,
    signin_fixtures::{
        IssuerOrder, IssuerProbe, IssuerProbeKey, IssuerTracePlan, ScriptedKeyReply, StandingProbe,
        StandingSample, TrustTraceHost, prove_issuer_trace, prove_standing_trace,
    },
};
use hegel::{TestCase, generators as gs};
use serde_json::Value;

#[hegel::test(test_cases = 64)]
fn real_host_standing_trace_matches_revision_oracle(case: TestCase) {
    let lifetime = case.draw_named(
        "lifetime_seconds",
        gs::integers::<i64>().min_value(1).max_value(60),
    );
    let count = case.draw_named(
        "clock_samples",
        gs::integers::<usize>().min_value(1).max_value(8),
    );
    let probes: Vec<_> = (0..count)
        .map(|_| StandingProbe {
            offset_from_expiry_ms: case.draw(
                gs::integers::<i64>()
                    .min_value(-lifetime * 1_000 - 1)
                    .max_value(1_000),
            ),
            sample: match case.draw(gs::integers::<u8>().max_value(2)) {
                0 => StandingSample::Warm,
                1 => StandingSample::Reopen,
                _ => StandingSample::ReopenWithUnavailableIssuer,
            },
        })
        .collect();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let mut fixture = Fixture::new().await;
        let outcome = prove_standing_trace(&mut fixture, ORGANIZATION, lifetime, &probes).await;
        fixture.close().await;
        outcome.unwrap();
    });
}

#[hegel::test(test_cases = 64)]
fn real_host_issuer_trace_matches_receipts_and_key_membership(case: TestCase) {
    let order = match case.draw_named("completion_order", gs::integers::<u8>().max_value(2)) {
        0 => IssuerOrder::EarlierDispatchCompletesLast,
        1 => IssuerOrder::LaterDispatchCompletesLast,
        _ => IssuerOrder::MalformedCompletionArrivesLast,
    };
    case.event(format!("completion order {order:?}"));
    let first_receipt_delay_ms = case.draw_named(
        "first_receipt_delay_ms",
        gs::integers::<i64>().min_value(1).max_value(2_000),
    );
    let between_receipts_ms = case.draw_named(
        "between_receipts_ms",
        gs::integers::<i64>().min_value(1).max_value(3_000),
    );
    let count = case.draw_named(
        "additional_probes",
        gs::integers::<usize>().min_value(1).max_value(12),
    );
    let probes = (0..count)
        .map(|_| IssuerProbe {
            key: if case.draw(gs::booleans()) {
                IssuerProbeKey::Stored
            } else {
                IssuerProbeKey::Successor
            },
            advance_ms: case.draw(gs::integers::<i64>().min_value(0).max_value(300_001)),
            response: match case.draw(gs::integers::<u8>().max_value(3)) {
                0 => ScriptedKeyReply::FirstKey,
                1 => ScriptedKeyReply::BothKeys,
                2 => ScriptedKeyReply::Malformed,
                _ => ScriptedKeyReply::Transient,
            },
        })
        .collect();
    let plan = IssuerTracePlan {
        order,
        first_receipt_delay_ms,
        between_receipts_ms,
        advertised_max_age_seconds: case.draw_named(
            "advertised_max_age_seconds",
            gs::integers::<u64>().min_value(301).max_value(3_600),
        ),
        probes,
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let fixture = Fixture::new().await;
        let outcome = prove_issuer_trace(&fixture, ORGANIZATION, &plan).await;
        fixture.close().await;
        outcome.unwrap();
    });
}

impl TrustTraceHost for Fixture {
    async fn revision(&self) -> ConformanceResult<(u16, Value)> {
        Ok(self.post(REVISIONS, &org_revision(3, "active")).await)
    }
    async fn reopen_owner(&mut self) -> ConformanceResult<()> {
        let runtime = self.runtime().await;
        let reset: Result<bool, central_storage::StoreFailure> =
            runtime.control("crash", &()).await;
        if reset.is_ok() {
            return Err("the owning object did not abort for cold reopen".into());
        }
        self.runtime().await;
        Ok(())
    }
}
