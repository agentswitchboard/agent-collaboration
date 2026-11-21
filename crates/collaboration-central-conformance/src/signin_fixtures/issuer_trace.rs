//! Observable issuer retention over the actual private provision and digest routes.
use super::{
    IssuerOrder, IssuerReply, NOW_SECONDS, TrustTraceHost, claims, digest, first_statement,
    issuer_order::await_fetch, published_keys, published_keys_except, set_claim, sign, stamp,
    trust_trace::expect_reply,
};
use crate::ConformanceResult;
use serde_json::json;

#[derive(Clone, Copy, Debug)]
pub enum ScriptedKeyReply {
    FirstKey,
    BothKeys,
    Malformed,
    Transient,
}

impl ScriptedKeyReply {
    fn wire(self) -> ConformanceResult<IssuerReply> {
        Ok(match self {
            Self::FirstKey => {
                IssuerReply::Document(published_keys_except(&["stamp-b".to_owned()])?)
            }
            Self::BothKeys => IssuerReply::Document(published_keys()?),
            Self::Malformed => IssuerReply::Document(json!({})),
            Self::Transient => IssuerReply::Unavailable,
        })
    }

    fn wire_with_max_age(self, max_age_seconds: u64) -> ConformanceResult<IssuerReply> {
        match self {
            Self::FirstKey => Ok(IssuerReply::CacheableDocument {
                document: published_keys_except(&["stamp-b".to_owned()])?,
                max_age_seconds,
            }),
            Self::BothKeys => Ok(IssuerReply::CacheableDocument {
                document: published_keys()?,
                max_age_seconds,
            }),
            Self::Malformed | Self::Transient => self.wire(),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum IssuerProbeKey {
    Stored,
    Successor,
}

#[derive(Clone, Copy, Debug)]
pub struct IssuerProbe {
    pub key: IssuerProbeKey,
    pub advance_ms: i64,
    pub response: ScriptedKeyReply,
}

pub struct IssuerTracePlan {
    pub order: IssuerOrder,
    pub first_receipt_delay_ms: i64,
    pub between_receipts_ms: i64,
    pub advertised_max_age_seconds: u64,
    pub probes: Vec<IssuerProbe>,
}

const FRESH_FOR_MS: i64 = 300_000;

#[derive(Clone, Copy)]
enum PublishedKeys {
    FirstOnly,
    Both,
}

/// The oracle tracks the last successful document and its scheduled receipt.
/// Its only sources are the fixture's scripted replies and injected clock.
#[derive(Default)]
struct IssuerOracle {
    retained: Option<(PublishedKeys, i64)>,
}

impl IssuerOracle {
    fn received(&mut self, reply: ScriptedKeyReply, receipt: i64) {
        let keys = match reply {
            ScriptedKeyReply::FirstKey => PublishedKeys::FirstOnly,
            ScriptedKeyReply::BothKeys => PublishedKeys::Both,
            ScriptedKeyReply::Malformed | ScriptedKeyReply::Transient => return,
        };
        self.retained = Some((keys, receipt));
    }

    fn can_answer(&self, key: IssuerProbeKey, now: i64) -> bool {
        self.retained.is_some_and(|(reply, receipt)| {
            now < receipt + FRESH_FOR_MS
                && matches!(
                    (key, reply),
                    (IssuerProbeKey::Stored, PublishedKeys::FirstOnly) | (_, PublishedKeys::Both)
                )
        })
    }
}

pub async fn prove_issuer_trace(
    host: &impl TrustTraceHost,
    organization: &str,
    plan: &IssuerTracePlan,
) -> ConformanceResult<()> {
    let first = stamp(organization, NOW_SECONDS)?;
    let mut oracle = IssuerOracle::default();
    let (mut now, retained_receipt) = complete_cold_pair(host, &first, plan, &mut oracle).await?;

    // A stale successor signed with B never writes the stamp or invalidates
    // the prepared standing. 409 proves B was trusted, 422 proves it wasn't.
    let unrelated = first_statement(organization, NOW_SECONDS, 1)?;
    let mut candidate = claims(organization, NOW_SECONDS);
    set_claim(&mut candidate, "replaces", json!(digest(&unrelated)))?;
    let stale_successor = sign(
        &json!({"typ":"fleet-organization-stamp+jwt","alg":"EdDSA","kid":"stamp-b"}),
        &candidate,
        8,
    )?;

    let before = host.issuer().fetches();
    let (status, _) = host.revision().await?;
    expect_reply(
        host.issuer(),
        "prepare standing with the retained A key",
        before,
        0,
        status,
        200,
    )?;

    probe(
        host,
        &mut oracle,
        &stale_successor,
        now,
        IssuerProbeKey::Successor,
        ScriptedKeyReply::Malformed,
        "last successful completion controls B membership",
    )
    .await?;
    now = retained_receipt + FRESH_FOR_MS - 1;
    probe(
        host,
        &mut oracle,
        &stale_successor,
        now,
        IssuerProbeKey::Stored,
        ScriptedKeyReply::Transient,
        "receipt boundary minus one millisecond",
    )
    .await?;
    now += 1;
    for response in [
        ScriptedKeyReply::Transient,
        ScriptedKeyReply::Malformed,
        ScriptedKeyReply::FirstKey,
    ] {
        probe(
            host,
            &mut oracle,
            &stale_successor,
            now,
            IssuerProbeKey::Stored,
            response,
            "at receipt expiry, retry until a valid response",
        )
        .await?;
    }

    for (index, step) in plan.probes.iter().enumerate() {
        now += step.advance_ms;
        probe(
            host,
            &mut oracle,
            &stale_successor,
            now,
            step.key,
            step.response,
            format_args!("generated issuer probe {index} ({step:?})"),
        )
        .await?;
    }
    Ok(())
}

async fn complete_cold_pair(
    host: &impl TrustTraceHost,
    first: &str,
    plan: &IssuerTracePlan,
    oracle: &mut IssuerOracle,
) -> ConformanceResult<(i64, i64)> {
    use IssuerOrder::{
        EarlierDispatchCompletesLast, LaterDispatchCompletesLast, MalformedCompletionArrivesLast,
    };
    use ScriptedKeyReply::{BothKeys, FirstKey, Malformed};

    let issuer = host.issuer();
    let before = issuer.fetches();
    let mut earlier_reply = issuer.hold_next_fetch();
    let mut earlier = std::pin::pin!(host.offer(first));
    await_fetch(&mut earlier_reply, earlier.as_mut()).await?;
    let mut later_reply = issuer.hold_next_fetch();
    let mut later = std::pin::pin!(host.offer(first));
    await_fetch(&mut later_reply, later.as_mut()).await?;
    if issuer.fetches() != before + 2 {
        return Err("two cold dispatches must each reach one issuer fetch".into());
    }

    let first_receipt = NOW_SECONDS * 1_000 + plan.first_receipt_delay_ms;
    let last_receipt = first_receipt + plan.between_receipts_ms;
    let (first_response, last_response) = match plan.order {
        EarlierDispatchCompletesLast => (FirstKey, BothKeys),
        LaterDispatchCompletesLast => (BothKeys, FirstKey),
        MalformedCompletionArrivesLast => (BothKeys, Malformed),
    };
    host.set_receipt_time(first_receipt).await?;
    let ((first_response_gate, first_request), (last_response_gate, last_request)) =
        if matches!(plan.order, LaterDispatchCompletesLast) {
            ((earlier_reply, earlier), (later_reply, later))
        } else {
            ((later_reply, later), (earlier_reply, earlier))
        };
    first_response_gate
        .respond(first_response.wire_with_max_age(plan.advertised_max_age_seconds)?)?;
    expect_reply(
        issuer,
        "first cold response",
        before,
        2,
        first_request.await?,
        200,
    )?;
    oracle.received(first_response, first_receipt);

    host.set_receipt_time(last_receipt).await?;
    last_response_gate
        .respond(last_response.wire_with_max_age(plan.advertised_max_age_seconds)?)?;
    let last_status = last_request.await?;
    let (expected, retained_receipt) = if matches!(plan.order, MalformedCompletionArrivesLast) {
        (503, first_receipt)
    } else {
        (200, last_receipt)
    };
    expect_reply(
        issuer,
        "last cold response",
        before,
        2,
        last_status,
        expected,
    )?;
    oracle.received(last_response, last_receipt);
    Ok((last_receipt, retained_receipt))
}

async fn probe(
    host: &impl TrustTraceHost,
    oracle: &mut IssuerOracle,
    stale_successor: &str,
    now: i64,
    key: IssuerProbeKey,
    response: ScriptedKeyReply,
    label: impl std::fmt::Display,
) -> ConformanceResult<()> {
    host.set_receipt_time(now).await?;
    let issuer = host.issuer();
    let before = issuer.fetches();
    let expected_fetches = usize::from(!oracle.can_answer(key, now));
    let expected_status = if expected_fetches == 0 {
        match key {
            IssuerProbeKey::Stored => 200,
            IssuerProbeKey::Successor => 409,
        }
    } else {
        issuer.hold_next_fetch().respond(response.wire()?)?;
        oracle.received(response, now);
        match (key, response) {
            (_, ScriptedKeyReply::Malformed | ScriptedKeyReply::Transient) => 503,
            (IssuerProbeKey::Stored, ScriptedKeyReply::FirstKey | ScriptedKeyReply::BothKeys) => {
                200
            }
            (IssuerProbeKey::Successor, ScriptedKeyReply::BothKeys) => 409,
            (IssuerProbeKey::Successor, ScriptedKeyReply::FirstKey) => 422,
        }
    };
    let actual = match key {
        IssuerProbeKey::Stored => host.digest_status().await?,
        IssuerProbeKey::Successor => host.offer(stale_successor).await?,
    };
    expect_reply(
        issuer,
        label,
        before,
        expected_fetches,
        actual,
        expected_status,
    )
}
