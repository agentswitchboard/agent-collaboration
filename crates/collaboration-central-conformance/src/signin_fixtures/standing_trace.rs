//! The revision route witnesses standing independently of the digest read.
use super::{
    IssuerReply, NOW_SECONDS, TrustTraceHost, claims, digest, set_claim, sign, stamp_header,
    statement_with_times, trust_trace::expect_reply,
};
use crate::ConformanceResult;
use serde_json::{Value, json};

#[derive(Clone, Copy, Debug)]
pub enum StandingSample {
    Warm,
    Reopen,
    ReopenWithUnavailableIssuer,
}

#[derive(Clone, Copy, Debug)]
pub struct StandingProbe {
    pub offset_from_expiry_ms: i64,
    pub sample: StandingSample,
}

pub async fn prove_standing_trace(
    host: &mut impl TrustTraceHost,
    organization: &str,
    lifetime_seconds: i64,
    generated_probes: &[StandingProbe],
) -> ConformanceResult<()> {
    let start = NOW_SECONDS * 1_000;
    let first_expiry = NOW_SECONDS + lifetime_seconds;
    let expiry = first_expiry * 1_000;
    let first = statement_with_times(organization, NOW_SECONDS, NOW_SECONDS, first_expiry)?;

    // An empty owner is unprovisioned and does not need the issuer.
    expect_revision_reply(host, "empty owner", 503, 0).await?;
    expect_revision_reply(host, "cached unprovisioned", 503, 0).await?;
    expect_stamp_offer_applied(
        host,
        "first stamp invalidates unprovisioned standing",
        &first,
        1,
    )
    .await?;
    expect_revision_reply(host, "first installed stamp at issuance", 200, 0).await?;

    // Keep the same observation while generated samples cross both interval
    // boundaries and move the injected clock in either direction.
    for offset in [
        -1,
        0,
        1,
        -lifetime_seconds * 1_000 - 1,
        -lifetime_seconds * 1_000,
    ] {
        let sample = expiry + offset;
        host.set_receipt_time(sample).await?;
        let expected = if (start..expiry).contains(&sample) {
            200
        } else {
            503
        };
        expect_revision_reply(
            host,
            format_args!("warm standing at expiry {offset:+} ms"),
            expected,
            0,
        )
        .await?;
    }

    check_generated_probes(host, start..expiry, generated_probes).await?;

    // Recheck an expired stamp in a fresh owner. Moving time backwards then
    // distinguishes definitive refusal from the warm interval projection.
    host.set_receipt_time(expiry).await?;
    host.reopen_owner().await?;
    expect_revision_reply(host, "cold definitive refusal at expiry", 503, 1).await?;
    host.set_receipt_time(expiry - 1).await?;
    expect_revision_reply(host, "definitive refusal survives clock rollback", 503, 0).await?;
    let previous = host.issuer_fixture().fetches();
    let status = host.digest_status().await?;
    expect_reply(
        host.issuer_fixture(),
        "digest still returns stored facts",
        previous,
        0,
        status,
        200,
    )?;
    expect_revision_reply(host, "digest did not grant standing", 503, 0).await?;

    let mut successor_claims = claims(organization, NOW_SECONDS);
    set_claim(&mut successor_claims, "iat", json!(NOW_SECONDS))?;
    set_claim(&mut successor_claims, "exp", json!(first_expiry + 60))?;
    set_claim(&mut successor_claims, "replaces", json!(digest(&first)))?;
    let successor = sign(&stamp_header(), &successor_claims, 7)?;
    expect_stamp_offer_applied(
        host,
        "successor invalidates definitive refusal",
        &successor,
        0,
    )
    .await?;
    host.set_receipt_time(expiry).await?;
    expect_revision_reply(host, "replacement interval passes the old expiry", 200, 0).await?;

    // Verification temporarily unavailable leaves the owner unchecked. The
    // next revision must retry the issuer and then retain the verified result.
    host.reopen_owner().await?;
    host.issuer_fixture()
        .hold_next_fetch()
        .send_issuer_reply(IssuerReply::Unavailable)?;
    expect_revision_reply(host, "unavailable verification at cold load", 503, 1).await?;
    expect_revision_reply(
        host,
        "unchecked standing retries after unavailability",
        200,
        1,
    )
    .await?;
    expect_revision_reply(host, "recovered verified standing is reused", 200, 0).await
}

async fn check_generated_probes(
    host: &mut impl TrustTraceHost,
    interval: std::ops::Range<i64>,
    probes: &[StandingProbe],
) -> ConformanceResult<()> {
    // Record the first conclusive observation in the current owner. Sample
    // times span at most 61 seconds, so a successful fetch stays warm.
    let mut checked_at = Some(interval.start);
    for (index, probe) in probes.iter().enumerate() {
        let now = interval.end + probe.offset_from_expiry_ms;
        host.set_receipt_time(now).await?;
        let issuer_available = match probe.sample {
            StandingSample::Warm => true,
            StandingSample::Reopen => {
                host.reopen_owner().await?;
                checked_at = None;
                true
            }
            StandingSample::ReopenWithUnavailableIssuer => {
                host.reopen_owner().await?;
                host.issuer_fixture()
                    .hold_next_fetch()
                    .send_issuer_reply(IssuerReply::Unavailable)?;
                checked_at = None;
                false
            }
        };
        let fetches = usize::from(checked_at.is_none());
        if issuer_available {
            checked_at.get_or_insert(now);
        }
        let trusted = checked_at
            .is_some_and(|checked| interval.contains(&checked) && interval.contains(&now));
        expect_revision_reply(
            host,
            format_args!("generated standing probe {index} ({probe:?})"),
            if trusted { 200 } else { 503 },
            fetches,
        )
        .await?;
    }
    Ok(())
}

async fn expect_revision_reply(
    host: &impl TrustTraceHost,
    label: impl std::fmt::Display,
    expected_status: u16,
    expected_fetches: usize,
) -> ConformanceResult<()> {
    let before = host.issuer_fixture().fetches();
    let (status, body) = host.post_standing_revision().await?;
    expect_reply(
        host.issuer_fixture(),
        &label,
        before,
        expected_fetches,
        status,
        expected_status,
    )?;
    let expected_body = if expected_status == 200 {
        json!({"acknowledgedRevision": 3})
    } else {
        Value::Null
    };
    if body == expected_body {
        Ok(())
    } else {
        Err(format!("{label}: expected revision body {expected_body}, got {body}").into())
    }
}

async fn expect_stamp_offer_applied(
    host: &impl TrustTraceHost,
    label: &str,
    statement: &str,
    expected_fetches: usize,
) -> ConformanceResult<()> {
    let before = host.issuer_fixture().fetches();
    let status = host.offer_signed_stamp(statement).await?;
    expect_reply(
        host.issuer_fixture(),
        label,
        before,
        expected_fetches,
        status,
        200,
    )
}
