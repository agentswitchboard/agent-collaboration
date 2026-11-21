//! Issuer-key refresh policy uses only the configured source, never JWT claims.
use crate::IssuerKeyCache;
use crate::issuer_key_cache::{IssuerKeySet, IssuerVerificationKey};
use fleet_contracts::{IssuerKeyId, IssuerKeysUnavailable};
use fleet_runtime_ports::{Clock, IssuerKeySource};

pub(crate) async fn resolve_issuer_key<Source: IssuerKeySource>(
    source: &Source,
    cache: &IssuerKeyCache,
    clock: &dyn Clock,
    key_id: &IssuerKeyId,
) -> Result<Option<IssuerVerificationKey>, IssuerKeysUnavailable> {
    if let Some(keys) = cache.fresh(source.configured_source(), receipt_time(clock)?)?
        && let Some(key) = keys.key(key_id)
    {
        return Ok(Some(key.clone()));
    }
    // A cold or forced refresh spends the call's one fetch, even on a miss.
    Ok(fetch_keys(source, cache, clock).await?.key(key_id).cloned())
}

async fn fetch_keys<Source: IssuerKeySource>(
    source: &Source,
    cache: &IssuerKeyCache,
    clock: &dyn Clock,
) -> Result<IssuerKeySet, IssuerKeysUnavailable> {
    let response = source
        .fetch_issuer_keys()
        .await
        .map_err(|reason| IssuerKeysUnavailable::Fetch { reason })?;
    // The TTL starts after the response, on this receiver's injected clock.
    cache.remember(
        source.configured_source().to_owned(),
        response,
        receipt_time(clock)?,
    )
}

fn receipt_time(clock: &dyn Clock) -> Result<chrono::DateTime<chrono::Utc>, IssuerKeysUnavailable> {
    chrono::DateTime::from_timestamp_millis(clock.now().unix_millis())
        .ok_or(IssuerKeysUnavailable::InvalidClock)
}

#[cfg(test)]
#[path = "issuer_key_resolution_test.rs"]
mod tests;
