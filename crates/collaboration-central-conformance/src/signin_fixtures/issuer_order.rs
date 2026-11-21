use super::{
    HeldIssuerFetch, IssuerReply, NOW_SECONDS, SignInKeys, published_keys, published_keys_except,
    stamp, stamp_under_second_key,
};
use crate::ConformanceResult;
use std::future::Future;

pub trait IssuerOrderHost {
    fn issuer(&self) -> &SignInKeys;
    fn set_receipt_time(&self, millis: i64) -> impl Future<Output = ConformanceResult<()>>;
    fn offer(&self, compact: &str) -> impl Future<Output = ConformanceResult<u16>>;
    fn digest_status(&self) -> impl Future<Output = ConformanceResult<u16>>;
}

#[derive(Clone, Copy, Debug)]
pub enum IssuerOrder {
    EarlierDispatchCompletesLast,
    LaterDispatchCompletesLast,
    MalformedCompletionArrivesLast,
}

async fn await_fetch(
    fetch: &mut HeldIssuerFetch,
    primary: std::pin::Pin<&mut impl Future<Output = ConformanceResult<u16>>>,
) -> ConformanceResult<()> {
    tokio::select! {
        arrived = fetch.until_requested() => Ok(arrived?),
        answer = primary => Err(format!("sign-in replied before reaching its held issuer: {answer:?}").into()),
    }
}

pub async fn prove_issuer_completion_order(
    host: &impl IssuerOrderHost,
    organization: &str,
    order: IssuerOrder,
) -> ConformanceResult<()> {
    let first_stamp = stamp(organization, NOW_SECONDS)?;
    let issuer = host.issuer();
    let before = issuer.fetches();

    let mut earlier_reply = issuer.hold_next_fetch();
    let mut earlier = std::pin::pin!(host.offer(&first_stamp));
    await_fetch(&mut earlier_reply, earlier.as_mut()).await?;
    let mut later_reply = issuer.hold_next_fetch();
    let mut later = std::pin::pin!(host.offer(&first_stamp));
    await_fetch(&mut later_reply, later.as_mut()).await?;
    if issuer.fetches() != before + 2 {
        return Err("both dispatches must reach exactly one issuer fetch each".into());
    }

    let first_receipt = NOW_SECONDS * 1_000 + 250;
    let last_receipt = NOW_SECONDS * 1_000 + 1_250;
    let two_keys = IssuerReply::Document(published_keys()?);
    let one_key = IssuerReply::Document(published_keys_except(&["stamp-b".to_owned()])?);
    host.set_receipt_time(first_receipt).await?;
    let (contains_second, receipt) = match order {
        IssuerOrder::EarlierDispatchCompletesLast => {
            later_reply.respond(one_key)?;
            require_status(later.await?, 200)?;
            host.set_receipt_time(last_receipt).await?;
            earlier_reply.respond(two_keys)?;
            require_status(earlier.await?, 200)?;
            (true, last_receipt)
        }
        IssuerOrder::LaterDispatchCompletesLast => {
            earlier_reply.respond(two_keys)?;
            require_status(earlier.await?, 200)?;
            host.set_receipt_time(last_receipt).await?;
            later_reply.respond(one_key)?;
            require_status(later.await?, 200)?;
            (false, last_receipt)
        }
        IssuerOrder::MalformedCompletionArrivesLast => {
            later_reply.respond(two_keys)?;
            require_status(later.await?, 200)?;
            host.set_receipt_time(last_receipt).await?;
            earlier_reply.respond(IssuerReply::Document(serde_json::json!({})))?;
            require_status(earlier.await?, 503)?;
            (true, first_receipt)
        }
    };
    if issuer.fetches() != before + 2 {
        return Err(format!("{order:?} unexpectedly triggered a third fetch").into());
    }

    issuer.fail_fetches();
    let probe = stamp_under_second_key(organization, NOW_SECONDS)?;
    require_status(
        host.offer(&probe).await?,
        if contains_second { 200 } else { 503 },
    )?;
    let after_probe = before + if contains_second { 2 } else { 3 };
    if issuer.fetches() != after_probe {
        return Err(format!("{order:?} retained the wrong set").into());
    }

    // Only the actual receipt of the last successful response starts the TTL.
    host.set_receipt_time(receipt + 299_999).await?;
    require_status(host.digest_status().await?, 200)?;
    if issuer.fetches() != after_probe {
        return Err(
            format!("{order:?} replaced retention before its receipt-based deadline").into(),
        );
    }
    host.set_receipt_time(receipt + 300_000).await?;
    require_status(host.digest_status().await?, 503)?;
    if issuer.fetches() != after_probe + 1 {
        return Err(format!("{order:?} missed the exact receipt-based expiration").into());
    }
    Ok(())
}

fn require_status(actual: u16, expected: u16) -> ConformanceResult<()> {
    if actual == expected {
        Ok(())
    } else {
        Err(format!("expected sign-in status {expected}, received {actual}").into())
    }
}
