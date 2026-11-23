//! Common assertions for bounded timed work on each real Central host.
use std::future::Future;

pub const OVERFLOW_POPULATION: u32 = 1_200;
const FOUR_BATCH_ITEMS: usize = 1_000;

pub trait DueDrainWitness {
    type Key: std::fmt::Debug + PartialEq;

    /// Advance the injected clock and await the real alarm or completion path.
    fn drive_first_mark(&self) -> impl Future<Output = ()>;
    fn drive_next_mark(&self) -> impl Future<Output = ()>;
    fn read_pending_alarm(&self) -> impl Future<Output = Option<i64>>;
    fn read_effect_count(&self) -> impl Future<Output = i64>;
    fn read_finished_keys(&self) -> impl Future<Output = Vec<Self::Key>>;
}

pub async fn capped_drain_hands_its_tail_to_the_next_mark<W: DueDrainWitness>(
    witness: &W,
    due_in_order: &[W::Key],
    fallback_mark: i64,
) {
    assert_eq!(due_in_order.len(), OVERFLOW_POPULATION as usize);
    let started = std::time::Instant::now();
    witness.drive_first_mark().await;
    println!(
        "four-pass local drain: {:.3} ms",
        started.elapsed().as_secs_f64() * 1e3
    );
    assert_eq!(
        witness.read_finished_keys().await,
        due_in_order[..FOUR_BATCH_ITEMS]
    );
    assert_eq!(witness.read_effect_count().await, FOUR_BATCH_ITEMS as i64);
    assert_eq!(witness.read_pending_alarm().await, Some(fallback_mark));

    witness.drive_next_mark().await;
    assert_eq!(witness.read_finished_keys().await, due_in_order);
    assert_eq!(
        witness.read_effect_count().await,
        i64::from(OVERFLOW_POPULATION)
    );
    assert_eq!(witness.read_pending_alarm().await, None);
}

#[derive(Clone, Copy, Debug)]
pub enum DueReplyRoute {
    WorkspaceLink,
    MailLink,
    SignInHttp,
}

impl DueReplyRoute {
    pub const ALL: [Self; 3] = [Self::WorkspaceLink, Self::MailLink, Self::SignInHttp];

    pub fn request_key(self) -> &'static str {
        match self {
            Self::WorkspaceLink => "019f0000-0000-7000-8000-00000000db01",
            Self::MailLink => "019f0000-0000-7000-8000-00000000db02",
            Self::SignInHttp => "019f0000-0000-7000-8000-00000000db03",
        }
    }
}

pub async fn reply_arrived_before_first_due_pass<Reply, State, Hold, Release, Settled>(
    reply: Reply,
    state: impl Fn() -> State,
    held: impl Fn() -> Hold,
    release: impl FnOnce() -> Release,
    settled: impl Fn() -> Settled,
) -> crate::ConformanceResult<()>
where
    Reply: Future<Output = crate::ConformanceResult<()>>,
    State: Future<Output = crate::ConformanceResult<String>>,
    Hold: Future<Output = crate::ConformanceResult<bool>>,
    Release: Future<Output = crate::ConformanceResult<()>>,
    Settled: Future<Output = crate::ConformanceResult<bool>>,
{
    reply.await?;
    if state().await? != "waiting" {
        return Err(
            "the first due item had already applied when the client received its reply".into(),
        );
    }
    if !observe_event(held).await? {
        return Err("the host did not start the deferred drain".into());
    }
    if state().await? != "waiting" {
        return Err("due work committed while its first pass was held".into());
    }
    release().await?;
    if !observe_event(settled).await? {
        return Err("the owned drain did not settle after its release".into());
    }
    Ok(())
}

pub async fn observe_event<Check>(check: impl Fn() -> Check) -> crate::ConformanceResult<bool>
where
    Check: Future<Output = crate::ConformanceResult<bool>>,
{
    for _ in 0..512 {
        if check().await? {
            return Ok(true);
        }
    }
    Ok(false)
}

pub async fn competing_reply_between_due_passes<Reply, Hold, Count, Competing, Release>(
    first_reply: Reply,
    held: impl Fn() -> Hold,
    effect_count: impl Fn() -> Count,
    competing_reply: Competing,
    release: impl FnOnce() -> Release,
) -> crate::ConformanceResult<()>
where
    Reply: Future<Output = crate::ConformanceResult<()>>,
    Hold: Future<Output = crate::ConformanceResult<bool>>,
    Count: Future<Output = crate::ConformanceResult<u64>>,
    Competing: Future<Output = crate::ConformanceResult<()>>,
    Release: Future<Output = crate::ConformanceResult<()>>,
{
    first_reply.await?;
    if !observe_event(held).await? || effect_count().await? != 250 {
        return Err("the first batch must commit exactly 250 items before yielding".into());
    }
    competing_reply.await?;
    if effect_count().await? != 250 {
        return Err("the competing event started a parallel due batch".into());
    }
    release().await?;
    if !observe_event(|| async { Ok(effect_count().await? == 600) }).await? {
        return Err("the original wake-up did not finish all 600 items".into());
    }
    Ok(())
}
