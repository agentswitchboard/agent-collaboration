//! The test harness's failure-only deadline for socket reads.

use crate::ConformanceResult;
use futures_util::{Stream, StreamExt};
use std::{future::Future, time::Duration};

/// Independent of the product clock. Nextest's shortest termination budget is 60 seconds.
const FIXTURE_READ_WATCHDOG: Duration = Duration::from_secs(20);

tokio::task_local! {
    static READ_WATCHDOG_LIMIT: Duration;
}

#[derive(Debug)]
pub struct ExpectedFrameMissing(pub(crate) &'static str);

impl std::fmt::Display for ExpectedFrameMissing {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "expected {} before the fixture read deadline",
            self.0
        )
    }
}

impl std::error::Error for ExpectedFrameMissing {}

pub async fn read_fixture_frame<Item>(
    stream: &mut (impl Stream<Item = Item> + Unpin),
    expected: &'static str,
) -> ConformanceResult<Option<Item>> {
    let limit = READ_WATCHDOG_LIMIT
        .try_with(|limit| *limit)
        .unwrap_or(FIXTURE_READ_WATCHDOG);
    tokio::time::timeout(limit, stream.next())
        .await
        .map_err(|_| ExpectedFrameMissing(expected).into())
}

/// Accelerate a withheld-frame control. Positive journeys always use the fixture default.
pub async fn with_withheld_frame_watchdog<Output>(control: impl Future<Output = Output>) -> Output {
    READ_WATCHDOG_LIMIT.scope(Duration::ZERO, control).await
}

pub async fn require_withheld_frame<Output>(
    expected: &'static str,
    control: impl Future<Output = ConformanceResult<Output>>,
) -> ConformanceResult<()> {
    match with_withheld_frame_watchdog(control).await {
        Err(error)
            if error
                .downcast_ref::<ExpectedFrameMissing>()
                .is_some_and(|missing| missing.0 == expected) =>
        {
            Ok(())
        }
        Err(error) => {
            Err(format!("expected the fixture watchdog to name {expected}, got {error}").into())
        }
        Ok(_) => Err(format!("withheld {expected} unexpectedly appeared").into()),
    }
}
