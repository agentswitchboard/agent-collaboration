//! Replay the conflicting cutoff case over the signed person list and point read.

use central_storage::SqlValue;
use serde_json::{Value, json};

use crate::ConformanceResult;
use crate::signin_fixtures::{
    Person,
    approval_rows::{AnswerClass, InitialApproval, seed, text, time},
};

use super::request::{list, read};
use super::{PersonApprovalHarness, expect, expect_status};

pub(super) async fn check(
    host: &impl PersonApprovalHarness,
    owner: &Person,
) -> ConformanceResult<()> {
    let ended = time(-5_184_000)?;
    let already_hidden = seed(
        host,
        10,
        AnswerClass::Once,
        InitialApproval::CancelledAt(ended),
    )
    .await?;
    let still_visible = seed(
        host,
        20,
        AnswerClass::Once,
        InitialApproval::CancelledAt(ended + chrono::TimeDelta::milliseconds(1)),
    )
    .await?;
    host.write(
        "UPDATE approval_requests SET erase_after_ms=?1 WHERE request_id=?2",
        vec![
            SqlValue::Integer((time(0)?.timestamp_millis() - 1).try_into()?),
            text(&still_visible),
        ],
    )
    .await?;

    let page = expect_status(list("all", None).send(host, owner).await?, 200)?;
    let page: Value = serde_json::from_slice(&page)?;
    let items = page
        .get("items")
        .and_then(Value::as_array)
        .ok_or("expected approval page")?;
    assert_eq!(items.len(), 1);
    assert_eq!(page.get("next"), Some(&Value::Null));
    assert_eq!(
        items
            .iter()
            .filter_map(|item| item.get("requestId").and_then(Value::as_str))
            .collect::<Vec<_>>(),
        [still_visible.as_str()],
        "the stored cutoff must not hide the approval that ended one millisecond later",
    );
    expect(
        read(&already_hidden).send(host, owner).await?,
        200,
        json!({"kind":"notVisible"}),
    )?;
    let point = expect_status(read(&still_visible).send(host, owner).await?, 200)?;
    let point: Value = serde_json::from_slice(&point)?;
    assert_eq!(
        point.pointer("/view/requestId").and_then(Value::as_str),
        Some(still_visible.as_str())
    );
    Ok(())
}
