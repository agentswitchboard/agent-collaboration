use super::{
    ConnectedLink, Fixture, MACHINE_ID, Result, assert_mac_failure, assert_probe_answer,
    central_probe, invoke_mac, mac_call, reply_to_mac_probe,
};
use central_link_protocol::{MAX_PENDING_TYPED_CALLS, TypedCallResult};
use futures_channel::oneshot;
use futures_util::{StreamExt, stream::FuturesUnordered};
use reqwest::StatusCode;
use std::collections::BTreeMap;

#[tokio::test]
async fn a_full_socket_refuses_without_sending_and_reuses_a_settled_slot() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = exercise_capacity(&fixture).await;
    fixture.close().await?;
    result
}

async fn exercise_capacity(fixture: &Fixture) -> Result<()> {
    const FIRST: u32 = 1000;
    const OVERFLOW: u32 = 2000;
    const FRESH: u32 = 2001;
    let sequences = FIRST..FIRST + u32::try_from(MAX_PENDING_TYPED_CALLS)?;
    let mut link = ConnectedLink::open(fixture).await?;
    let mut pending = FuturesUnordered::new();
    for sequence in sequences.clone() {
        pending.push(async move {
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>((
                sequence,
                invoke_mac(fixture, sequence).await?,
            ))
        });
    }
    let (slot_opened, until_slot_opens) = oneshot::channel();
    let collect = async {
        let mut slot_opened = Some(slot_opened);
        while let Some(answer) = pending.next().await {
            let (sequence, answer) = answer?;
            assert_probe_answer(answer, sequence).await?;
            if sequence == FIRST {
                let _ = slot_opened
                    .take()
                    .ok_or("first result delivered twice")?
                    .send(());
            }
        }
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    };
    let drive = async {
        let mut correlations = BTreeMap::new();
        for _ in sequences.clone() {
            let (correlation, sequence) = mac_call(&mut link.socket).await?;
            assert!(sequences.contains(&sequence));
            assert!(correlations.insert(sequence, correlation).is_none());
        }
        assert_mac_failure(
            invoke_mac(fixture, OVERFLOW).await?,
            StatusCode::CONFLICT,
            "neverReached",
        )
        .await?;
        // The overflow response precedes this barrier. An extra CallMac would be read first.
        assert!(matches!(
            central_probe(&mut link.socket, &MACHINE_ID.parse()?, 99).await?,
            TypedCallResult::Answered { .. }
        ));

        let first = correlations
            .remove(&FIRST)
            .ok_or("capacity did not include the first sequence")?;
        reply_to_mac_probe(&mut link.socket, first.clone(), FIRST).await?;
        until_slot_opens.await?;
        let (_, fresh) = tokio::try_join!(
            async {
                let (correlation, sequence) = mac_call(&mut link.socket).await?;
                assert_eq!(sequence, FRESH);
                assert_ne!(correlation, first);
                assert!(!correlations.values().any(|pending| pending == &correlation));
                reply_to_mac_probe(&mut link.socket, correlation, FRESH).await
            },
            invoke_mac(fixture, FRESH),
        )?;
        assert_probe_answer(fresh, FRESH).await?;

        for (sequence, correlation) in correlations {
            reply_to_mac_probe(&mut link.socket, correlation, sequence).await?;
        }
        Ok(())
    };
    tokio::try_join!(drive, collect)?;
    link.close().await
}
