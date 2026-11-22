//! Bounded sequence enumeration over the real owning routes and revoke seam.
//! This supplements, rather than replaces, each named BA1 edge scenario.
use super::support::*;

struct SequenceState {
    generation: i64,
    active: Option<String>,
}
async fn assert_invariant(f: &MachineFixture, state: &SequenceState, step: &str) -> Result<()> {
    let active = f
        .rows(
            "SELECT approval_id FROM login_machine_approvals WHERE state='enrolled' ORDER BY approval_id",
        )
        .await?;
    assert!(
        active.len() <= 1,
        "{step}: more than one enrolled login for the machine_id"
    );
    let expected = state
        .active
        .as_ref()
        .map(|id| vec![vec![text(id)]])
        .unwrap_or_default();
    assert_eq!(
        active, expected,
        "{step}: owning active enrollment differs from sequence model"
    );
    assert_eq!(
        f.rows("SELECT enrollment_generation FROM login_machines")
            .await?,
        vec![vec![number(state.generation)]],
        "{step}: generation drift"
    );
    Ok(())
}
async fn stale_grant(f: &MachineFixture, enrollment: &str, state: &SequenceState) -> Result<()> {
    let rows = f
        .rows(
            "SELECT approval_id,base_generation,state FROM login_machine_approvals ORDER BY approval_id",
        )
        .await?;
    let row = rows
        .iter()
        .find(|row| row.first() == Some(&text(enrollment)))
        .ok_or("stale approval absent")?;
    assert_ne!(
        row.get(1),
        Some(&number(state.generation)),
        "the scenario must actually cross a generation before a pending Grant"
    );
    assert_eq!(
        row.get(2),
        Some(&text("approved")),
        "stale path must remain a never-activated approval"
    );
    let before = snapshot(f).await?;
    assert_eq!(
        f.post(TOKEN, &token(enrollment, enrollment)).await?,
        (200, refused("superseded"))
    );
    assert!(
        snapshot(f).await? == before,
        "a stale pending approval mutated state"
    );
    assert_invariant(f, state, "stale pending grant").await
}

#[expect(
    clippy::panic_in_result_fn,
    reason = "Every bounded sequence step compares real SQL state with an independent generation/active-login model."
)]
#[tokio::test]
async fn bounded_three_login_sequences_preserve_one_enrolled_and_fence_every_stale_base_generation()
-> Result<()> {
    // Both first-winner orders cross three real operation interleavings after
    // replacement: stale pending Grant, old-client repeat and owner revocation.
    // No random source, retry or timing search is used.
    const ORDERS: [[u8; 3]; 3] = [[0, 1, 2], [0, 2, 1], [2, 1, 0]];
    for first_slot in [0_u16, 1] {
        for order in ORDERS {
            let f = MachineFixture::new().await?;
            let stale_slot = 1 - first_slot;
            let new_slot = 2;
            let mut state = SequenceState {
                generation: 0,
                active: None,
            };
            let mut approved = Vec::new();
            for slot in [0_u16, 1] {
                let answer = f
                    .post(
                        DEVICE,
                        &approval(
                            0xd100 + slot,
                            INSTALLATION,
                            &format!("Login {slot}"),
                            &[WORKSPACE],
                            f.now(),
                        ),
                    )
                    .await?;
                let enrollment_id = enrollment(&answer.1);
                approved.push((slot, enrollment_id));
                assert_invariant(&f, &state, "initial approval").await?;
            }
            let first = approved
                .iter()
                .find(|(slot, _)| *slot == first_slot)
                .unwrap()
                .1
                .clone();
            let stale = approved
                .iter()
                .find(|(slot, _)| *slot == stale_slot)
                .unwrap()
                .1
                .clone();
            assert_eq!(
                f.post(TOKEN, &token(&first, &first)).await?,
                (200, grant(&first, INSTALLATION, f.now(), None))
            );
            state.generation = 1;
            state.active = Some(first.clone());
            assert_invariant(&f, &state, "first activation").await?;
            assert_eq!(
                f.post(TOKEN, &token(&first, &first)).await?,
                (200, grant(&first, INSTALLATION, f.now(), None))
            );
            assert_invariant(&f, &state, "active repeat").await?;
            stale_grant(&f, &stale, &state).await?;
            let newer = enrollment(
                &f.post(
                    DEVICE,
                    &approval(
                        0xd100 + new_slot,
                        INSTALLATION,
                        &format!("Login {new_slot}"),
                        &[WORKSPACE],
                        f.now(),
                    ),
                )
                .await?
                .1,
            );
            assert_invariant(&f, &state, "new approval preserves current login").await?;
            assert_eq!(
                f.post(TOKEN, &token(&newer, &newer)).await?,
                (
                    200,
                    grant(&newer, INSTALLATION, f.now(), Some((&first, &first)))
                )
            );
            state.generation = 2;
            state.active = Some(newer.clone());
            assert_invariant(&f, &state, "replacement activation").await?;
            for action in order {
                match action {
                    0 => stale_grant(&f, &stale, &state).await?,
                    1 => {
                        let before = snapshot(&f).await?;
                        assert_eq!(
                            f.post(TOKEN, &token(&first, &first)).await?,
                            (200, refused("superseded"))
                        );
                        assert!(snapshot(&f).await? == before);
                        assert_invariant(&f, &state, "old active repeat").await?;
                    }
                    _ => {
                        let ended = f
                            .revoke(0xd200 + new_slot, INSTALLATION, &newer)
                            .await??
                            .ok_or("modeled active revoke returned none")?;
                        assert_eq!(
                            serde_json::to_value(ended)?,
                            json!({"enrollmentId":newer,"clientId":newer})
                        );
                        state.generation = 3;
                        state.active = None;
                        assert_invariant(&f, &state, "owner revocation").await?;
                    }
                }
            }
            stale_grant(&f, &stale, &state).await?;
            let before = snapshot(&f).await?;
            assert_eq!(
                f.post(TOKEN, &token(&newer, &newer)).await?,
                (200, refused("enrollmentNotActive"))
            );
            assert!(snapshot(&f).await? == before);
            assert_invariant(&f, &state, "revoked repeat").await?;
            let replay = f
                .post(
                    DEVICE,
                    &approval(
                        0xd100 + stale_slot,
                        INSTALLATION,
                        "changed submitted snapshot",
                        &[WORKSPACE_TWO],
                        f.now(),
                    ),
                )
                .await?;
            assert_eq!(
                replay,
                (
                    200,
                    json!({"result":"refused","reason":{"kind":"requestConflict"}})
                )
            );
            assert!(snapshot(&f).await? == before);
            assert_invariant(&f, &state, "approval replay after ABA").await?;
            f.close().await?;
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum EndEnrollment {
    OwnerRevoke,
    IdleExpiry,
}

async fn assert_allowance_pairs(
    fixture: &MachineFixture,
    expected_pairs: &[(&str, &str)],
    active_records: i64,
    withdrawn_records: i64,
) -> Result<()> {
    let expected: Vec<_> = expected_pairs
        .iter()
        .map(|(workspace, state)| vec![text(workspace), text(state)])
        .collect();
    let rows = fixture
        .rows("SELECT workspace_id,state FROM login_machine_allowances")
        .await?;
    assert_eq!(rows.len(), expected.len());
    for pair in expected {
        assert!(
            rows.contains(&pair),
            "missing expected allowance pair {pair:?} in {rows:?}"
        );
    }
    assert_eq!(
        fixture
            .rows("SELECT count(*) FILTER (WHERE allowance_state='active'),count(*) FILTER (WHERE allowance_state='withdrawn') FROM signin_change_records WHERE kind='machineAllowanceChanged'")
            .await?,
        vec![vec![number(active_records), number(withdrawn_records)]]
    );
    Ok(())
}

#[tokio::test]
async fn generated_allowance_traces_survive_reopen_and_terminalization() -> Result<()> {
    use fleet_machine_login::policy::IDLE_EXPIRY_SECONDS;

    for (selected, omitted) in [(WORKSPACE, WORKSPACE_TWO), (WORKSPACE_TWO, WORKSPACE)] {
        for end in [EndEnrollment::OwnerRevoke, EndEnrollment::IdleExpiry] {
            let mut f = MachineFixture::new().await?;
            let selected_approval =
                approval(0xe001, INSTALLATION, "Selected", &[selected], f.now());
            let (status, approved) = f.post(DEVICE, &selected_approval).await?;
            assert_eq!(status, 200);
            let active = enrollment(&approved)?;
            assert_allowance_pairs(&f, &[(selected, "active")], 1, 0).await?;
            assert_eq!(f.post(DEVICE, &selected_approval).await?, (200, approved));
            assert_allowance_pairs(&f, &[(selected, "active")], 1, 0).await?;

            assert_eq!(
                f.post(TOKEN, &token(&active, &active)).await?,
                (200, grant(&active, INSTALLATION, f.now(), None))
            );
            let other = approval(0xe002, INSTALLATION, "Omitted", &[omitted], f.now());
            assert_eq!(f.post(DEVICE, &other).await?.0, 200);
            assert_allowance_pairs(&f, &[(selected, "active"), (omitted, "active")], 2, 0).await?;
            f.reopen().await?;
            assert_allowance_pairs(&f, &[(selected, "active"), (omitted, "active")], 2, 0).await?;

            match end {
                EndEnrollment::OwnerRevoke => {
                    let ended = f
                        .revoke(0xe100, INSTALLATION, &active)
                        .await??
                        .ok_or("the owner did not revoke the current enrollment")?;
                    assert_eq!(
                        serde_json::to_value(ended)?,
                        json!({"enrollmentId":active,"clientId":active})
                    );
                }
                EndEnrollment::IdleExpiry => {
                    f.advance(u64::try_from(IDLE_EXPIRY_SECONDS * 1000)?)
                        .await?;
                    assert_eq!(
                        f.post(TOKEN, &token(&active, &active)).await?,
                        (200, refused("idleExpired"))
                    );
                }
            }
            assert_allowance_pairs(&f, &[(selected, "withdrawn"), (omitted, "withdrawn")], 2, 2)
                .await?;
            assert_eq!(
                f.post(TOKEN, &token(&active, &active)).await?,
                (200, refused("enrollmentNotActive"))
            );
            assert_allowance_pairs(&f, &[(selected, "withdrawn"), (omitted, "withdrawn")], 2, 2)
                .await?;

            let replacement = approval(0xe003, INSTALLATION, "Replacement", &[selected], f.now());
            let (_, answer) = f.post(DEVICE, &replacement).await?;
            let replacement = enrollment(&answer)?;
            assert_allowance_pairs(&f, &[(selected, "active"), (omitted, "withdrawn")], 3, 2)
                .await?;
            assert_eq!(
                f.post(TOKEN, &token(&replacement, &replacement)).await?,
                (200, grant(&replacement, INSTALLATION, f.now(), None))
            );
            assert_allowance_pairs(&f, &[(selected, "active"), (omitted, "withdrawn")], 3, 2)
                .await?;
            f.close().await?;
        }
    }
    Ok(())
}
