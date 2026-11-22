use super::{OfferStamp, ReplayStanding, StampEffect, StampMachine, StampRefusal};
use crate::organization_authority::{
    StoredOrganizationStamp, StoredStampRead, VerifiedFleetOrganizationStatement,
    fleet_statement_test_support::{
        claims, header, signed_statement, verified_claims, verified_fixture,
        verified_foreign_organization, verified_next_key,
    },
};
use fleet_runtime_ports::SyncTime;
use serde_json::json;
use state_machine_core::{StateMachine, TransitionError};

struct FixtureStamp {
    compact: String,
    verified: VerifiedFleetOrganizationStatement,
}
impl std::ops::Deref for FixtureStamp {
    type Target = VerifiedFleetOrganizationStatement;
    fn deref(&self) -> &Self::Target {
        &self.verified
    }
}
fn stored_stamp_from_fixture(stamp: &FixtureStamp) -> StoredOrganizationStamp {
    StoredOrganizationStamp::new(
        stamp.verified.organization_id().clone(),
        stamp.compact.clone(),
    )
}
async fn first_stamp() -> FixtureStamp {
    FixtureStamp {
        compact: signed_statement(&header(), &claims()),
        verified: verified_fixture().await,
    }
}
async fn signed_fixture(input: serde_json::Value) -> FixtureStamp {
    FixtureStamp {
        compact: signed_statement(&header(), &input),
        verified: verified_claims(input).await,
    }
}

#[derive(Debug, Eq, PartialEq)]
enum StampOfferDecision {
    Initialize,
    AlreadyProvisioned,
    Replace,
}

fn offer_fixture_stamp(
    retained: Option<&FixtureStamp>,
    incoming: &FixtureStamp,
) -> Result<StampOfferDecision, StampRefusal> {
    offer_with_standing(
        retained,
        incoming,
        ReplayStanding::TrustedAt(SyncTime::from_unix_millis(1_800_000_000_000)),
    )
}

#[expect(
    clippy::panic_in_result_fn,
    reason = "Unexpected reducer states fail the oracle."
)]
fn offer_with_standing(
    retained: Option<&FixtureStamp>,
    incoming: &FixtureStamp,
    replay: ReplayStanding,
) -> Result<StampOfferDecision, StampRefusal> {
    let state = retained.map_or(StoredStampRead::Unprovisioned, |prior| {
        StoredStampRead::Stamped(stored_stamp_from_fixture(prior))
    });
    match StampMachine::transition(
        &state,
        OfferStamp {
            verified: &incoming.verified,
            compact: &incoming.compact,
            replay,
        },
    ) {
        Ok((next, effects)) => match effects.as_slice() {
            [StampEffect::RepairReplay] => {
                assert_eq!(next, state);
                Ok(StampOfferDecision::AlreadyProvisioned)
            }
            [StampEffect::InitializeStamp] => {
                assert_eq!(
                    next,
                    StoredStampRead::Stamped(stored_stamp_from_fixture(incoming))
                );
                Ok(StampOfferDecision::Initialize)
            }
            [StampEffect::ReplaceStamp { predecessor }] => {
                assert_eq!(state, StoredStampRead::Stamped(predecessor.clone()));
                assert_eq!(
                    next,
                    StoredStampRead::Stamped(stored_stamp_from_fixture(incoming))
                );
                Ok(StampOfferDecision::Replace)
            }
            other => panic!("unexpected effects: {other:?}"),
        },
        Err(TransitionError::Domain(refusal)) => Err(refusal),
        Err(other) => panic!("unexpected refusal: {other:?}"),
    }
}

async fn replacement_for(
    stored: &VerifiedFleetOrganizationStatement,
    issued_at: i64,
) -> FixtureStamp {
    let mut input = claims();
    input["iat"] = json!(issued_at);
    input["replaces"] = json!(stored.digest().as_str());
    signed_fixture(input).await
}

#[tokio::test]
async fn fleet_replacement_initializes_only_without_a_signed_predecessor() {
    let first = first_stamp().await;
    let replacement = replacement_for(&first, 1_799_999_950).await;

    assert_eq!(
        offer_fixture_stamp(None, &first),
        Ok(StampOfferDecision::Initialize)
    );
    assert_eq!(
        offer_fixture_stamp(None, &replacement),
        Err(StampRefusal::StaleOrganizationStatement)
    );
}

#[tokio::test]
async fn fleet_replacement_accepts_the_matching_signed_predecessor() {
    let first = first_stamp().await;
    let replacement = replacement_for(&first, 1_799_999_950).await;

    let decision = offer_fixture_stamp(Some(&first), &replacement);

    assert_eq!(decision, Ok(StampOfferDecision::Replace));
}

#[tokio::test]
async fn fleet_replacement_keeps_a_setup_retry_after_a_lost_reply() {
    let first = first_stamp().await;
    let mut retry_claims = claims();
    retry_claims["iat"] = json!(1_799_999_951);
    let retry = signed_fixture(retry_claims).await;

    let decision = offer_fixture_stamp(Some(&first), &retry);

    assert_eq!(decision, Ok(StampOfferDecision::AlreadyProvisioned));
    assert_ne!(first.digest(), retry.digest());
}

#[tokio::test]
async fn fleet_replacement_clock_correction_cannot_rebase_a_delayed_candidate() {
    let first = first_stamp().await;
    let clock_ahead = replacement_for(&first, 1_799_999_980).await;
    let clock_corrected = replacement_for(&first, 1_799_999_950).await;
    assert!(clock_ahead.issued_at() > clock_corrected.issued_at());

    let first_decision = offer_fixture_stamp(Some(&first), &clock_corrected);
    let delayed_decision = offer_fixture_stamp(Some(&clock_corrected), &clock_ahead);

    assert_eq!(first_decision, Ok(StampOfferDecision::Replace));
    assert_eq!(
        delayed_decision,
        Err(StampRefusal::StaleOrganizationStatement)
    );
}

#[tokio::test]
async fn fleet_replacement_two_candidates_for_one_predecessor_have_one_decision_winner() {
    let first = first_stamp().await;
    let winner = replacement_for(&first, 1_799_999_950).await;
    let other = replacement_for(&first, 1_799_999_951).await;

    let admitted = offer_fixture_stamp(Some(&first), &winner);
    // Model the next serialized transaction reading the already committed winner.
    let next = offer_fixture_stamp(Some(&winner), &other);

    assert_eq!(admitted, Ok(StampOfferDecision::Replace));
    assert_eq!(next, Err(StampRefusal::StaleOrganizationStatement));
    // This is pure current-state policy; it does not claim real concurrent SQL serialization.
}

#[tokio::test]
async fn fleet_replacement_original_setup_replay_preserves_the_replacement() {
    let original = first_stamp().await;
    let mut input = claims();
    input["replaces"] = json!(original.digest().as_str());
    let mut next_header = header();
    next_header["kid"] = json!("next-stamp-fixture");
    let stored = FixtureStamp {
        compact: signed_statement(&next_header, &input),
        verified: verified_next_key(input).await,
    };
    assert_ne!(original.key_id(), stored.key_id());
    let before_digest = stored.digest().clone();
    let before_key = stored.key_id().to_owned();

    let decision = offer_fixture_stamp(Some(&stored), &original);

    assert_eq!(decision, Ok(StampOfferDecision::AlreadyProvisioned));
    assert_eq!(stored.digest(), &before_digest);
    assert_eq!(stored.key_id(), before_key);
    // The decision cannot mutate a record count: no storage or apply operation exists here.
}

#[tokio::test]
async fn fleet_replacement_never_changes_the_stored_organization() {
    let stored = first_stamp().await;
    let mut foreign_claims = claims();
    foreign_claims["sub"] =
        json!(crate::organization_authority::fleet_statement_test_support::FOREIGN_ORGANIZATION);
    foreign_claims["organization_id"] = foreign_claims["sub"].clone();
    let foreign = FixtureStamp {
        compact: signed_statement(&header(), &foreign_claims),
        verified: verified_foreign_organization().await,
    };

    let decision = offer_fixture_stamp(Some(&stored), &foreign);

    assert_eq!(decision, Err(StampRefusal::OrganizationMismatch));
}

#[tokio::test]
async fn fleet_replacement_byte_identical_replay_of_a_renewal_answers_without_a_swap() {
    // Wrong build caught: a lost-reply retry of a committed renewal is refused as
    // stale (its replaces names the predecessor, not the stored statement itself).
    let first = first_stamp().await;
    let renewal = replacement_for(&first, 1_799_999_950).await;

    let replay = offer_fixture_stamp(Some(&renewal), &renewal);

    assert_eq!(replay, Ok(StampOfferDecision::AlreadyProvisioned));
}

#[hegel::test(test_cases = 256)]
fn offers_follow_the_independent_retained_stamp_trace(case: hegel::TestCase) {
    use hegel::generators as gs;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let first = runtime.block_on(first_stamp());
    let successor = runtime.block_on(replacement_for(&first, 1_799_999_950));
    let competing = runtime.block_on(replacement_for(&first, 1_799_999_951));
    let stamps = [&first, &successor, &competing];
    let mut retained = None;
    for _ in 0..case.draw(gs::integers::<usize>().min_value(1).max_value(32)) {
        let candidate = case.draw(gs::integers::<usize>().max_value(2));
        let replay_allowed = case.draw(gs::booleans());
        let replay = if replay_allowed {
            ReplayStanding::TrustedAt(SyncTime::from_unix_millis(1_800_000_000_000))
        } else {
            ReplayStanding::Refused
        };
        // Label 0 is the only first-style offer. Labels 1 and 2 both name it.
        let (expected, next) = match (retained, candidate) {
            (None, 0) => (Ok(StampOfferDecision::Initialize), Some(0)),
            (Some(0), 1 | 2) => (Ok(StampOfferDecision::Replace), Some(candidate)),
            (Some(current), offer) if replay_allowed && (offer == 0 || offer == current) => {
                (Ok(StampOfferDecision::AlreadyProvisioned), retained)
            }
            _ => (Err(StampRefusal::StaleOrganizationStatement), retained),
        };
        let current = retained.and_then(|index: usize| stamps.get(index).copied());
        let offered = stamps.get(candidate).unwrap();
        assert_eq!(
            offer_with_standing(current, offered, replay),
            expected,
            "retained={retained:?}, offer={candidate}"
        );
        retained = next;
    }
}

#[test]
fn the_stamp_table_covers_absent_retained_replays_swaps_and_refusals() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let first = runtime.block_on(first_stamp());
    let successor = runtime.block_on(replacement_for(&first, 1_799_999_950));
    let competing = runtime.block_on(replacement_for(&first, 1_799_999_951));
    let states = [
        StoredStampRead::Unprovisioned,
        StoredStampRead::Stamped(stored_stamp_from_fixture(&first)),
        StoredStampRead::Stamped(stored_stamp_from_fixture(&successor)),
    ];
    let events = [&first, &successor, &competing].map(|stamp| OfferStamp {
        verified: &stamp.verified,
        compact: &stamp.compact,
        replay: ReplayStanding::TrustedAt(SyncTime::from_unix_millis(1_800_000_000_000)),
    });
    state_machine_core::check_state_machine_transitions::<StampMachine<'_>>(
        states.iter().flat_map(|state| {
            events
                .iter()
                .cloned()
                .map(move |event| (state.clone(), event))
        }),
    )
    .unwrap();
}

#[tokio::test]
async fn only_a_replay_depends_on_the_retained_expiry() {
    let first = first_stamp().await;
    let expires = first.expires_at().0 * 1_000;
    let at_retained_expiry = ReplayStanding::TrustedAt(SyncTime::from_unix_millis(expires));
    let just_before = ReplayStanding::TrustedAt(SyncTime::from_unix_millis(expires - 1));

    assert_eq!(
        offer_with_standing(Some(&first), &first, just_before),
        Ok(StampOfferDecision::AlreadyProvisioned)
    );
    assert_eq!(
        offer_with_standing(Some(&first), &first, at_retained_expiry),
        Err(StampRefusal::StaleOrganizationStatement)
    );

    let mut successor = claims();
    successor["replaces"] = json!(first.digest().as_str());
    successor["exp"] = json!(first.expires_at().0 + 60);
    let successor = signed_fixture(successor).await;
    assert_eq!(
        offer_with_standing(Some(&first), &successor, at_retained_expiry),
        Ok(StampOfferDecision::Replace)
    );
}
