use super::super::{
    EnrollmentChange, EnrollmentFacts, MachineState, StoredEnrollment, StoredEnrollmentState,
    policy_fixture::{
        CountingIds, NOW_MS, NOW_S, active_workspaces, approved, assert_refused, decide_token,
        empty_facts, enrolled, facts, id, person, project, records, time, token,
    },
};
use super::decide_machine_token;
use fleet_contracts::{EnrollmentRevocationReason, MachineTokenDecision, MachineTokenRefusal};
use fleet_identity_protocol::UnixSeconds;
use fleet_signin::organization_change::{
    OrganizationChangeKind as Kind, OrganizationChangeOrigin as Origin,
};

#[test]
fn ba1_first_grant_wins_in_either_order_and_never_activates_the_other_approval() {
    for (winner, loser) in [(40, 41), (41, 40)] {
        let facts = facts(approved(winner, 1), Some(enrolled(39, 0, NOW_S - 60)), 1);
        let plan = decide_token(&facts, NOW_MS);
        assert!(
            matches!(&plan.answer, MachineTokenDecision::Grant { superseded: Some(ended), .. } if ended.enrollment_id.as_str() == id(39) && ended.client_id.as_str() == id(39))
        );
        let EnrollmentChange::ReplaceEnrollment(old) = &plan.changes[0] else {
            panic!("end old login first")
        };
        assert_eq!(
            old.state,
            StoredEnrollmentState::Revoked {
                client_id: id(39).try_into().unwrap(),
                reason: EnrollmentRevocationReason::Superseded {}
            }
        );
        assert_eq!(old.ended_at_ms, Some(NOW_MS));
        let next = project(&facts, &plan);
        assert_eq!(next.machine.as_ref().unwrap().generation.get(), 2);
        assert_eq!(active_workspaces(&next), active_workspaces(&facts));
        assert_eq!(
            next.current.as_ref().unwrap().enrollment().id.as_str(),
            id(winner)
        );
        let records = records(&plan);
        assert_eq!(
            records.iter().map(|record| record.kind).collect::<Vec<_>>(),
            vec![Kind::MachineRevoked, Kind::MachineEnrolled]
        );
        assert_ne!(records[0].request_id, records[1].request_id);
        for record in records {
            assert_eq!(
                record.origin,
                Origin::Person {
                    person_id: person().as_str().to_owned(),
                    permission_checked: "machine:enroll".to_owned()
                }
            );
            assert!(
                record
                    .request_id
                    .as_str()
                    .starts_with(facts.enrollment.as_ref().unwrap().request_id.as_str())
            );
        }
        let loser_facts = EnrollmentFacts {
            enrollment: Some(approved(loser, 1)),
            ..next.clone()
        };
        assert_refused(
            &decide_token(&loser_facts, NOW_MS + 1),
            MachineTokenRefusal::Superseded {},
        );
        let old_facts = EnrollmentFacts {
            enrollment: Some(old.clone()),
            ..next
        };
        assert_refused(
            &decide_token(&old_facts, NOW_MS + 1),
            MachineTokenRefusal::Superseded {},
        );
    }
}

#[test]
fn first_and_repeat_grants_use_the_fixed_six_hundred_second_policy() {
    let active = enrolled(40, 0, NOW_S);
    for facts in [
        facts(approved(40, 0), None, 0),
        facts(active.clone(), Some(active), 1),
    ] {
        let plan = decide_token(&facts, NOW_MS);
        let MachineTokenDecision::Grant {
            decided_at,
            grant_expires_at,
            ..
        } = plan.answer
        else {
            panic!("expected a machine Grant")
        };
        assert_eq!(decided_at, UnixSeconds(NOW_S));
        assert_eq!(grant_expires_at, UnixSeconds(NOW_S + 600));
    }
}

#[test]
fn first_ever_grant_has_no_superseded_client_and_active_repeat_only_renews_last_grant() {
    let facts = facts(approved(40, 0), None, 0);
    let plan = decide_token(&facts, NOW_MS);
    assert!(matches!(
        plan.answer,
        MachineTokenDecision::Grant {
            superseded: None,
            ..
        }
    ));
    let next = project(&facts, &plan);
    assert_eq!(next.machine.as_ref().unwrap().generation.get(), 1);
    let renewal = decide_token(&next, NOW_MS + 600_000);
    let row = StoredEnrollment {
        state: StoredEnrollmentState::Enrolled {
            client_id: id(40).try_into().unwrap(),
            last_grant_at: UnixSeconds(NOW_S + 600),
        },
        ..next.enrollment.as_ref().unwrap().clone()
    };
    assert_eq!(
        renewal.changes,
        vec![EnrollmentChange::ReplaceEnrollment(row)]
    );
    assert!(records(&renewal).is_empty());
}

#[test]
fn unredeemed_approval_does_not_displace_the_working_login() {
    let working = enrolled(39, 0, NOW_S - 60);
    let facts = facts(working.clone(), Some(working), 1);
    let plan = decide_token(&facts, NOW_MS);
    assert!(matches!(
        plan.answer,
        MachineTokenDecision::Grant {
            superseded: None,
            ..
        }
    ));
    assert_eq!(plan.changes.len(), 1);
    assert!(matches!(
        plan.changes[0],
        EnrollmentChange::ReplaceEnrollment(_)
    ));
}

#[test]
fn true_code_deadline_lapses_only_the_candidate_at_and_after_the_boundary() {
    let candidate = StoredEnrollment {
        code_expires_at: UnixSeconds(NOW_S),
        ..approved(40, 1)
    };
    let facts = facts(candidate.clone(), Some(enrolled(39, 0, NOW_S - 60)), 1);
    assert!(matches!(
        decide_token(&facts, NOW_S * 1000 - 1).answer,
        MachineTokenDecision::Grant { .. }
    ));
    for now_ms in [NOW_S * 1000, NOW_S * 1000 + 1] {
        let mut request = token(&candidate);
        request.client_id = id(99).try_into().unwrap();
        let plan =
            decide_machine_token(&request, &facts, time(now_ms), &CountingIds::new()).unwrap();
        assert_eq!(
            plan.answer,
            MachineTokenDecision::Refuse {
                reason: MachineTokenRefusal::EnrollmentNotActive {}
            }
        );
        assert_eq!(
            plan.changes,
            vec![EnrollmentChange::ReplaceEnrollment(StoredEnrollment {
                state: StoredEnrollmentState::Lapsed,
                ended_at_ms: Some(now_ms),
                ..candidate.clone()
            })]
        );
        assert!(records(&plan).is_empty());
    }
}

#[test]
fn wrong_client_unknown_org_and_inactive_states_never_mint_or_write() {
    for current in [approved(40, 1), enrolled(40, 0, NOW_S)] {
        let active_id = if matches!(current.state, StoredEnrollmentState::Approved) {
            39
        } else {
            40
        };
        let facts = facts(current.clone(), Some(enrolled(active_id, 0, NOW_S)), 1);
        let mut request = token(&current);
        request.client_id = id(99).try_into().unwrap();
        let ids = CountingIds::new();
        assert_refused(
            &decide_machine_token(&request, &facts, time(NOW_MS), &ids).unwrap(),
            MachineTokenRefusal::WrongClient {},
        );
        request = token(&current);
        request.organization_id = id(2).try_into().unwrap();
        assert_refused(
            &decide_machine_token(&request, &facts, time(NOW_MS), &ids).unwrap(),
            MachineTokenRefusal::EnrollmentNotActive {},
        );
        assert_eq!(ids.minted(), 0);
    }
    let request = token(&approved(40, 0));
    assert_refused(
        &decide_machine_token(&request, &empty_facts(), time(NOW_MS), &CountingIds::new()).unwrap(),
        MachineTokenRefusal::EnrollmentNotActive {},
    );
    for state in [
        StoredEnrollmentState::Lapsed,
        StoredEnrollmentState::Revoked {
            client_id: id(40).try_into().unwrap(),
            reason: EnrollmentRevocationReason::Owner {},
        },
        StoredEnrollmentState::Expired {
            client_id: id(40).try_into().unwrap(),
        },
    ] {
        assert_refused(
            &decide_token(
                &facts(
                    StoredEnrollment {
                        ended_at_ms: Some(NOW_MS - 1),
                        state,
                        ..approved(40, 0)
                    },
                    None,
                    1,
                ),
                NOW_MS,
            ),
            MachineTokenRefusal::EnrollmentNotActive {},
        );
    }
}

#[test]
fn exact_sixty_day_idle_boundary_withdraws_allowances_with_automatic_compound_records() {
    let last = NOW_S - 60 * 24 * 60 * 60;
    let active = enrolled(40, 0, last);
    let facts = facts(active.clone(), Some(active), 1);
    assert!(matches!(
        decide_token(&facts, NOW_S * 1000 - 1).answer,
        MachineTokenDecision::Grant { .. }
    ));
    let plan = decide_token(&facts, NOW_S * 1000);
    assert_eq!(
        plan.answer,
        MachineTokenDecision::Refuse {
            reason: MachineTokenRefusal::IdleExpired {}
        }
    );
    let next = project(&facts, &plan);
    assert_eq!(next.machine.as_ref().unwrap().generation.get(), 2);
    assert_eq!(next.machine.as_ref().unwrap().state, MachineState::Revoked);
    assert!(active_workspaces(&next).is_empty());
    assert_eq!(
        next.enrollment.as_ref().unwrap().state,
        StoredEnrollmentState::Expired {
            client_id: id(40).try_into().unwrap()
        }
    );
    assert_eq!(
        next.enrollment.as_ref().unwrap().ended_at_ms,
        Some(NOW_S * 1000)
    );
    let audit = records(&plan);
    assert_eq!(audit.len(), 1);
    assert_eq!(audit[0].kind, Kind::MachineExpired);
    assert_eq!(audit[0].origin, Origin::Automatic {});
    let withdrawals: Vec<_> = super::super::policy_fixture::allowance_records(&plan).collect();
    assert_eq!(withdrawals.len(), 2);
    for record in withdrawals {
        assert_eq!(
            record.state,
            fleet_signin::organization_change::MachineAllowanceState::Withdrawn
        );
        assert_eq!(
            record.machine_id,
            facts.machine.as_ref().unwrap().machine_id
        );
        assert_eq!(record.origin, Origin::Automatic {});
        assert_eq!(
            record.request_id.as_str(),
            format!("{}:machineAllowanceChanged:{}", id(40), record.workspace_id)
        );
    }
    assert_refused(
        &decide_token(&next, NOW_MS + 1000),
        MachineTokenRefusal::EnrollmentNotActive {},
    );
}

#[test]
fn each_grant_restarts_the_idle_period_using_whole_seconds() {
    let active = enrolled(40, 0, NOW_S);
    let facts = facts(active.clone(), Some(active), 1);
    let renewal_interval = INACTIVITY_ORACLE_SECONDS * 1000 * 2 / 3;
    let plan = decide_token(&facts, NOW_MS + renewal_interval);
    let next = project(&facts, &plan);
    assert!(matches!(
        decide_token(&next, NOW_MS + 2 * renewal_interval).answer,
        MachineTokenDecision::Grant { .. }
    ));
}

#[test]
fn replacement_retry_keeps_the_predecessor_and_checks_live_client_before_returning_it() {
    let before = facts(approved(40, 1), Some(enrolled(39, 0, NOW_S)), 1);
    let first = decide_token(&before, NOW_MS);
    let MachineTokenDecision::Grant {
        superseded: Some(expected),
        ..
    } = first.answer.clone()
    else {
        panic!("replacement must name its predecessor")
    };
    let next = project(&before, &first);
    let retry = decide_token(&next, NOW_MS + 1000);
    assert!(
        matches!(&retry.answer, MachineTokenDecision::Grant { superseded: Some(actual), .. } if actual == &expected)
    );
    assert!(records(&retry).is_empty());
    assert_eq!(retry.changes.len(), 1);
    let mut wrong_client = token(next.enrollment.as_ref().unwrap());
    wrong_client.client_id = id(99).try_into().unwrap();
    assert_refused(
        &decide_machine_token(
            &wrong_client,
            &next,
            time(NOW_MS + 1000),
            &CountingIds::new(),
        )
        .unwrap(),
        MachineTokenRefusal::WrongClient {},
    );
    let no_longer_active = EnrollmentFacts {
        current: None,
        ..next
    };
    assert!(
        decide_machine_token(
            &token(no_longer_active.enrollment.as_ref().unwrap()),
            &no_longer_active,
            time(NOW_MS + 1000),
            &CountingIds::new(),
        )
        .is_err()
    );
}

#[test]
fn idle_expiry_fences_pending_approvals_and_generation_exhaustion_returns_no_plan() {
    let active = enrolled(39, 0, NOW_S - INACTIVITY_ORACLE_SECONDS);
    let before = facts(active.clone(), Some(active), 1);
    let expired = decide_token(&before, NOW_MS);
    let after = project(&before, &expired);
    assert_eq!(after.machine.as_ref().unwrap().generation.get(), 2);
    let pending = EnrollmentFacts {
        enrollment: Some(approved(40, 1)),
        ..after
    };
    assert_refused(
        &decide_token(&pending, NOW_MS),
        MachineTokenRefusal::Superseded {},
    );

    let max = 9_007_199_254_740_991;
    let exhausted_active = enrolled(39, max - 1, NOW_S - 60 * 24 * 60 * 60);
    let exhausted = facts(exhausted_active.clone(), Some(exhausted_active), max);
    assert!(
        decide_machine_token(
            &token(exhausted.enrollment.as_ref().unwrap()),
            &exhausted,
            time(NOW_MS),
            &CountingIds::new()
        )
        .is_err()
    );
}

const INACTIVITY_ORACLE_SECONDS: i64 = chrono::TimeDelta::days(60).num_seconds();
