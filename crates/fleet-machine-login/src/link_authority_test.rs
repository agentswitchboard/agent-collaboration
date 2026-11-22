use super::*;
use crate::policy::{EnrollmentGeneration, MachineRow, MachineState, StoredEnrollment};

const NOW_MS: i64 = 1_800_000_000_123;

fn uuid_text(suffix: u64) -> String {
    format!("018f1111-2222-7000-8000-{suffix:012x}")
}

fn attachment_with_deadline() -> MachineConnectionAttachment {
    MachineConnectionAttachment::from_token_claims(
        uuid_text(1).try_into().unwrap(),
        uuid_text(3).try_into().unwrap(),
        uuid_text(4).try_into().unwrap(),
        uuid_text(4).try_into().unwrap(),
        UnixSeconds(NOW_MS / 1000 + 600),
    )
}

fn stored_authority(attachment: &MachineConnectionAttachment) -> (MachineRow, StoredEnrollment) {
    let machine = MachineRow {
        machine_id: attachment.machine_id().clone(),
        label: "test machine".to_owned().try_into().unwrap(),
        generation: EnrollmentGeneration::try_from(1).unwrap(),
        state: MachineState::Enrolled,
    };
    let enrollment = StoredEnrollment {
        id: attachment.enrollment_id().clone(),
        machine_id: attachment.machine_id().clone(),
        request_id: uuid_text(6).try_into().unwrap(),
        approved_by: uuid_text(7).try_into().unwrap(),
        label: machine.label.clone(),
        allowed_workspaces: Vec::new().try_into().unwrap(),
        base_generation: EnrollmentGeneration::default(),
        approved_at_ms: NOW_MS - 1000,
        code_expires_at: UnixSeconds(NOW_MS / 1000 + 100),
        state: StoredEnrollmentState::Enrolled {
            client_id: attachment.client_id().clone(),
            last_grant_at: UnixSeconds(NOW_MS / 1000),
        },
        ended_at_ms: None,
    };
    (machine, enrollment)
}

#[test]
fn only_the_live_matched_enrollment_can_use_the_attachment() {
    let attachment = attachment_with_deadline();
    let (machine, enrollment) = stored_authority(&attachment);
    let authority = EnrollmentAuthority::checked(
        Some(machine.clone()),
        Some(enrollment.clone()),
        Some(enrollment.clone()),
    )
    .unwrap();
    let admitted = classify_link(
        &attachment,
        attachment.organization_id(),
        &authority,
        NOW_MS,
    )
    .unwrap()
    .ok()
    .unwrap();
    assert_eq!(admitted.attachment(), attachment);
    let wrong_client = MachineConnectionAttachment::from_token_claims(
        attachment.organization_id().clone(),
        attachment.machine_id().clone(),
        attachment.enrollment_id().clone(),
        uuid_text(9).try_into().unwrap(),
        attachment.effective_expires_at(),
    );
    let refusal = classify_link(
        &wrong_client,
        attachment.organization_id(),
        &authority,
        NOW_MS,
    )
    .unwrap()
    .err()
    .unwrap();
    assert_eq!(refusal.reason, MachineConnectionRefusal::WrongClient {});
    let other_org = uuid_text(9).try_into().unwrap();
    let foreign = classify_link(&attachment, &other_org, &authority, NOW_MS)
        .unwrap()
        .err()
        .unwrap();
    assert_eq!(
        foreign.reason,
        MachineConnectionRefusal::MachineInAnotherOrganization {}
    );
    let absent = EnrollmentAuthority::checked(None, None, None).unwrap();
    let unknown = classify_link(&attachment, attachment.organization_id(), &absent, NOW_MS)
        .unwrap()
        .err()
        .unwrap();
    assert_eq!(unknown.reason, MachineConnectionRefusal::UnknownMachine {});
    for changed in [
        MachineRow {
            generation: machine.generation.next().unwrap(),
            ..machine.clone()
        },
        MachineRow {
            state: MachineState::Revoked,
            ..machine
        },
    ] {
        assert!(matches!(
            EnrollmentAuthority::checked(
                Some(changed),
                Some(enrollment.clone()),
                Some(enrollment.clone())
            ),
            Err(StoreFailure::Corrupt { .. })
        ));
    }
}

#[test]
fn stored_ending_wins_on_both_sides_of_the_original_deadline() {
    let attachment = attachment_with_deadline();
    let (machine, enrollment) = stored_authority(&attachment);
    let authority = EnrollmentAuthority::checked(
        Some(machine.clone()),
        Some(enrollment.clone()),
        Some(enrollment.clone()),
    )
    .unwrap();
    let deadline = attachment.effective_expires_at().0 * 1000;
    assert!(
        classify_link(
            &attachment,
            attachment.organization_id(),
            &authority,
            deadline - 1
        )
        .unwrap()
        .is_ok()
    );
    for now in [deadline, deadline + 1] {
        let expired = classify_link(&attachment, attachment.organization_id(), &authority, now)
            .unwrap()
            .err()
            .unwrap();
        assert_eq!(expired.close, MachineLinkCloseCode::Expired);
    }
    for (state, cause) in [
        (
            StoredEnrollmentState::Revoked {
                client_id: attachment.client_id().clone(),
                reason: EnrollmentRevocationReason::Owner {},
            },
            MachineLinkCloseCode::Revoked,
        ),
        (
            StoredEnrollmentState::Expired {
                client_id: attachment.client_id().clone(),
            },
            MachineLinkCloseCode::Inactive,
        ),
    ] {
        let ended = EnrollmentAuthority::checked(
            Some(MachineRow {
                state: MachineState::Revoked,
                generation: machine.generation.next().unwrap(),
                ..machine.clone()
            }),
            Some(StoredEnrollment {
                state,
                ended_at_ms: Some(NOW_MS),
                ..enrollment.clone()
            }),
            None,
        )
        .unwrap();
        for now in [deadline - 1, deadline, deadline + 1] {
            let terminal = classify_link(&attachment, attachment.organization_id(), &ended, now)
                .unwrap()
                .err()
                .unwrap();
            assert_eq!(terminal.close, cause);
            assert_eq!(
                terminal.ending.unwrap().enrollment_id,
                *attachment.enrollment_id()
            );
        }
    }
}

#[test]
fn persisted_attachments_reject_unknown_fields_missing_identity_and_invalid_expiry() {
    let attachment = attachment_with_deadline();
    let encoded = serde_json::to_value(&attachment).unwrap();
    assert_eq!(encoded.as_object().unwrap().len(), 5);
    let mut extra = encoded.clone();
    extra["permissions"] = serde_json::json!([]);
    assert!(serde_json::from_value::<MachineConnectionAttachment>(extra).is_err());
    let mut missing = encoded.clone();
    missing.as_object_mut().unwrap().remove("enrollmentId");
    assert!(serde_json::from_value::<MachineConnectionAttachment>(missing).is_err());
    let mut non_v7 = encoded;
    non_v7["machineId"] = serde_json::json!("018f1111-2222-4000-8000-000000000003");
    assert!(serde_json::from_value::<MachineConnectionAttachment>(non_v7).is_err());
    for expiry in [-1, i64::MAX] {
        let invalid = MachineConnectionAttachment::from_token_claims(
            attachment.organization_id().clone(),
            attachment.machine_id().clone(),
            attachment.enrollment_id().clone(),
            attachment.client_id().clone(),
            UnixSeconds(expiry),
        );
        assert!(matches!(
            validate_shape(&invalid),
            Err(StoreFailure::Corrupt { .. })
        ));
    }
}
