use super::*;
use switchboard_harness_contracts::{InputId, OwnerIncarnation, SessionGeneration, TurnId};

fn fixture_uuid(last_digit: char) -> Uuid {
    Uuid::parse_str(&format!("00000000-0000-7000-8000-00000000000{last_digit}")).expect("UUIDv7")
}

#[test]
fn request_id_digest_matches_node_json_for_each_scalar_kind() {
    assert_eq!(
        json_sha256(&42).expect("numeric request id"),
        "73475cb40a568e8da8a045ced110137e159f890ac4da883b6b17dc651b3a8049"
    );
    assert_eq!(
        json_sha256(&"42").expect("string request id"),
        "8334c554c7276f59674810b92fff5197cd46bf6ccbe872742f9b04ca31dfe3d1"
    );
}

#[test]
fn authorization_fingerprint_covers_nonce_and_every_field() {
    let nonce = fixture_uuid('1');
    let authorization = DispatchAuthorization {
        incarnation: OwnerIncarnation(fixture_uuid('2')),
        run: SessionGeneration::from_number(3),
        input_id: InputId::new("opaque-input").expect("fixture input"),
        authorization_seq: 4,
        target: AuthorizationTarget::ExistingTurn {
            turn: TurnId(fixture_uuid('5')),
        },
    };
    let expected = "5a8d36fd197a10107c4125da182a915c788ac4268f552377c61f72bde5387030";
    assert_eq!(
        authorization_fingerprint(nonce, &authorization).expect("fingerprint"),
        expected
    );
    let different = [
        DispatchAuthorization {
            incarnation: OwnerIncarnation(fixture_uuid('6')),
            ..authorization.clone()
        },
        DispatchAuthorization {
            run: SessionGeneration::from_number(6),
            ..authorization.clone()
        },
        DispatchAuthorization {
            input_id: InputId::new("other-input").expect("fixture input"),
            ..authorization.clone()
        },
        DispatchAuthorization {
            authorization_seq: 6,
            ..authorization.clone()
        },
        DispatchAuthorization {
            target: AuthorizationTarget::Session,
            ..authorization.clone()
        },
        DispatchAuthorization {
            target: AuthorizationTarget::ExistingTurn {
                turn: TurnId(fixture_uuid('6')),
            },
            ..authorization.clone()
        },
    ];
    for changed in different {
        assert_ne!(
            authorization_fingerprint(nonce, &changed).expect("changed fingerprint"),
            expected
        );
    }
    assert_ne!(
        authorization_fingerprint(fixture_uuid('6'), &authorization).expect("changed nonce"),
        expected
    );
}

#[test]
fn witness_appends_private_records_without_identifiers_or_message_bodies() {
    let directory = tempfile::tempdir().expect("witness directory");
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).expect("private root");
    let config = WitnessConfig {
        directory: directory.path().into(),
        nonce: fixture_uuid('1'),
    };
    let authorization = DispatchAuthorization {
        incarnation: OwnerIncarnation(fixture_uuid('2')),
        run: SessionGeneration::from_number(3),
        input_id: InputId::new("opaque-input").expect("fixture input"),
        authorization_seq: 4,
        target: AuthorizationTarget::NewTurn,
    };
    config.record(
        &authorization,
        WorkWitnessEvent::Offered {
            work_kind: WorkWitnessKind::Prompt,
        },
    );
    config.record(
        &authorization,
        WorkWitnessEvent::FrameId {
            method: "session/prompt",
            rpc_id_sha256: json_sha256(&"raw-fixture-id").expect("digest"),
        },
    );
    let path = directory.path().join(BINDING_FILE);
    assert!(is_private_file(
        &fs::metadata(&path).expect("witness metadata")
    ));
    let bytes = fs::read_to_string(path).expect("witness bytes");
    assert!(!bytes.contains("raw-fixture-id"));
    assert!(!bytes.contains("opaque-input"));
    let rows = bytes
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("witness row"))
        .collect::<Vec<_>>();
    let [offered, frame] = rows.as_slice() else {
        panic!("exactly two witness rows");
    };
    assert_eq!(offered["event"], "offered");
    assert_eq!(offered["workKind"], "prompt");
    assert_eq!(frame["event"], "frameId");
    assert_eq!(frame["method"], "session/prompt");
    assert_eq!(
        offered["authorizationFingerprint"],
        frame["authorizationFingerprint"]
    );
}
