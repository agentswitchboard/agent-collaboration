use security_framework::os::macos::keychain::CreateOptions;

use super::*;

fn isolated_keychain() -> (tempfile::TempDir, SecKeychain) {
    let directory = tempfile::tempdir().unwrap();
    let keychain = CreateOptions::new()
        .password("disposable test keychain")
        .create(directory.path().join("central-login.keychain"))
        .unwrap();
    (directory, keychain)
}

#[test]
fn keychain_update_that_ignores_the_expected_value_would_overwrite_a_newer_item() {
    let (_directory, keychain) = isolated_keychain();
    let item = LoginKeychainCredential {
        account: "fixture-issuer|fixture-client".into(),
    };
    assert!(item.load_from(&keychain).unwrap().is_none());
    item.save_in(&keychain, None, b"first").unwrap();
    let predecessor = item.load_from(&keychain).unwrap().unwrap();

    item.save_in(&keychain, Some(&predecessor), b"next")
        .unwrap();

    assert_eq!(
        item.save_in(&keychain, Some(&predecessor), b"stale"),
        Err(FleetCredentialError::Changed)
    );
    assert_eq!(
        item.save_in(&keychain, None, b"stale"),
        Err(FleetCredentialError::Changed)
    );
    assert_eq!(
        item.load_from(&keychain).unwrap().unwrap().as_slice(),
        b"next"
    );
}

#[test]
fn using_the_node_service_would_read_and_change_the_node_item() {
    let (_directory, keychain) = isolated_keychain();
    let account = "fixture-issuer|fixture-client";
    let node_service = "dev.switchboard.fleet-cli";
    keychain
        .set_generic_password(node_service, account, b"node fixture")
        .unwrap();
    let item = LoginKeychainCredential {
        account: account.into(),
    };

    assert!(item.load_from(&keychain).unwrap().is_none());
    item.save_in(&keychain, None, b"rust fixture").unwrap();

    let (node, _) = keychain
        .find_generic_password(node_service, account)
        .unwrap();
    assert_eq!(node.as_ref(), b"node fixture");
    assert_eq!(
        item.load_from(&keychain).unwrap().unwrap().as_slice(),
        b"rust fixture"
    );
}
