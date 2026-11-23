use super::central_migrator;

/// A non-test-support build returns `migrations/central` unchanged, so that
/// directory must hold exactly 0001–0015. test-support appends only the probe
/// fixture after it, so no probe migration can ship.
#[test]
fn production_history_matches_the_approved_catalog_and_test_support_appends_the_probe() {
    let production = sqlx::migrate!("migrations/central")
        .iter()
        .map(|migration| (migration.version, migration.description.to_string()))
        .collect::<Vec<_>>();
    assert_eq!(
        production
            .iter()
            .map(|(version, _)| *version)
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
        "{production:?}"
    );
    assert!(
        production[3].1.contains("central work attempts"),
        "{production:?}"
    );

    assert_eq!(production[4].1, "engine due views");
    assert_eq!(production[5].1, "signin provisioned for engine");

    assert_eq!(production[6].1, "direct message receipts");
    assert_eq!(production[7].1, "machine allowance audit");
    assert_eq!(production[8].1, "engine due entries");
    assert_eq!(production[9].1, "approval requests");
    assert_eq!(production[10].1, "actor identity");
    assert_eq!(production[11].1, "approval applying time");
    assert_eq!(production[12].1, "approval destination request key");
    assert_eq!(production[13].1, "approval destination chain");
    assert_eq!(production[14].1, "session project workspace");

    let built = central_migrator()
        .iter()
        .map(|migration| (migration.version, migration.description.to_string()))
        .collect::<Vec<_>>();

    #[cfg(feature = "test-support")]
    assert_eq!(
        built,
        production
            .iter()
            .cloned()
            .chain([(16, "engine probe fixture".to_owned())])
            .collect::<Vec<_>>()
    );
    #[cfg(not(feature = "test-support"))]
    assert_eq!(built, production);
}
