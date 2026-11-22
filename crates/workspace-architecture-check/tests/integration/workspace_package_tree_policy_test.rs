use super::parse_cargo_tree_features;
use std::collections::BTreeSet;

#[test]
fn feature_parser_excludes_the_styled_repeat_marker() {
    let expected = BTreeSet::from(["alloc", "default", "derive", "serde_derive", "std"]);

    for feature_column in [
        "alloc,default,derive,serde_derive,std \x1b[33m\x1b[2m(*)\x1b[39m\x1b[22m",
        "alloc,default,derive,serde_derive,std\t \x1b[33m\x1b[2m(*)\x1b[39m\x1b[22m",
        "alloc,default,derive,serde_derive,std (*)",
        "alloc,default,derive,serde_derive,std\t",
    ] {
        assert_eq!(
            parse_cargo_tree_features(feature_column),
            expected,
            "feature column {feature_column:?}",
        );
    }

    for feature_column in [
        " \x1b[33m\x1b[2m(*)\x1b[39m\x1b[22m",
        "\t \x1b[33m\x1b[2m(*)\x1b[39m\x1b[22m",
        " (*)",
        "\t",
        "",
    ] {
        assert_eq!(
            parse_cargo_tree_features(feature_column),
            BTreeSet::new(),
            "empty feature column {feature_column:?}",
        );
    }
}
