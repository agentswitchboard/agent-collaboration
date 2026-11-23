//! Keep the manually chosen public wire roots and the generated types in sync.

use fleet_identity_protocol::{
    BindFleetSession, BindFleetSessionReply, CreateFleetSession, CreateFleetSessionReply,
    EndFleetSession, EndFleetSessionReply,
};
use specta::Types;
use std::{fs, path::Path};

#[test]
fn session_lifecycle_types_match_generated_typescript() {
    let roots = Types::default()
        .register::<CreateFleetSession>()
        .register::<CreateFleetSessionReply>()
        .register::<BindFleetSession>()
        .register::<BindFleetSessionReply>()
        .register::<EndFleetSession>()
        .register::<EndFleetSessionReply>();
    let exported = specta_typescript::Typescript::default()
        .export(&roots, specta_serde::PhasesFormat)
        .expect("export session lifecycle wire types");
    let generated = exported
        .lines()
        .map(str::trim_end)
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    let artifact = Path::new(env!("CARGO_MANIFEST_DIR")).join("contracts/session-lifecycle.ts");
    if std::env::var("UPDATE_SESSION_LIFECYCLE_TYPES").as_deref() == Ok("1") {
        fs::write(&artifact, &generated).expect("write session lifecycle types");
    }
    assert_eq!(
        fs::read_to_string(artifact).expect("read session lifecycle types"),
        generated,
    );
}
