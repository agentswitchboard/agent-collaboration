use super::build_sample_service;

#[test]
fn builds_with_default_settings() {
    let _service = build_sample_service(Default::default());
}
