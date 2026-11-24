use std::{
    ffi::{OsStr, OsString},
    os::unix::ffi::OsStringExt,
};

use super::enrollment_machine_label;

#[test]
fn machine_label_uses_the_hostname_or_the_durable_machine_id() {
    let machine_id = "019a0000-0000-7000-8000-000000000003"
        .parse()
        .expect("fixture machine ID");
    assert_eq!(
        enrollment_machine_label(Some(OsStr::new("  skybook.local  ")), &machine_id),
        "skybook.local"
    );
    let invalid_utf8 = OsString::from_vec(vec![0xff]);
    let oversize = "x".repeat(257);
    for unusable in [
        None,
        Some(OsStr::new("   ")),
        Some(OsStr::new("bad\nname")),
        Some(OsStr::new(&oversize)),
        Some(invalid_utf8.as_os_str()),
    ] {
        assert_eq!(
            enrollment_machine_label(unusable, &machine_id),
            "019a0000-0000-7000-8000-000000000003"
        );
    }
}
