#![expect(
    clippy::expect_used,
    reason = "fixture setup and wire observations must fail at the test site"
)]

mod device_login_journey;
mod enrollment_install_races;
mod scripted_sign_in;
mod secret_surfaces;
