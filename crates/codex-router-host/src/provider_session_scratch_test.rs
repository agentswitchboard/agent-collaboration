use super::provider_scratch_preparer;
use std::os::unix::fs::PermissionsExt;

#[tokio::test]
async fn each_create_gets_its_own_private_stamped_folder() {
    let root = tempfile::tempdir().unwrap();
    let scratch_root = root.path().join("scratch");
    let clock = crate::external_provider_runtime::acp_scripted_fixture::manual_clock();
    let prepare = provider_scratch_preparer(scratch_root.clone(), clock);
    let first = prepare().await.unwrap();
    let second = prepare().await.unwrap();
    assert_ne!(first, second);
    for folder in [&first, &second] {
        assert_eq!(folder.parent(), Some(scratch_root.as_path()));
        let name = folder.file_name().unwrap().to_str().unwrap();
        let id = name.strip_prefix("session-").unwrap();
        assert_eq!(uuid::Uuid::parse_str(id).unwrap().get_version_num(), 7);
        let mode = std::fs::metadata(folder).unwrap().permissions().mode();
        assert_eq!(mode & 0o077, 0, "owner-private");
        assert!(
            folder
                .join(codex_native_integration::SCRATCH_LAST_USED_FILE_NAME)
                .is_file(),
            "stamped as used, so the sweep keeps it"
        );
    }
}
