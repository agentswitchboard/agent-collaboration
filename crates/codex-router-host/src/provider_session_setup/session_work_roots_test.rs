use super::session_work_roots;
use std::path::{Path, PathBuf};

/// Oracle: R18's session list, "the session's project tmp/ ... any roots its
/// creator names at start": the configured roots, then the project's tmp/,
/// then the named roots, each once.
#[test]
fn a_sessions_roots_add_its_project_tmp_and_named_roots_once() {
    // Arrange
    let configured = vec![PathBuf::from("/a"), PathBuf::from("/b")];
    let named = vec![PathBuf::from("/b"), PathBuf::from("/c")];

    // Act
    let roots = session_work_roots(&configured, Path::new("/project"), &named);

    // Assert
    assert_eq!(
        roots,
        vec![
            PathBuf::from("/a"),
            PathBuf::from("/b"),
            PathBuf::from("/project/tmp"),
            PathBuf::from("/c"),
        ]
    );
}
