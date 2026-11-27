use switchboard_session_payloads::{
    FileChange, FileChangeKind, FileDiff, LineRange, SearchOver, ToolAction,
};

use super::{assert_wire, target};

#[test]
fn file_read_keeps_optional_line_range() {
    assert_wire(
        r#"{"kind":"fileRead","path":"notes.txt","lines":{"firstLine":4,"lineCount":2}}"#,
        ToolAction::FileRead {
            path: "notes.txt".into(),
            lines: Some(LineRange::new(4, 2).unwrap()),
        },
    );
    assert_wire(
        r#"{"kind":"fileRead","path":"notes.txt"}"#,
        ToolAction::FileRead {
            path: "notes.txt".into(),
            lines: None,
        },
    );
}

#[test]
fn search_identifies_each_place_and_fetch_keeps_the_url() {
    for (wire, over) in [
        (
            r#"{"kind":"search","query":"router","over":{"kind":"fileNames"}}"#,
            SearchOver::FileNames,
        ),
        (
            r#"{"kind":"search","query":"router","over":{"kind":"fileContents"}}"#,
            SearchOver::FileContents,
        ),
        (
            r#"{"kind":"search","query":"router","over":{"kind":"web"}}"#,
            SearchOver::Web,
        ),
    ] {
        assert_wire(
            wire,
            ToolAction::Search {
                query: "router".into(),
                over,
            },
        );
    }
    assert_wire(
        r#"{"kind":"fetch","url":"https://example.invalid/notes"}"#,
        ToolAction::Fetch {
            url: "https://example.invalid/notes".into(),
        },
    );
}

#[test]
fn sub_agent_keeps_an_optional_child_session_pair() {
    assert_wire(
        r#"{"kind":"subAgent","description":"Read notes","child":{"machineId":"01890f2e-7b4c-7cc0-98c4-000000000001","sessionId":"native/thread"}}"#,
        ToolAction::SubAgent {
            description: Some("Read notes".into()),
            child: Some(target()),
        },
    );
    assert_wire(
        r#"{"kind":"subAgent"}"#,
        ToolAction::SubAgent {
            description: None,
            child: None,
        },
    );
}

#[test]
fn file_change_keeps_a_unified_patch_and_a_move_source() {
    assert_wire(
        r#"{"kind":"fileChange","changes":[{"path":"notes.txt","change":{"kind":"changed"},"diff":{"kind":"unifiedPatch","patch":"-before\n+after"}},{"path":"new.txt","change":{"kind":"moved","from":"old.txt"}}]}"#,
        ToolAction::FileChange {
            changes: vec![
                FileChange {
                    path: "notes.txt".into(),
                    change: FileChangeKind::Changed,
                    diff: Some(FileDiff::UnifiedPatch {
                        patch: "-before\n+after".into(),
                    }),
                },
                FileChange {
                    path: "new.txt".into(),
                    change: FileChangeKind::Moved {
                        from: "old.txt".into(),
                    },
                    diff: None,
                },
            ],
        },
    );
}
