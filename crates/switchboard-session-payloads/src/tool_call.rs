//! The requested action and the reported state of an assistant tool call.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize};
use workspace_ids::SessionRef;

use crate::object_reader::{deserialize_object, wire_enum, wire_object};

wire_object! {
    pub struct ToolCall {
        #[serde(skip_serializing_if = "Option::is_none")]
        pub source_name: Option<String>,
        pub kind: ToolKind,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub title: Option<String>,
        pub action: ToolAction,
        pub state: ToolState,
        pub locations: Vec<FileLocation>,
    }
}

wire_enum! {
    pub enum ToolKind {
        Read,
        Edit,
        Delete,
        Move,
        Search,
        Execute,
        Think,
        Fetch,
        Other,
    }
}

wire_enum! {
    pub enum ToolAction {
        NotReported,
        Shell {
            command: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            folder: Option<String>,
        },
        FileChange { changes: Vec<FileChange> },
        FileRead {
            path: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            lines: Option<LineRange>,
        },
        Search { query: String, over: SearchOver },
        Fetch { url: String },
        SubAgent {
            #[serde(skip_serializing_if = "Option::is_none")]
            description: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            child: Option<SessionRef>,
        },
        Other { input: RawInput },
    }
}

wire_object! {
    pub struct FileLocation {
        pub path: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub line: Option<u32>,
    }
}

/// A nonempty range starting at a one-based line number.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LineRange {
    first_line: u32,
    line_count: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LineRangeError {
    FirstLineZero,
    LineCountZero,
}

impl fmt::Display for LineRangeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::FirstLineZero => "firstLine must be at least 1",
            Self::LineCountZero => "lineCount must be at least 1",
        })
    }
}

impl std::error::Error for LineRangeError {}

impl LineRange {
    pub const fn new(first_line: u32, line_count: u32) -> Result<Self, LineRangeError> {
        if first_line == 0 {
            return Err(LineRangeError::FirstLineZero);
        }
        if line_count == 0 {
            return Err(LineRangeError::LineCountZero);
        }
        Ok(Self {
            first_line,
            line_count,
        })
    }

    #[must_use]
    pub const fn first_line(self) -> u32 {
        self.first_line
    }

    #[must_use]
    pub const fn line_count(self) -> u32 {
        self.line_count
    }
}

impl<'de> Deserialize<'de> for LineRange {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        struct Wire {
            first_line: u32,
            line_count: u32,
        }

        let Wire {
            first_line,
            line_count,
        } = deserialize_object(deserializer)?;
        Self::new(first_line, line_count).map_err(serde::de::Error::custom)
    }
}

wire_enum! {
    pub enum SearchOver {
        FileNames,
        FileContents,
        Web,
    }
}

wire_object! {
    pub struct FileChange {
        pub path: String,
        pub change: FileChangeKind,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub diff: Option<FileDiff>,
    }
}

wire_enum! {
    pub enum FileChangeKind {
        Added,
        Changed,
        Removed,
        Moved { from: String },
    }
}

wire_enum! {
    pub enum FileDiff {
        Texts {
            #[serde(skip_serializing_if = "Option::is_none")]
            old: Option<String>,
            new: String,
        },
        UnifiedPatch { patch: String },
    }
}

wire_enum! {
    pub enum RawInput {
        NotReported,
        /// An opaque JSON value, including null. Equality follows serde_json;
        /// numbers outside its exact representation are subject to its numeric limits.
        Value { value: serde_json::Value },
        TooLarge { bytes: u64 },
    }
}

wire_enum! {
    pub enum ToolState {
        Pending,
        Running {
            #[serde(skip_serializing_if = "Option::is_none")]
            output_so_far: Option<BoundedText>,
        },
        Completed {
            #[serde(skip_serializing_if = "Option::is_none")]
            output: Option<BoundedText>,
            #[serde(skip_serializing_if = "Option::is_none")]
            exit_code: Option<i32>,
        },
        Failed {
            #[serde(skip_serializing_if = "Option::is_none")]
            output: Option<BoundedText>,
            #[serde(skip_serializing_if = "Option::is_none")]
            exit_code: Option<i32>,
            #[serde(skip_serializing_if = "Option::is_none")]
            message: Option<BoundedText>,
        },
        Declined {
            #[serde(skip_serializing_if = "Option::is_none")]
            message: Option<BoundedText>,
        },
    }
}

wire_object! {
    /// Text as reported, with a marker when the producer shortened it.
    pub struct BoundedText {
        pub text: String,
        pub cut: bool,
    }
}
