import assert from "node:assert/strict";
import { describe, test } from "node:test";

import {
  displayText,
  formatCliFallback,
  formatNudge,
  formatReport,
  formatSummary,
} from "./format-nudges.ts";
import type { ReviewReport } from "./run-review-checks.ts";

describe("formatNudge", () => {
  test("prints a judged finding with its place, rule, message and last judgment", () => {
    const line = formatNudge({
      kind: "finding",
      file: "crates/alpha/src/lib.rs",
      line: 12n,
      rule: "review.time.production_decides_on_system_clock",
      message: "Look here.",
      judgment: { question: "clock_source", probability: 0.912 },
    });

    assert.equal(
      line,
      "crates/alpha/src/lib.rs:12  review.time.production_decides_on_system_clock  Look here.  (p(clock_source)=0.91)",
    );
  });

  test("prints a mechanical finding without a probability", () => {
    const line = formatNudge({
      kind: "finding",
      file: "crates/alpha/src/lib.rs",
      line: 3n,
      rule: "review.example",
      message: "Look here.",
      judgment: undefined,
    });

    assert.equal(line, "crates/alpha/src/lib.rs:3  review.example  Look here.");
  });

  test("tells the reviewer to decide an uncertain result", () => {
    const line = formatNudge({
      kind: "uncertain",
      file: "crates/alpha/src/lib_test.rs",
      line: 40n,
      rule: "review.time.test_file_waits_on_real_time",
      judgment: { question: "waits_on_real_time", probability: 0.2 },
    });

    assert.equal(
      line,
      "crates/alpha/src/lib_test.rs:40  review.time.test_file_waits_on_real_time  uncertain: read the code and decide  (p(waits_on_real_time)=0.20)",
    );
  });

  test("escapes control characters in the displayed path and message", () => {
    const line = formatNudge({
      kind: "finding",
      file: "crates/alpha/src/odd\nname\u001b[2J.rs",
      line: 1n,
      rule: "review.example",
      message: "two\rlines",
      judgment: undefined,
    });

    assert.equal(line, "crates/alpha/src/odd\\nname\\u001b[2J.rs:1  review.example  two\\rlines");
  });
});

describe("displayText", () => {
  test("leaves ordinary text, including non-ASCII, unchanged", () => {
    assert.equal(displayText("crates/café/src/lib.rs"), "crates/café/src/lib.rs");
  });
});

const reportWithUnavailable: ReviewReport = {
  nudges: [],
  unavailable: [
    { file: "crates/alpha/src/a.rs", line: 5n, rule: "review.b", reason: "credential_unavailable" },
    { file: "crates/alpha/src/b.rs", line: 9n, rule: "review.a", reason: "credential_unavailable" },
  ],
  piecesSelected: 2,
  piecesEvaluated: 0,
  modelRequests: 0,
};

describe("formatReport", () => {
  test("lists every unavailable piece with its place, then the CLI fallback once", () => {
    const lines = formatReport(reportWithUnavailable, [
      "/repository/crates/alpha/src/a.rs",
      "/repository/crates/alpha/src/b.rs",
    ]);

    assert.deepEqual(lines, [
      "crates/alpha/src/a.rs:5  review.b  unavailable (credential_unavailable)",
      "crates/alpha/src/b.rs:9  review.a  unavailable (credential_unavailable)",
      "2 pieces were not evaluated. Read them yourself, or run the same checks with the sunstack CLI from the repository root:",
      "  sunstack lint --config review/sunstack.lint.json /repository/crates/alpha/src/a.rs /repository/crates/alpha/src/b.rs",
      "2 pieces selected, 0 evaluated, 2 unavailable, 0 findings, 0 uncertain, 0 model requests",
    ]);
  });

  test("says there was nothing to check when no piece was selected", () => {
    const lines = formatReport({ ...reportWithUnavailable, unavailable: [], piecesSelected: 0 }, [
      "/repository/crates/alpha/src/a.rs",
    ]);

    assert.deepEqual(lines, ["No rule pieces selected in 1 files: nothing to check."]);
  });
});

describe("formatCliFallback", () => {
  test("is empty when every piece was evaluated", () => {
    assert.deepEqual(formatCliFallback([], ["/repository/crates/alpha/src/lib.rs"]), []);
  });

  test("quotes paths so the command stays one copyable line", () => {
    const lines = formatCliFallback(reportWithUnavailable.unavailable, [
      "/repository/crates/odd name/src/lib.rs",
      "/repository/crates/it's/src/lib.rs",
      "/repository/crates/new\nline/src/lib.rs",
    ]);

    assert.equal(
      lines[1],
      "  sunstack lint --config review/sunstack.lint.json '/repository/crates/odd name/src/lib.rs' '/repository/crates/it'\\''s/src/lib.rs' $'/repository/crates/new\\x0aline/src/lib.rs'",
    );
  });
});

describe("formatSummary", () => {
  test("counts selected, evaluated, unavailable, findings and uncertain separately", () => {
    const summary = formatSummary({
      nudges: [
        { kind: "finding", file: "a.rs", line: 1n, rule: "r", message: "m", judgment: undefined },
        { kind: "uncertain", file: "b.rs", line: 2n, rule: "r", judgment: undefined },
        { kind: "uncertain", file: "c.rs", line: 3n, rule: "r", judgment: undefined },
      ],
      unavailable: [{ file: "d.rs", line: 4n, rule: "r", reason: "credential_unavailable" }],
      piecesSelected: 7,
      piecesEvaluated: 6,
      modelRequests: 2,
    });

    assert.equal(
      summary,
      "7 pieces selected, 6 evaluated, 1 unavailable, 1 findings, 2 uncertain, 2 model requests",
    );
  });
});
