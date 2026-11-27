# Review checks

This package runs the review checks on a change's Rust files. The rules
themselves, and how to review a change, are in [REVIEW.md](../REVIEW.md). Each
check is a nudge: it points at a line that may break a rule, and the reviewer
decides.

The script runs the checks through `@sunstack/sdk`. It reads the same rule
files the `sunstack` CLI reads:

| File                      | What it holds                                                                        |
| ------------------------- | ------------------------------------------------------------------------------------ |
| `sunstack.lint.json`      | The configuration: the rulebook, checked files, evidence kinds, model and limits.    |
| `../REVIEW.md`            | The rulebook text. Each rule statement ends with a hidden marker (`R001` and so on). |
| `rules/review.rules.json` | One entry per marker: its evidence kind, questions and decision steps.               |
| `evidence/`               | The ast-grep project each evidence kind selects its pieces with.                     |

Every checked file is a Rust file under `crates/`. The Host crate rule (R004)
reads only the top-level `crates/codex-router-host/src/`, without test files or
`lib.rs`; that scope sits in `evidence/rules/source-file.yml`, because the
configuration has no per-kind file list.

## Running

From the repository root, with the change's head checked out:

```sh
pnpm --dir review install --frozen-lockfile
node review/src/review-checks.ts [--base <rev>] [--head <rev>]
```

The default head is `HEAD` and the default base is
`git merge-base origin/main <head>`. Each revision must be a non-empty value
that is not shaped like an option; each is resolved to a commit on its own, and
the script lists the Rust files under `crates/` that
`git diff --name-only --diff-filter=ACMR <base>...<head> --` names.

The checks read those files from the working tree. Before running, the script
stops if `HEAD` is not the requested head, or if any of those files has staged
or unstaged changes against it.

It prints one line per nudge, `path:line  rule  message`. A judged nudge also
shows the probability of the last question the model answered on its decision
path, as `(p(<question>)=0.82)`; an uncertain one says
`uncertain: read the code and decide`. Each piece the run could not evaluate
gets its own line, `path:line  rule  unavailable (<reason>)`. A summary line
counts the pieces selected, evaluated and unavailable, the findings, the
uncertain results and the model requests. When there are no changed Rust files,
or no rule selects a piece in them, one line says there was nothing to check.
Control characters in paths and messages are shown escaped.

When some pieces were not evaluated, the script also prints, once, the
equivalent `sunstack lint --config review/sunstack.lint.json <files>` command.
Today the SDK cannot reach the model credential from Node, so judged pieces come
back `credential_unavailable` while the CLI evaluates them. The script never
runs the CLI itself.

Exit codes:

| Code | Meaning                                                                                                                                                                       |
| ---- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 0    | The run completed, including with nudges or uncertain results, or there was nothing to check.                                                                                 |
| 1    | An unknown revision, a git failure, a checkout that differs from the head, an invalid configuration, or a run that did not complete (including when pieces were unavailable). |
| 2    | Bad arguments: an unknown option, a positional argument, or a missing, empty or option-shaped revision.                                                                       |

## Developing

```sh
pnpm --dir review run check
pnpm --dir review run lint
pnpm --dir review run format:check
pnpm --dir review run test
sunstack lint --config review/sunstack.lint.json --check
```

The tests make no model requests. They run the configuration on the small Rust
tree in `test-fixtures/` (admission, the pieces each evidence kind selects, and
a mechanical pass on a one-request budget), and drive git and the command's exit
codes in temporary git repositories with a stub in place of the SDK runner.

`pnpm-workspace.yaml` exempts `@sunstack/sdk@0.25.0` and its three native
packages from pnpm's minimum release age, only because 0.25.0 is newer than
that gate and a fresh frozen install refuses it. Remove the exemptions when the
SDK version is bumped or once 0.25.0 ages past the gate.

## Adding a rule

1. Write the rule statement in [REVIEW.md](../REVIEW.md) as one sentence that
   states the rule on its own, ending with the next marker (`<!-- R005 -->`).
2. Add its entry under that marker in `rules/review.rules.json`, and any new
   evidence kind in `sunstack.lint.json` with its ast-grep rule in
   `evidence/rules/`.
3. Add fixture files for its evidence kind and extend the keyless tests.
4. Measure it as [REVIEW.md](../REVIEW.md#adding-a-rule) describes before it is
   trusted: cases first, a check by hand, an independent review, then a
   development run and a held-out run.
5. Run `sunstack lint --config review/sunstack.lint.json --check`.
