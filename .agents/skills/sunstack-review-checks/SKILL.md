---
name: sunstack-review-checks
description: Use when reviewing a codex-router pull request, branch or plan against REVIEW.md, or running its review checks on changed Rust files. Not for verifying behaviour (verify-codex-router).
---

# Sunstack Review Checks

The review checks are nudges: each one says "look here, this may break rule X", and you decide. A nudge is a reason to read the code, never a verdict, and a quiet run is not an approval. The rules, and how to review a change, are in [REVIEW.md](../../../REVIEW.md); read it there rather than from memory. Run everything from the repository root.

## Workflow

1. Read the change and the design it claims to follow. Completion: you can name that design, or say the change names none.
2. IF the work is a plan with no code yet, skip steps 3 and 4: check the plan against the rules in REVIEW.md, and cite each problem by the plan file and its section heading or line. Then report as in step 5.
3. Run the checks on the change's range, with its head checked out:

   ```sh
   pnpm --dir review install --frozen-lockfile
   node review/src/review-checks.ts --base <base> --head <head>
   ```

   With no arguments it checks `git merge-base origin/main HEAD` to `HEAD`. Read the result by its exit code and output:
   - **Exit 0, "nothing to check":** the change has no Rust files under `crates/`, or no rule selects a piece in them. The checks had nothing to say; the review is your reading alone.
   - **Exit 0 with a summary line:** the run completed. Go on to step 4.
   - **Exit 1 with a summary line:** the run is incomplete. Its stderr names each gap, and every unavailable piece has its own line. Go on to step 4, and treat each gap as places to read yourself.
   - **Exit 1 with no summary line:** an unknown revision, a git failure, a checkout that differs from the head, an invalid configuration, or a run that did not complete stopped the checks. Fix the checkout or the revision when that is the cause and rerun; otherwise report stderr.
   - **Exit 2:** the arguments were wrong. Fix them and rerun.

   Completion: the exit code and the summary line or "nothing to check" line are recorded.
4. Read every nudge at its line, with the function or file around it, against its rule in REVIEW.md. Each rule there names the check it maps to.
   - **finding:** confirm or dismiss it, with the reason.
   - **uncertain:** the check could not tell; read the code and decide.
   - **unavailable:** the check could not evaluate that piece; its line gives the place and the reason. Read it yourself against the rule. Run the printed `sunstack lint` command, which makes small paid model requests, only when those pieces matter to the review's decision and your reading can't settle them. Never look for, read or supply a model credential.

   Completion: every nudge and every unavailable piece has your decision and its reason.
5. Report each confirmed finding with its repository-relative `path:line`, its rule, and why. The check output holds absolute local paths: never paste it raw into a public pull request, issue or thread.

The review is not done while any nudge or unavailable piece lacks a decision.
