# Herdr agent input guard

This companion patch adds a typed, terminal-owned automation target to Herdr.
Recognizing or starting an agent reserves its terminal for agent input. Exit and
shell respawn retain the reservation. Raw pane text, input and keys reject before
writing bytes; direct agent interaction uses `agent prompt` or `agent send-keys`.
Agent Mail already uses the guarded agent API and retains undelivered work.

To deliberately return an exited agent lane to shell automation, read `pane get`
and release the exact observed terminal:

```sh
herdr pane input-target PANE terminal --terminal TERMINAL_ID
```

Release requires that terminal identity, a live idle shell, and no live or
launching agent. `agent` instead of `terminal` reserves an empty lane. Intent
follows terminal identity through layout moves and persists in saved sessions.
Legacy agent metadata conservatively restores agent intent; missing intent
restores as `unassigned` and rejects raw input until explicitly assigned. Deferred
native agent resumes reserve agent input before a runtime attaches. Human TUI input and
direct terminal attachment remain available; this is an automation contract,
not an authorization sandbox. An unpatched server does not enforce it.

## Source and build

The patch applies to `herdrdev/herdr` commit
`a124eed73c1f911ddf89a6ac5b2f7ab70d76f5c2` (package 0.9.3), under the accompanying
Apache 2.0 license. It includes `src/terminal/input_target.rs`; existing published
client codecs and endpoint fixtures are unchanged.

Patch SHA-256:
`1be9657ec03b51d18c24de98eda8e4c6885cef346fab4569e2b33e3e75318f7e`.
Apply it in a separate checkout at that exact revision:

```sh
git apply --check /path/to/herdr-agent-input-guard.patch
git apply /path/to/herdr-agent-input-guard.patch
```

Build with Rust 1.96.1, Zig 0.16.0 and locked dependencies. The release uses
Herdr's existing build identity flags so it is distinguishable from upstream:

```sh
HERDR_BUILD_CHANNEL=agent-input-guard HERDR_BUILD_ID=2 \
  CARGO_BUILD_JOBS=2 cargo build --release --locked --bin herdr
```

The resulting version is `0.9.3-agent-input-guard.2`. This is a custom build,
not an official Herdr release. Installing Agent Mail does not replace Herdr.
Use Herdr's live handoff after validating the candidate with an isolated server;
never stop or kill a user's server to perform this update. Do not hand off to an
unpatched older binary and assume the input guard will remain enforced.

Handoff from Herdr 0.9.1 regenerates terminal handles while preserving pane IDs
and shell processes. Revalidate affected external bindings against the unchanged
native agent session; stale registrations must not be guessed or rebound. The
isolated cross-version smoke check verifies the actual receiving server version,
preserved pane and shell PID, immediate rejection during import, rejection after
agent exit, and successful explicit shell release.

The updated Herdr skill is in [skill/SKILL.md](skill/SKILL.md). It retains the
installed skill's workflow and adds guarded input and explicit-release guidance.
Its optional command instructions apply only when the installed binary provides
`pane input-target`; the upstream stable skill remains unchanged.

## Verification

The final patch passed 3,758 native Rust tests, strict Clippy, formatting,
architecture checks, integration-asset and documentation/release-workflow checks.
Final focused regressions cover all raw paths without writes, stale-identity
release, live-agent release rejection, persistence, CLI validation and actual
agent exit followed by live server handoff. Disabling the guard makes its
regression fail. All 19 CLI specification tests passed after the final CLI change.

One existing maintenance test could not run successfully on Apple Git 2.39.5,
which lacks `git diff --default-prefix` (149 of 150 maintenance tests passed).
That failure was not waived or fixed by changing upstream tests. Windows SDK
qualification was not run. Review used sequential design, correctness and
protocol/persistence passes by one agent, not independent reviews.
