# Managed launch acceptance (v0.6.0)

Tested locally on 2026-09-29 with Codex 0.157.0 and Claude Code 2.1.285.
An isolated project, Mail database and Codex home were used. Global client trust,
permissions and the user's existing Mail groups were not changed.

## Real engineering cycle

1. Registered coordinator, worker and reviewer; assigned a Python duration parser
   to the Codex worker. The worker implemented the parser and eight tests, then
   sent Mail message 1 containing evidence. It did not accept its own task.
2. Clarified the requirement to reject every zero-valued component. Claude reviewed
   the actual files, ran tests, independently reproduced the mismatch, and sent
   message 2. The original passing tests were insufficient for the clarified spec.
3. The coordinator updated the task to version 2 and resolved message 1 atomically.
   The worker process had stopped (the test runner itself also restarted).
4. Reopened the same Codex thread through `agent-mail run worker -- codex resume ID`.
   After the resume attachment fix, Mail woke it without typed input. It recovered
   version 2, corrected implementation/tests/docs, ran nine tests and sent message 3.
5. Resumed Claude with the updated review assignment. It independently rechecked
   the corrected behavior and sent approval in message 4, leaving acceptance to
   the coordinator.
6. Committed the isolated fixture as `c855d5763f24ce3f415d69229819b1982400e722`.
   The coordinator accepted both tasks at that revision and atomically resolved
   messages 3 and 4. Its final context contained zero open tasks and zero pending
   requests. All nine fixture tests passed independently.

This is a small real implementation/review cycle, not a sustained production
campaign or a remote-machine test. The fixture commit is local test evidence,
not a commit in this repository.

## Observations and corrections

- Native Codex rejects `--no-daemon` together with `--remote`; managed interactive
  launches use a private server without that conflicting flag.
- Native hook trust was reviewed once through Codex's normal UI. Configuration
  initially reported `awaiting_hook`; a real hook changed it to `hook_observed`.
  Hook execution is not proof of model consumption or current process health.
- Codex can load a resumed thread before firing a hook. Discovering the sole loaded
  thread on the private backend removes the need for a manual first prompt. An
  empty, ambiguous or paginated thread list is never guessed.
- The initial handoff was repeated once as a queue notification after startup
  recovery. The worker correctly recognized it had already submitted its result.
  This bounded redundant notification remains; startup and transport receipts
  cannot be equated without evidence of consumption.
- There were two recovery payloads in the Codex trace, 5,529 and 5,829 UTF-8 bytes,
  both with the skill, and two queued updates. These are bytes, not token counts.
  No manual identity export, explicit endpoint attach, or typed resume prompt was
  required on the corrected flow. Initial setup, normal hook trust, review policy
  decisions and opening/resuming clients remained operator actions.
- An old recovery sentence encouraged rereading the already-injected skill. It
  was removed. Startup/resume and the first recovery after compaction supply the
  fixed bundled instructions; ordinary tool hooks do not repeat them.
- The developer's login-shell PATH selected an older installed binary during the
  unreleased-build test. The worker used the explicit development binary path.
  Final installation testing must use matching published binaries throughout.
- Claude once tried to read its own outgoing findings using the recipient-only
  `mail show`; Mail correctly refused. This did not block the review or its result.

## Lifecycle and regression coverage

Normal Codex UI exit reaps its private backend. Final PTY checks confirmed
SIGTERM during both backend startup and thread discovery exits 143 and reaps
the owned children. The launcher handles termination
and hangup; discovery remains cancellable. Headless Codex and other commands keep
process replacement behavior. SIGKILL cannot run launcher cleanup.

Regression coverage includes binding/launch isolation, refusing secondary sessions,
persisted delivery pause, no-state `--skill`, startup/compaction instruction recovery,
ambiguous thread discovery, migrations, runtime retry budgets and existing delivery
contracts. The delivery worker is still explicitly started with `service run`.

Validation at the live-test checkpoint: **74 tests and one doctest passed**, strict Clippy
(`--all-targets --all-features -- -D warnings`), formatting, diff whitespace and
skill validation passed. The graph review reported incomplete baseline obligation
evidence; it was used for orientation, not as an all-clear.

The later typed-lifecycle and operating-skill pass was verified with 77 tests
and one doctest, including an isolated CLI assignment/blocker/review/correction/
acceptance flow. The expanded skill is larger than the guide used in the live
trace above; those payload byte measurements do not describe the final guide.
Startup/reset includes the fixed guide plus bounded state; routine updates omit
the guide. No new live-model measurement is claimed for that documentation pass.
