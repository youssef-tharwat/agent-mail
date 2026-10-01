#!/usr/bin/env bash
# Inert until de-sponsor supplies a real named grant and exact composed source.
# public-controls grades only the11 controls. full-acceptance retains all missing gates.
set -euo pipefail
umask 077

if [[ $# != 5 || ( $1 != public-controls && $1 != full-acceptance ) ]]; then
  echo 'usage: bash scripts/durable-execution-acceptance.sh MODE GRANT.json SOURCE.json SOURCE.tar HARNESS.json' >&2
  exit 64
fi
[[ $(uname -s) == Linux ]] || { echo 'root-granted Linux runner required' >&2; exit 64; }
for program in jq sha256sum timeout realpath cargo rustc ps awk flock; do
  command -v "$program" >/dev/null || { echo "missing required tool: $program" >&2; exit 69; }
done
mode=$1
grant=$(realpath "$2")
source_manifest=$(realpath "$3")
source_archive=$(realpath "$4")
harness_manifest=$(realpath "$5")
digest() { sha256sum "$1" | cut -d' ' -f1; }

# Grant JSON records an external root allocation. Parsing it cannot allocate capacity.
jq -e --arg mode "$mode" '
  .schema_version == 1 and .issuer == "de-sponsor" and .task == "failure-acceptance"
  and .workload == ("failure-acceptance-" + $mode)
  and (.grant_id | type == "string" and length > 0)
  and (.runner_id | type == "string" and length > 0)
  and (.source_profile | test("^[A-Za-z0-9_.-]+$"))
  and (.runner_workdir | type == "string" and startswith("/"))
  and (.source_workdir | type == "string" and startswith("/"))
  and (.state_dir | test("^/tmp/agent-mail-durable-execution/failure-acceptance/[A-Za-z0-9_-]+$"))
  and (.target_dir | type == "string" and startswith("/"))
  and (.reuse_target | type == "boolean")
  and (.cargo_home | type == "string" and startswith("/"))
  and (.rustup_home | type == "string" and startswith("/"))
  and (.tool_path | type == "string" and length > 0)
  and (.stop_epoch | type == "number" and floor == .)
  and (.delete_epoch | type == "number" and floor == .)
  and .build_jobs == 2 and .native_sessions == 0 and .telemetry == "disabled"
  and ([.source_manifest_sha256,.source_archive_sha256,.harness_manifest_sha256] | all(test("^[a-f0-9]{64}$")))
' "$grant" >/dev/null
workdir=$(jq -r .source_workdir "$grant")
runner_workdir=$(jq -r .runner_workdir "$grant")
state=$(jq -r .state_dir "$grant")
target=$(jq -r .target_dir "$grant")
delete_epoch=$(jq -r .delete_epoch "$grant")
stop_epoch=$(jq -r .stop_epoch "$grant")
effective_stop=$((delete_epoch - 180))
if (( stop_epoch < effective_stop )); then effective_stop=$stop_epoch; fi
grant_id=$(jq -r .grant_id "$grant")
cargo_home=$(jq -r .cargo_home "$grant")
rustup_home=$(jq -r .rustup_home "$grant")
tool_path=$(jq -r .tool_path "$grant")
[[ $(pwd -P) == "$workdir" && $(realpath "$runner_workdir") == "$runner_workdir" ]] || { echo 'canonical grant cwd required' >&2; exit 65; }
[[ $runner_workdir != / && $workdir == "$runner_workdir"/* && $target == "$runner_workdir"/* ]] || { echo 'source/target must be inside the granted runner workdir' >&2; exit 65; }
[[ $target != "$workdir" && ! -L $target && $(realpath -m "$target") == "$target" ]] || { echo 'separate canonical target required' >&2; exit 65; }
[[ ! -e $state && ! -L $state ]] || { echo 'fresh isolated state required; preserve previous runs' >&2; exit 65; }
if [[ $(jq -r .reuse_target "$grant") == true ]]; then
  [[ -d $target ]] || { echo 'granted prepared target missing' >&2; exit 65; }
else
  [[ ! -e $target ]] || { echo 'target reuse needs explicit grant' >&2; exit 65; }
fi
[[ $(digest "$source_manifest") == "$(jq -r .source_manifest_sha256 "$grant")" ]] || { echo 'source manifest mismatch' >&2; exit 65; }
[[ $(digest "$source_archive") == "$(jq -r .source_archive_sha256 "$grant")" ]] || { echo 'source archive mismatch' >&2; exit 65; }
[[ $(digest "$harness_manifest") == "$(jq -r .harness_manifest_sha256 "$grant")" ]] || { echo 'harness manifest mismatch' >&2; exit 65; }
[[ $((effective_stop - $(date +%s))) -gt 30 ]] || { echo 'insufficient time before strict stop boundary' >&2; exit 75; }
exec 9>"$runner_workdir/failure-acceptance-workload.lock"
flock -n 9 || { echo 'acceptance workload already active in this runner workdir' >&2; exit 75; }

verify_map() {
  local manifest=$1 name expected
  jq -e '.files | type == "object" and length > 0' "$manifest" >/dev/null
  while IFS=$'\t' read -r name expected; do
    [[ $name =~ ^[A-Za-z0-9_./-]+$ && $name != /* && $name != *..* && $expected =~ ^[a-f0-9]{64}$ ]] || return 65
    [[ -f $name && ! -L $name && $(realpath "$name") == "$workdir"/* ]] || return 65
    [[ $(digest "$name") == "$expected" ]] || { echo "source mismatch: $name" >&2; return 65; }
  done < <(jq -r '.files | to_entries[] | [.key,.value] | @tsv' "$manifest")
}
verify_map "$source_manifest"
verify_map "$harness_manifest"
# The root composition must contain this exact harness, rather than an unrecorded overlay.
jq -e --slurpfile harness "$harness_manifest" '
  .files as $source | ($harness[0].files | to_entries | length == 4)
  and ($harness[0].files | to_entries | all($source[.key] == .value))
' "$source_manifest" >/dev/null
for required in Cargo.toml Cargo.lock build.rs src/lib.rs tests/failure_acceptance.rs tests/failure_acceptance/support.rs tests/failure_acceptance/scenarios.json scripts/durable-execution-acceptance.sh; do
  jq -e --arg path "$required" '.files[$path] | type == "string"' "$source_manifest" >/dev/null
done
mkdir -p "$(dirname "$state")"
[[ $(realpath "$(dirname "$state")") == /tmp/agent-mail-durable-execution/failure-acceptance ]] || { echo 'state parent symlink refused' >&2; exit 65; }
mkdir "$state"
mkdir -p "$target"
mkdir "$state/fixture-home" "$state/tmp"
cp "$grant" "$state/grant.json"
cp "$source_manifest" "$state/source.json"
cp "$harness_manifest" "$state/harness.json"
cp tests/failure_acceptance/scenarios.json "$state/planned-matrix.json"
jq -n --slurpfile grant "$grant" --slurpfile source "$source_manifest" \
  --arg mode "$mode" --arg wrapper "$(digest scripts/durable-execution-acceptance.sh)" \
  --arg verified "$(date -u +%FT%TZ)" --argjson stop "$effective_stop" '
  $grant[0] as $g | {schema_version:1, mode:$mode, grant_id:$g.grant_id, runner_id:$g.runner_id,
    source_profile:$g.source_profile, source_manifest_sha256:$g.source_manifest_sha256,
    source_archive_sha256:$g.source_archive_sha256,harness_manifest_sha256:$g.harness_manifest_sha256,
    wrapper_sha256:$wrapper,source_workdir:$g.source_workdir,state_dir:$g.state_dir,target_dir:$g.target_dir,
    reused_target:$g.reuse_target,effective_stop_epoch:$stop,delete_epoch:$g.delete_epoch,
    debug_assertions:true,build_jobs:2,telemetry:"disabled",verified_at:$verified,source_files:$source[0].files,
    meaning:"audit provenance after wrapper checks; no runtime/admission/qualification authority"}
' >"$state/run-provenance.json"

finish() {
  local original=$? source_ok=0 harness_ok=0 processes_ok=0 pins_ok=0
  trap - EXIT
  verify_map "$state/source.json" >"$state/source-post.log" 2>&1 || source_ok=$?
  verify_map "$state/harness.json" >"$state/harness-post.log" 2>&1 || harness_ok=$?
  [[ $(digest "$source_manifest") == "$(digest "$state/source.json")" && $(digest "$harness_manifest") == "$(digest "$state/harness.json")" && $(digest "$grant") == "$(digest "$state/grant.json")" ]] || pins_ok=1
  [[ $(digest "$source_archive") == "$(jq -r .source_archive_sha256 "$state/grant.json")" ]] || pins_ok=1
  ps -eo pid=,args= | awk -v prefix="$target/debug/" 'index($2,prefix)==1 { print }' >"$state/remaining-candidate-processes.txt" || processes_ok=$?
  [[ ! -s $state/remaining-candidate-processes.txt ]] || processes_ok=1
  jq -n --argjson exit "$original" --argjson source "$source_ok" --argjson harness "$harness_ok" \
    --argjson pins "$pins_ok" --argjson processes "$processes_ok" --arg finished "$(date -u +%FT%TZ)" \
    '{exit:$exit,source_post_exit:$source,harness_post_exit:$harness,pins_post_exit:$pins,remaining_process_check:$processes,finished:$finished,native_started:false}' >"$state/wrapper-exit.json"
  if [[ $source_ok != 0 || $harness_ok != 0 || $processes_ok != 0 || $pins_ok != 0 ]]; then exit 65; fi
  exit "$original"
}
trap finish EXIT

run_phase() {
  local label=$1 remaining code started
  shift
  verify_map "$state/source.json"
  remaining=$((effective_stop - $(date +%s) - 15))
  [[ $remaining -gt 0 ]] || { echo 'strict stop margin reached' >&2; return 75; }
  started=$(date -u +%FT%TZ)
  printf '%s\0' "$@" | jq -Rs 'split("\u0000")[:-1]' >"$state/$label.argv.json"
  set +e
  timeout --signal=TERM --kill-after=15s "${remaining}s" \
    env -i PATH="$tool_path" HOME="$state/fixture-home" TMPDIR="$state/tmp" \
      CARGO_HOME="$cargo_home" RUSTUP_HOME="$rustup_home" CARGO_TARGET_DIR="$target" \
      CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 RUSTFLAGS='-C debug-assertions=yes' \
      DO_NOT_TRACK=1 OTEL_SDK_DISABLED=true CARGO_TERM_COLOR=never \
      AGENT_MAIL_STATE_DIR="$state" AGENT_MAIL_ACCEPTANCE_GRANT="$grant_id" \
      AGENT_MAIL_ACCEPTANCE_PROVENANCE="$state/run-provenance.json" \
      "$@" >"$state/$label.stdout" 2>"$state/$label.stderr"
  code=$?
  set -e
  jq -n --arg phase "$label" --argjson code "$code" --argjson seconds "$remaining" \
    --arg started "$started" --arg finished "$(date -u +%FT%TZ)" \
    '{phase:$phase,exit:$code,term_timeout_seconds:$seconds,kill_grace_seconds:15,started:$started,finished:$finished}' >"$state/$label.exit.json"
  return "$code"
}
run_phase rust-version rustc -Vv
run_phase build cargo build --offline --locked --bin agent-mail
run_phase public-controls cargo test --offline --locked --test failure_acceptance -- --test-threads=1 --nocapture
report="$state/acceptance-results.json"
[[ -f $report ]] || { echo 'missing complete result matrix' >&2; exit 65; }
jq -e --slurpfile provenance "$state/run-provenance.json" '
  .schema_version == 2 and .run_provenance == $provenance[0]
  and .source_profile == $provenance[0].source_profile
  and .public_controls.verdict == "pass" and .public_controls.executed_checks == 11
  and (.checks | length == 11) and (.checks | all(.verdict == "pass"))
  and (.scenarios | length == 20) and (.crash_cuts | length == 12) and (.native_rows | length == 5)
' "$report" >/dev/null
if [[ $mode == full-acceptance ]]; then
  if ! jq -e '.full_acceptance == "pass" and ([.scenarios[],.crash_cuts[],.native_rows[]] | all(.verdict == "pass"))' "$report" >/dev/null; then
    echo "Full acceptance incomplete; original results: $report" >&2
    exit 3
  fi
else
  echo "Public controls completed; full acceptance is separately incomplete. Results: $report"
fi
