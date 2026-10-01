# Artifact evidence

Artifacts identify evidence without claiming that it was reviewed or accepted.
SQLite stores bounded metadata and links. Agent Mail stores managed payloads in
`artifacts/objects/<group-hash>/<sha256>` inside its private state directory.
Original bytes determine identity; producers, artifact IDs and provenance stay
separate, so repeated witnesses share storage within one group.

Create a JSON draft with an ID, location, optional digest/media type/size and
provenance:

```json
{"id":"build-log-42","location":{"kind":"managed"},"digest":null,"media_type":"text/plain","size":null,"provenance":"CI run 42, compile step"}
```

Use `artifact ingest --file draft.json --input build.log` to import exact bytes,
`artifact fetch build-log-42 --output copied.log` to retrieve them, and
`artifact check build-log-42` to report access or integrity. Fetch verifies into
a disk spool before returning bytes. Retrieval never executes content.

External resources use `{"kind":"external","uri":"https://host/object"}`.
Repository resources include `kind: repository`, `repository`, `revision`, and a
relative `path`. Repository and external references need explicit remote URIs;
embedded user information, queries and fragments are refused to prevent credential
storage. This local implementation reports a concrete unavailable result for
external transports. It preserves exact repository/revision/path or URI and any
recorded digest, and does not claim remote durability or successful verification.
Explicitly retrieve using the appropriate authorized transport, then ingest the
bytes with the expected digest and size to verify that import.

Existing task string evidence remains readable and unchanged. A typed
`{"kind":"legacy","reference":"/sender/local/log"}` keeps an old reference
without pretending that its path is accessible on another machine. Legacy checks
report unsupported verification. Conversion into managed evidence requires an
explicit ingest and verifies the provided bytes.

`artifact link ID --file link.json` attaches evidence to a task, message, or exact
record revision. Examples:

```json
{"kind":"task","id":"review-build"}
{"kind":"message","id":42}
{"kind":"record_revision","id":"contract","version":3}
```

The task's current writer, the message's sender, or the record's designated writer
controls these links. Record revision links are retained permanently. Task evidence
cannot be unlinked once that task has an accepted revision in its history.
Producer-controlled retention pins also protect objects. Metadata mutations run on
the group's home machine; stale actor generations are rejected. Recovery returns
at most 16 metadata references per task; retrieval is explicit. Metadata pages have
at most 100 entries. References do not copy payloads into task/history/mail rows.

Metadata and links synchronize through home-authorized, monotonically versioned
snapshots. Message links use global UUIDs on the wire, preserving identity when
local numeric message IDs differ. Snapshot retries neither duplicate metadata nor
revert newer links or pins. Managed payload bytes remain local: a replacement
machine can identify an exact digest and gets an explicit unavailable result until
it imports the matching bytes. Import preserves the original producer and metadata.

## Storage and retention

Whole objects use SHA-256 over original bytes and codec version `identity-v1` or
`zstd-v1`. Inputs below 4096 bytes remain uncompressed. Larger inputs use Zstandard
level 3 only when it saves at least 10%. Digests and original/stored lengths are
checked after decoding; unsupported codecs, corruption, truncation, oversized
payloads and digest mismatches fail visibly. Decoder windows are capped at 8 MiB.
I/O uses 64 KiB buffers and disk spools rather than holding a whole witness in RAM.
Default decoded and stored limits are 256 MiB per object, temporary workspace is
512 MiB, and persistent quota is 1 GiB per group. The Rust API accepts explicit
`ArtifactLimits` for smaller budgets. Quota failure refuses new evidence without
evicting protected objects.

One cross-process file lock covers local uploads, publication, links, pins, reads,
backup and pruning. Identical concurrent ingests create one object; readers protect
objects until verification and output finish. Durable object publication and parent
directory sync happen before readable metadata commits. Interrupted publication
leaves a temporary file or an orphan that maintenance can reclaim. Pruning commits
unavailability before deleting bytes, so interruption can leave an orphan without
exposing partial content. Replica pruning is refused because incoming home metadata
may need its local objects.

`artifact stats` reports logical referenced bytes, unique original bytes, physical
stored bytes, temporary bytes, reclaimable bytes and quota usage.
`artifact prune --grace SECONDS` is a dry run; `--apply` removes only objects with
no links and no pins. Grace restarts when a link or pin is released. Every prune
records its candidate digests and reclaimed-byte accounting in `artifact_audit`.
Removing one link cannot delete shared bytes needed by another link. Descriptors
remain after pruning and then report unavailable; explicit reingest can restore
bytes under their original identity. Orphan and temporary files are swept only
while the same lock excludes active uploads and reads.

## Backup and restore

`artifact backup DESTINATION` is a local operator command. It produces a consistent
SQLite snapshot, every managed blob in that snapshot, and
`artifact-backup.json`. It verifies every original digest before publishing the
backup directory. External resources contribute metadata only.
`artifact restore SOURCE DESTINATION` verifies the manifest, database, all lengths
and original digests, applies quota limits, and publishes a new destination after
validation. Restoring SQLite alone is explicitly refused as an incomplete artifact
backup. Restore never replaces an existing destination.

## Validation and measurement

Run `cargo test --test artifacts` for concurrent deduplication, round trips,
corruption, codec/size/quota failures, interrupted input, link/prune races, retained
witnesses, stable lifecycle footprint, actor scope and backup/restore validation.
Run `cargo test --test artifacts artifact_capacity_measurement -- --ignored
--nocapture` for representative logs, JSON, diffs, tiny witnesses, compressed
content and binary data, including repeated concurrent witnesses. It prints disk
accounting and per-workload ingest/fetch/backup/restore elapsed time. Measurements
below are local observations rather than performance guarantees.

`artifact links ID --limit 100` returns portable targets and an optional
`next_cursor`; pass that cursor to `--after` for another page. Cursors bind the
artifact, group, actor and identity generation. Record and message inspection
return at most 16 artifact metadata entries. Message evidence is visible only to
its sender and delivery recipients; it does not expose another mailbox's mail.

Prune output accounts for tracked `digests`, `orphan_digests`, and
`temporary_files`, with the physical byte total across all three. Dry runs use the
same lock, reference checks and grace period as applied deletion, including stale
upload and verification spools. An applied prune records `prune-planned` before
committing metadata changes and `prune-completed` after deleting and syncing the
listed files. An interruption can leave the planned audit without a completion;
its remaining orphan bytes appear in the next dry run. Stats include unreferenced
tracked files and orphan/spool files in reclaimable bytes, while active readers
and uploads hold the lock.

### Measured fixture run

A local ARM64 container with 4 CPUs and 6 GiB RAM ran the debug build with
Rust 1.95. The benchmark ingested six synthetic fixtures, retrieved each exact
payload, then concurrently ingested two additional copies of the log fixture.
These measurements describe this fixture run, rather than production throughput.

| Fixture | Original bytes | Ingest (ms) | Fetch (ms) |
| --- | ---: | ---: | ---: |
| Text logs | 1,170,000 | 65.42 | 48.26 |
| JSON | 1,200,000 | 54.40 | 47.13 |
| Diffs | 1,120,000 | 57.25 | 44.54 |
| Small witness | 19 | 10.63 | 0.60 |
| Already compressed content | 149 | 8.82 | 1.02 |
| Incompressible binary | 1,000,000 | 51.34 | 41.11 |

The eight artifact references represented 6,830,168 logical bytes and 4,490,168
unique original bytes. Managed payloads occupied 1,000,650 physical bytes, with
zero temporary bytes remaining. Compression saved 77.7% against unique original
bytes; compression and deduplication together saved 85.3% against logical bytes.
These totals measure payload storage and exclude SQLite metadata and its journal.

The complete benchmark took 1.15 seconds wall time, 0.97 seconds user CPU time and
0.05 seconds system CPU time, with peak resident memory of 19,372 KiB. Verified
backup took 214.33 ms and verified restore took 196.59 ms. Backup and restore
included the consistent metadata snapshot and all six unique managed objects.
