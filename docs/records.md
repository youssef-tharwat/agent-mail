# Shared records

Shared records hold contracts, briefs, and decisions visible to every registered participant in a group. Each record has a stable group-local ID and one designated writer: its creator. Assignment of a task does not transfer record authority. Every operation validates the caller's current identity generation. Writes run on the group's home machine; home-authored revisions and exact links synchronize to remote group participants.

A record revision contains a title (256 UTF-8 bytes), full text (65,536 UTF-8 bytes), and a recovery summary (512 UTF-8 bytes). JSON-encoded body text also has a 200 KiB transport limit. Updates require the observed revision and a nonempty reason (512 UTF-8 bytes). Each update creates an immutable revision, explicitly names the superseded revision, and advances the current pointer. Corrections preserve the text earlier decisions relied upon. Identical retries from the same identity generation return their original result; different content using the same observed revision fails.

Tasks and messages can link an exact record ID and revision. The current task writer or message sender attaches links, with at most sixteen references per target. A new owner reads the task's references and fetches the pinned revision through its own registered group identity. References report both their pinned revision and the current revision, making later corrections visible without silently changing the governing contract. Message references keep sender/recipient access checks. Reading shared text grants no access to another participant's private mail.

Recovery and listing return summaries and references, with at most six current records and twenty revision summaries per page. Current records page after a stable ID; history pages newest first before a revision. Full text is fetched explicitly. Reads never resolve messages or accept tasks. Large binary evidence belongs in artifact storage.

The storage migration adds tables and immutable revision guards without rewriting existing tasks or mail.

## CLI

Use an authenticated participant session and the selected group:

```sh
agent-mail record create --file contract.json
agent-mail record update contract --file correction.json
agent-mail record show contract --revision 1
agent-mail record list --after contract
agent-mail record history contract --before 20
agent-mail record link --file reference.json
```

Creation JSON:

```json
{"id":"contract","title":"API contract","body":"The full shared contract text","summary":"Frozen API contract"}
```

Correction JSON:

```json
{"revision":1,"title":"API contract","body":"Corrected shared contract text","summary":"Corrected API contract","reason":"Fix an incorrect response field"}
```

Task reference JSON pins the governing revision:

```json
{"target":{"task":"implement-api"},"id":"contract","revision":1}
```

A message reference uses `{"message":42}` as its target. Full record text is separate from the task/mail reference metadata.
