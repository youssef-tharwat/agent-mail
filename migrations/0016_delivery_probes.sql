-- Operational proof, separate from business mail and transport event receipts.
CREATE TABLE delivery_probes (
 recipient INTEGER PRIMARY KEY REFERENCES mailboxes(id) ON DELETE CASCADE,
 binding_version INTEGER NOT NULL,
 route_key TEXT NOT NULL,
 nonce TEXT NOT NULL UNIQUE,
 created INTEGER NOT NULL,
 deadline INTEGER NOT NULL,
 attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts BETWEEN 0 AND 3),
 next_attempt INTEGER NOT NULL DEFAULT 0,
 transport_accepted_at INTEGER,
 runtime_received_at INTEGER,
 acknowledged_at INTEGER,
 healthy_at INTEGER,
 failed INTEGER NOT NULL DEFAULT 0 CHECK(failed IN (0,1))
);
PRAGMA user_version=16;
