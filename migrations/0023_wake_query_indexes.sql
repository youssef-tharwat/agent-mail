-- Match each correlated wake-view predicate without scanning recipient history.
CREATE INDEX coordination_event_supersession
ON coordination_events(recipient,kind,subject,id);

-- Wake subjects are text. Index the existing expressions to retain exact text
-- matching, rather than coercing a subject into a numeric record ID.
CREATE INDEX pending_delivery_subject
ON deliveries(recipient,CAST(message AS TEXT),state);
CREATE INDEX attention_subject
ON attention_occurrences(recipient,CAST(id AS TEXT));

PRAGMA user_version=23;
