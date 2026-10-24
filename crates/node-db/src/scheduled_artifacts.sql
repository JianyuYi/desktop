-- Widen the common identity check without renaming a table referenced by existing records.
CREATE TEMP TABLE saved_scheduled_artifact_identities AS SELECT * FROM execution_identities;
DROP TABLE execution_identities;
CREATE TABLE execution_identities (
    operation TEXT PRIMARY KEY,
    execution TEXT NOT NULL UNIQUE,
    kind TEXT NOT NULL CHECK(kind IN ('worktree','clone','plugin','agent_session','deliver_revision','deliver_scheduled_artifacts'))
);
INSERT INTO execution_identities SELECT * FROM saved_scheduled_artifact_identities;
DROP TABLE saved_scheduled_artifact_identities;
-- Revision delivery introduced the single terminal-event outbox. Scheduled-artifact uploads reuse
-- it instead of growing a second one, so its rows reference the common identity table rather than
-- one family's record: a row is written in the same transaction as the family's terminal state and
-- is released only by that execution's exact acknowledgement.
CREATE TEMP TABLE saved_terminal_events AS SELECT * FROM revision_outbox;
DROP TABLE revision_outbox;
CREATE TABLE revision_outbox (
    execution TEXT PRIMARY KEY REFERENCES execution_identities(execution),
    event TEXT NOT NULL CHECK(json_valid(event))
);
INSERT INTO revision_outbox SELECT * FROM saved_terminal_events;
DROP TABLE saved_terminal_events;
-- An upload is admitted already started (its permit is checked at admission), so it has no accepted
-- state. It freezes no plan: a restarted Node reports `interrupted` instead of resuming, because
-- Cloud hands a retried upload a new execution.
CREATE TABLE scheduled_artifact_deliveries (
    execution TEXT PRIMARY KEY REFERENCES execution_identities(execution),
    input TEXT NOT NULL CHECK(json_valid(input)),
    state TEXT NOT NULL CHECK(state IN ('running','completed')),
    result TEXT CHECK(result IS NULL OR json_valid(result)),
    CHECK((state='completed') = (result IS NOT NULL))
);
CREATE TRIGGER bind_new_scheduled_artifact AFTER INSERT ON scheduled_artifact_deliveries BEGIN
    INSERT INTO execution_controllers SELECT NEW.execution,controller FROM controller_binding;
END;
