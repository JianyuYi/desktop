use ora_application::{FileChange, RepositoryError};
use ora_domain::WorkflowNodeRunId;
use rusqlite::{OptionalExtension, Transaction, TransactionBehavior, params};

use super::engine_repository_error_from_database;
use crate::repository::RepositoryPool;

/// Serializes node file changes into the payload JSON array shared by complete and fail paths.
pub(super) fn file_changes_json(file_changes: &[FileChange]) -> serde_json::Value {
    serde_json::json!(
        file_changes
            .iter()
            .map(|change| {
                serde_json::json!({
                    "path": change.path,
                    "additions": change.additions,
                    "deletions": change.deletions,
                })
            })
            .collect::<Vec<_>>()
    )
}

/// Merges caller-supplied keys into an existing payload JSON object (empty map if NULL/invalid).
pub(super) fn merge_payload_keys(
    existing: Option<&str>,
    keys: impl IntoIterator<Item = (&'static str, serde_json::Value)>,
) -> Result<String, crate::DatabaseError> {
    let mut map = match existing.and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
    {
        Some(serde_json::Value::Object(map)) => map,
        _ => serde_json::Map::new(),
    };
    for (key, value) in keys {
        map.insert(key.to_string(), value);
    }
    Ok(serde_json::Value::Object(map).to_string())
}

/// Merges completion keys into an existing node-run payload without dropping checkpoint keys.
///
/// `stop_reason` is inserted only when `Some`; `file_changes` only when non-empty. `patch` carries
/// caller-supplied keys — the forced-completion audit — merged in the same transaction so a
/// committed node can never be missing its audit. When there is nothing to insert, the existing
/// payload is returned unchanged so a success cannot turn a checkpoint blob into `NULL`.
pub(super) fn merge_complete_payload(
    existing: Option<String>,
    stop_reason: Option<String>,
    file_changes: Vec<FileChange>,
    patch: Option<serde_json::Value>,
) -> Result<Option<String>, crate::DatabaseError> {
    let patch = match patch {
        Some(serde_json::Value::Object(map)) => map,
        // A scalar or array patch cannot describe payload keys; ignoring it leaves the row's
        // provenance intact instead of replacing the whole object.
        _ => serde_json::Map::new(),
    };
    if stop_reason.is_none() && file_changes.is_empty() && patch.is_empty() {
        return Ok(existing);
    }
    let mut map = payload_object(existing.as_deref());
    if let Some(reason) = stop_reason {
        map.insert("stop_reason".to_string(), serde_json::json!(reason));
    }
    if !file_changes.is_empty() {
        map.insert("file_changes".to_string(), file_changes_json(&file_changes));
    }
    map.extend(patch);
    Ok(Some(serde_json::Value::Object(map).to_string()))
}

/// Decodes a node-run payload into its object form, treating NULL or invalid JSON as empty.
pub(super) fn payload_object(existing: Option<&str>) -> serde_json::Map<String, serde_json::Value> {
    match existing.and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok()) {
        Some(serde_json::Value::Object(map)) => map,
        _ => serde_json::Map::new(),
    }
}

/// Merges `payload.checkpoint` (and optional `checkpoint_error`) onto one node-run row.
///
/// A missing row is provenance-only and succeeds as a no-op so a late write cannot fail the node.
pub(super) fn record_node_checkpoint(
    pool: &RepositoryPool,
    node_run_id: &WorkflowNodeRunId,
    snapshot_id: &str,
    checkpoint: Option<&str>,
    checkpoint_error: Option<&str>,
    now: i64,
) -> Result<(), RepositoryError> {
    pool.with_connection_mut(|connection| {
        let transaction = Transaction::new(connection, TransactionBehavior::Immediate)?;
        let existing = transaction
            .query_row(
                "SELECT payload FROM workflow_node_runs WHERE id = ?1 AND is_deleted = 0",
                params![node_run_id.as_ref()],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()?;
        let Some(existing) = existing else {
            return Ok(());
        };
        let mut keys = vec![
            ("checkpoint", serde_json::json!(checkpoint)),
            ("snapshot_id", serde_json::json!(snapshot_id)),
        ];
        if let Some(error) = checkpoint_error {
            keys.push(("checkpoint_error", serde_json::json!(error)));
        }
        let payload = merge_payload_keys(existing.as_deref(), keys)?;
        transaction.execute(
            "UPDATE workflow_node_runs SET payload = ?2, updated_at = ?3
             WHERE id = ?1 AND is_deleted = 0",
            params![node_run_id.as_ref(), payload, now],
        )?;
        transaction.commit()?;
        Ok(())
    })
    .map_err(engine_repository_error_from_database)
}

/// Merges `payload.injected_failure_context` onto one node-run row.
///
/// A missing row is provenance-only and succeeds as a no-op so a late write cannot fail the node.
pub(super) fn record_node_injected_failure(
    pool: &RepositoryPool,
    node_run_id: &WorkflowNodeRunId,
    text: &str,
) -> Result<(), RepositoryError> {
    pool.with_connection_mut(|connection| {
        let transaction = Transaction::new(connection, TransactionBehavior::Immediate)?;
        let existing = transaction
            .query_row(
                "SELECT payload FROM workflow_node_runs WHERE id = ?1 AND is_deleted = 0",
                params![node_run_id.as_ref()],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()?;
        let Some(existing) = existing else {
            return Ok(());
        };
        let payload = merge_payload_keys(
            existing.as_deref(),
            [("injected_failure_context", serde_json::json!(text))],
        )?;
        transaction.execute(
            "UPDATE workflow_node_runs SET payload = ?2 WHERE id = ?1 AND is_deleted = 0",
            params![node_run_id.as_ref(), payload],
        )?;
        transaction.commit()?;
        Ok(())
    })
    .map_err(engine_repository_error_from_database)
}
