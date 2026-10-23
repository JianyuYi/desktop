//! Node-run status transitions committed outside the scheduling engine.
//!
//! The interactive chain — an agent node parking at awaiting input after its first turn, a
//! human follow-up turn beginning, and that turn ending — commits guarded node-run transitions
//! directly instead of entering `WorkflowRunEngine`. This sink keeps those commits on the same
//! discipline as the engine's own transitions (ADR "node runtime orchestration" D7): the
//! guarded transition commits first, and exactly one run invalidation is published when it
//! commits. A rejected guard is an idempotent no-op that commits nothing and publishes
//! nothing.

use crate::clock::SystemClock;
use crate::git_cleanup::KeyedResourceLocks;
use ora_application::{
    AdvanceWorkflowRunResult, Clock, RepositoryError, WorkflowRunEngineRepository,
    WorkflowRunInvalidationPublisher,
};
use ora_db::{RepositoryPool, SqliteWorkflowRunEngineRepository};
use ora_domain::{WorkflowNodeRunId, WorkflowNodeStatus, WorkflowRunId};
use std::sync::Arc;

/// Commits the engine-external node-run transitions and publishes their run invalidations.
///
/// The sink shares the engine's invalidation mechanism (the same
/// [`WorkflowRunInvalidationPublisher`] bridge onto the application event hub), so observers
/// cannot tell an engine-committed transition from an interactive one — both re-query the same
/// persisted state.
pub(crate) struct WorkflowRunTransitions {
    pool: RepositoryPool,
    events: Arc<dyn WorkflowRunInvalidationPublisher>,
    /// The same per-run gate the engine and the completion path take (ADR root D2).
    run_locks: Arc<KeyedResourceLocks>,
}

impl WorkflowRunTransitions {
    /// Creates the sink over the run persistence and the shared invalidation mechanism.
    pub(crate) fn new(
        pool: RepositoryPool,
        events: Arc<dyn WorkflowRunInvalidationPublisher>,
        run_locks: Arc<KeyedResourceLocks>,
    ) -> Self {
        Self {
            pool,
            events,
            run_locks,
        }
    }

    /// Commits one guarded node-run status transition, publishing the owning run's
    /// invalidation only when the repository accepted it.
    ///
    /// The run id is resolved from the committed row: callers such as the turn-cleanup path
    /// hold only the node-run id. A row that vanishes between the commit and the read (a
    /// concurrent restart) publishes nothing — a lost event only leaves a stale view that the
    /// next transition or refresh clears.
    pub(crate) fn transition_node_run_status(
        &self,
        node_run_id: &WorkflowNodeRunId,
        from: WorkflowNodeStatus,
        to: WorkflowNodeStatus,
    ) -> Result<AdvanceWorkflowRunResult, RepositoryError> {
        let repository = SqliteWorkflowRunEngineRepository::new(self.pool.clone());
        let result = repository.transition_node_run_status(
            node_run_id,
            from,
            to,
            SystemClock.now_timestamp_millis(),
        )?;
        if !matches!(result, AdvanceWorkflowRunResult::Advanced) {
            return Ok(result);
        }
        if let Some(node_run) = repository.find_node_run_by_id(node_run_id)? {
            self.events.publish_run_invalidated(&node_run.run_id);
        }
        Ok(result)
    }

    /// Parks one running interactive node with its completion snapshot in the same guarded
    /// transaction, then publishes the run invalidation once the commit succeeds.
    ///
    /// The sink takes the per-run gate itself (ADR D3 step 4): callers must not already hold it —
    /// the gate is not reentrant — and must therefore call this from a blocking context. Holding
    /// the gate across the commit is what makes a park serialize against a completion, a cancel,
    /// and the next scheduling wave instead of relying on the guard alone.
    ///
    /// A rejected guard (a cancel or completion won the race) is an idempotent no-op: no payload
    /// is written and no invalidation is published.
    pub(crate) fn park_node_run(
        &self,
        run_id: &WorkflowRunId,
        node_run_id: &WorkflowNodeRunId,
        completion: &serde_json::Value,
    ) -> Result<AdvanceWorkflowRunResult, RepositoryError> {
        let _gate = self.run_locks.acquire_exclusive(run_id.as_ref());
        let repository = SqliteWorkflowRunEngineRepository::new(self.pool.clone());
        let result = repository.park_node_run(
            node_run_id,
            completion,
            SystemClock.now_timestamp_millis(),
        )?;
        if matches!(result, AdvanceWorkflowRunResult::Advanced)
            && let Some(node_run) = repository.find_node_run_by_id(node_run_id)?
        {
            self.events.publish_run_invalidated(&node_run.run_id);
        }
        Ok(result)
    }

    /// Publishes one run invalidation after a commit that happened outside this sink.
    ///
    /// Resume snapshot-switch commits through the engine repository rather than this sink; the
    /// caller publishes afterwards so observers re-query the switched snapshot (ADR D7).
    pub(crate) fn publish_run_invalidated(&self, run_id: &ora_domain::WorkflowRunId) {
        self.events.publish_run_invalidated(run_id);
    }
}

#[cfg(test)]
mod tests {
    use super::WorkflowRunTransitions;
    use crate::git_cleanup::KeyedResourceLocks;
    use crate::workflow::run::test_fixture::{
        AGENT_GRAPH, RecordingInvalidations, bootstrap, run_test, started_run,
    };
    use ora_application::WorkflowRunRepository;
    use ora_db::SqliteWorkflowRunRepository;
    use ora_domain::WorkflowNodeStatus;
    use pretty_assertions::assert_eq;
    use std::sync::Arc;

    /// A committed transition publishes exactly one invalidation carrying the owning run id:
    /// the same discipline the engine applies to its own scheduling-wave transitions.
    #[test]
    fn committed_transitions_publish_one_invalidation() {
        run_test(async {
            let (temp, pool) = bootstrap();
            let (run_id, node_runs) = started_run(&temp, &pool, AGENT_GRAPH);
            let agent = node_runs.iter().find(|n| n.node_id == "agent").unwrap();
            let recording = Arc::new(RecordingInvalidations::default());
            let transitions = WorkflowRunTransitions::new(
                pool.clone(),
                recording.clone(),
                KeyedResourceLocks::new(),
            );

            let result = transitions.transition_node_run_status(
                &agent.id,
                WorkflowNodeStatus::Running,
                WorkflowNodeStatus::Pending,
            );

            assert_eq!(
                result.unwrap(),
                ora_application::AdvanceWorkflowRunResult::Advanced
            );
            assert_eq!(
                *recording.published.lock().unwrap(),
                vec![run_id.to_string()],
                "a committed transition publishes exactly one invalidation"
            );
            let parked = SqliteWorkflowRunRepository::new(pool)
                .list_node_runs(&run_id)
                .unwrap()
                .into_iter()
                .find(|n| n.node_id == "agent")
                .unwrap();
            assert_eq!(parked.status, WorkflowNodeStatus::Pending);
        });
    }

    /// A rejected guard (the node is no longer in the expected status) is an idempotent
    /// no-op: nothing commits, so nothing publishes.
    #[test]
    fn rejected_transitions_publish_nothing() {
        run_test(async {
            let (temp, pool) = bootstrap();
            let (_run_id, node_runs) = started_run(&temp, &pool, AGENT_GRAPH);
            let agent = node_runs.iter().find(|n| n.node_id == "agent").unwrap();
            let recording = Arc::new(RecordingInvalidations::default());
            let transitions = WorkflowRunTransitions::new(
                pool.clone(),
                recording.clone(),
                KeyedResourceLocks::new(),
            );

            // The node is Running, so a Pending -> Running guard must reject.
            let result = transitions.transition_node_run_status(
                &agent.id,
                WorkflowNodeStatus::Pending,
                WorkflowNodeStatus::Running,
            );

            assert_eq!(
                result.unwrap(),
                ora_application::AdvanceWorkflowRunResult::NotRunning
            );
            assert!(
                recording.published.lock().unwrap().is_empty(),
                "an idempotently rejected transition must not publish"
            );
        });
    }

    /// A park commit writes the snapshot and the Pending status in one guarded update and
    /// publishes exactly one invalidation.
    #[test]
    fn park_node_run_commits_snapshot_and_status() {
        run_test(async {
            let (temp, pool) = bootstrap();
            let (run_id, node_runs) = started_run(&temp, &pool, AGENT_GRAPH);
            let agent = node_runs.iter().find(|n| n.node_id == "agent").unwrap();
            let recording = Arc::new(RecordingInvalidations::default());
            let transitions = WorkflowRunTransitions::new(
                pool.clone(),
                recording.clone(),
                KeyedResourceLocks::new(),
            );
            let snapshot = serde_json::json!({
                "completion": {
                    "turnKey": { "userSeq": 1, "endedSeq": 2 },
                    "blockers": []
                }
            });

            let result = transitions
                .park_node_run(&run_id, &agent.id, &snapshot)
                .unwrap();

            assert_eq!(result, ora_application::AdvanceWorkflowRunResult::Advanced);
            assert_eq!(
                *recording.published.lock().unwrap(),
                vec![run_id.to_string()]
            );
            let parked = SqliteWorkflowRunRepository::new(pool)
                .list_node_runs(&run_id)
                .unwrap()
                .into_iter()
                .find(|n| n.node_id == "agent")
                .unwrap();
            assert_eq!(parked.status, WorkflowNodeStatus::Pending);
            let payload: serde_json::Value =
                serde_json::from_str(parked.payload.as_deref().unwrap()).unwrap();
            assert_eq!(payload["completion"]["turnKey"]["userSeq"], 1);
            assert_eq!(payload["completion"]["turnKey"]["endedSeq"], 2);
        });
    }

    /// A rejected park guard writes neither the snapshot nor an invalidation.
    #[test]
    fn rejected_park_writes_nothing() {
        run_test(async {
            let (temp, pool) = bootstrap();
            let (run_id, node_runs) = started_run(&temp, &pool, AGENT_GRAPH);
            let agent = node_runs.iter().find(|n| n.node_id == "agent").unwrap();
            let recording = Arc::new(RecordingInvalidations::default());
            let transitions = WorkflowRunTransitions::new(
                pool.clone(),
                recording.clone(),
                KeyedResourceLocks::new(),
            );

            transitions
                .transition_node_run_status(
                    &agent.id,
                    WorkflowNodeStatus::Running,
                    WorkflowNodeStatus::Pending,
                )
                .unwrap();
            recording.published.lock().unwrap().clear();
            let before = SqliteWorkflowRunRepository::new(pool.clone())
                .list_node_runs(&run_id)
                .unwrap()
                .into_iter()
                .find(|n| n.node_id == "agent")
                .unwrap()
                .payload;

            let result = transitions
                .park_node_run(
                    &run_id,
                    &agent.id,
                    &serde_json::json!({ "completion": { "blockers": [] } }),
                )
                .unwrap();

            assert_eq!(
                result,
                ora_application::AdvanceWorkflowRunResult::NotRunning
            );
            assert!(recording.published.lock().unwrap().is_empty());
            let after = SqliteWorkflowRunRepository::new(pool)
                .list_node_runs(&run_id)
                .unwrap()
                .into_iter()
                .find(|n| n.node_id == "agent")
                .unwrap()
                .payload;
            assert_eq!(after, before);
        });
    }
}
