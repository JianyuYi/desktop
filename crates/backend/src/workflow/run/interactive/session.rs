//! Coordinates human follow-up turns against interactive workflow node sessions.
//!
//! An interactive node parks at `Pending` (awaiting input) after its first turn. When the user
//! sends a follow-up message through the ordinary `prompt_session` path, this module validates the
//! node against the per-run gate, flips it to `Running` while the agent answers, and flips it back
//! to `Pending` when the turn ends or the stream is dropped. Terminal nodes are read-only: a
//! session bound to a `Succeeded`/`Failed`/`Cancelled` node no longer accepts prompts through this
//! path, which keeps completed workflow nodes from mutating the worktree with no node-run
//! provenance.

use crate::error::BackendError;
use crate::git_cleanup::KeyedResourceLocks;
use ora_application::{
    AdvanceWorkflowRunResult, AgentOutputContract, ApplicationError, RepositoryError,
    WorkflowGraph, WorkflowRunEngineRepository,
};
use ora_db::{RepositoryPool, SqliteWorkflowRunEngineRepository};
use ora_domain::{
    PromptInactivityPolicy, SessionId, WorkflowNodeRunId, WorkflowNodeStatus, WorkflowRunStatus,
};
use ora_history::{SessionHistory, read_session_history};
use ora_logging::ora_warn;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use super::super::completion_snapshot::{
    CompletionSnapshot, completion_payload, derive_completion_snapshot, unknown_completion_snapshot,
};
use super::CompletingNodeRuns;
use crate::workflow::run::transitions::WorkflowRunTransitions;

/// How long cleanup waits for the dropped turn's durable `TurnEnded` before parking provisionally.
///
/// The runtime writes the boundary as part of settling the cancelled operation, so this window
/// only covers the hand-off between the actor's confirmation and the history file.
const TURN_END_WAIT: Duration = Duration::from_secs(2);

/// How often cleanup re-reads the persisted history while waiting for `TurnEnded`.
const TURN_END_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// A granted turn carries workflow policy only when it has a workflow owner.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum HumanTurnAdmission {
    Ordinary,
    Workflow {
        node_run_id: WorkflowNodeRunId,
        prompt_inactivity: PromptInactivityPolicy,
    },
}

/// Validates and flips an awaiting interactive node to `Running` before a human turn.
///
/// Workflow turns retain the frozen node's policy; an unbound session keeps ordinary defaults.
/// Rejects with `WorkflowNodeNotAwaitingInput` when the node is terminal, already running, being
/// completed, or its run is no longer executing, so a finished workflow node can never accept
/// another prompt through this path.
pub(crate) async fn begin_human_turn(
    pool: &RepositoryPool,
    run_locks: &Arc<KeyedResourceLocks>,
    completing_node_runs: &Arc<CompletingNodeRuns>,
    transitions: &Arc<WorkflowRunTransitions>,
    session_id: &str,
) -> Result<HumanTurnAdmission, BackendError> {
    let pool = pool.clone();
    let run_locks = run_locks.clone();
    let completing_node_runs = completing_node_runs.clone();
    let transitions = transitions.clone();
    let session_id = SessionId::new(session_id);
    tokio::task::spawn_blocking(move || {
        let repository = SqliteWorkflowRunEngineRepository::new(pool);
        let node_run = repository
            .find_node_run_by_session_id(&session_id)
            .map_err(repository_error)?;
        let Some(node_run) = node_run else {
            // No workflow node is bound to this session: an ordinary session prompt.
            return Ok(HumanTurnAdmission::Ordinary);
        };
        // Serialize against completion/cancel/scheduling for this run before deciding whether the
        // prompt may start, so a concurrent completion cannot be raced past.
        let _gate = run_locks.acquire_exclusive(node_run.run_id.as_ref());
        // Re-read the node under the gate: its status may have changed since the first read.
        let node_run = repository
            .find_node_run_by_session_id(&session_id)
            .map_err(repository_error)?;
        let Some(node_run) = node_run else {
            return Ok(HumanTurnAdmission::Ordinary);
        };
        // The owning run must still be executing; a terminal run rejects a follow-up turn.
        let context = repository
            .find_execution_context(&node_run.run_id)
            .map_err(repository_error)?;
        let Some(context) = context else {
            return Err(node_not_awaiting(&node_run.node_id));
        };
        if context.run.status != WorkflowRunStatus::Running {
            return Err(node_not_awaiting(&node_run.node_id));
        }
        // A terminal node is read-only; a non-Pending node is not awaiting input.
        if node_run.status != WorkflowNodeStatus::Pending {
            return Err(node_not_awaiting(&node_run.node_id));
        }
        if completing_node_runs
            .lock()
            .map_err(|_poisoned| node_not_awaiting(&node_run.node_id))?
            .contains(&node_run.id)
        {
            return Err(node_not_awaiting(&node_run.node_id));
        }
        // Resolve policy before the state transition, so a malformed frozen graph cannot strand
        // an awaiting node at Running or let a newer draft change an admitted turn's behavior.
        let graph = WorkflowGraph::parse(&context.graph_json)
            .map_err(ApplicationError::WorkflowRunGraphParse)?;
        let config = graph
            .execution_node(&node_run.node_id)
            .and_then(|node| node.agent_config.as_ref())
            .ok_or_else(|| node_not_awaiting(&node_run.node_id))?;
        let prompt_inactivity = config.prompt_inactivity;
        // The guarded transition both grants the turn and races against any concurrent mutation;
        // a rejected transition means the node is no longer awaiting and the prompt must not
        // start. It commits through the shared transition sink, so a granted turn publishes the
        // run invalidation observers re-query (ADR D7) while a rejection publishes nothing.
        match transitions
            .transition_node_run_status(
                &node_run.id,
                WorkflowNodeStatus::Pending,
                WorkflowNodeStatus::Running,
            )
            .map_err(repository_error)?
        {
            AdvanceWorkflowRunResult::Advanced => Ok(HumanTurnAdmission::Workflow {
                node_run_id: node_run.id,
                prompt_inactivity,
            }),
            AdvanceWorkflowRunResult::NotRunning | AdvanceWorkflowRunResult::NotFound => {
                Err(node_not_awaiting(&node_run.node_id))
            }
        }
    })
    .await
    .map_err(|source| BackendError::internal("repository operation did not complete", source))?
}

/// Parks an interactive node once its human turn ends or its stream is dropped.
///
/// Cleanup may run before the runtime has durably written this turn's `TurnEnded`, so it polls the
/// persisted history for a bounded time before deriving the snapshot the read projection and a
/// later completion both use. A turn that never settles parks with the provisional unknown
/// snapshot, which a later `prepare` repairs once history catches up.
///
/// This is deliberately exempt from the per-run gate: it is a guarded `Running → Pending` cleanup
/// transition that computes no ready set, dispatches nothing, and becomes a no-op when the node has
/// already reached a terminal state. Routing the fire-and-forget drop hook through the gate would
/// add a run-id lookup and a blocking lock for no scheduling benefit. The commit still goes
/// through the shared transition sink, so a completed park publishes the run invalidation while
/// a no-op park publishes nothing.
pub(crate) async fn end_human_turn(
    pool: &RepositoryPool,
    sessions_root: &Path,
    transitions: &Arc<WorkflowRunTransitions>,
    node_run_id: &WorkflowNodeRunId,
) -> Result<(), BackendError> {
    let context = {
        let pool = pool.clone();
        let node_run_id = node_run_id.clone();
        tokio::task::spawn_blocking(move || TurnEndContext::load(&pool, &node_run_id))
            .await
            .map_err(|source| {
                BackendError::internal("repository operation did not complete", source)
            })??
    };
    let Some(context) = context else {
        return Ok(());
    };
    let snapshot = match context.session_id.as_ref() {
        Some(session_id) => {
            settled_snapshot(sessions_root, session_id, context.output_contract.as_ref()).await
        }
        // A node without a bound session has no history to read; the conservative provisional
        // snapshot keeps it parkable and lets a later completion force the decision.
        None => unknown_completion_snapshot(true),
    };
    let completion = completion_payload(&snapshot);
    let transitions = transitions.clone();
    let node_run_id = node_run_id.clone();
    tokio::task::spawn_blocking(move || {
        transitions
            .park_node_run(&node_run_id, &completion)
            .map_err(repository_error)?;
        Ok(())
    })
    .await
    .map_err(|source| BackendError::internal("repository operation did not complete", source))?
}

/// Restores an awaiting node after a turn that never started and settled no turn.
///
/// The prompt failed before the runtime accepted it, so there is no new user message and no
/// `TurnEnded` to snapshot. The plain guarded transition puts the node back at `Pending` without
/// overwriting the snapshot of the turn that previously settled.
pub(crate) async fn restore_human_turn(
    transitions: &Arc<WorkflowRunTransitions>,
    node_run_id: &WorkflowNodeRunId,
) -> Result<(), BackendError> {
    let transitions = transitions.clone();
    let node_run_id = node_run_id.clone();
    tokio::task::spawn_blocking(move || {
        transitions
            .transition_node_run_status(
                &node_run_id,
                WorkflowNodeStatus::Running,
                WorkflowNodeStatus::Pending,
            )
            .map_err(repository_error)?;
        Ok(())
    })
    .await
    .map_err(|source| BackendError::internal("repository operation did not complete", source))?
}

/// The persisted inputs cleanup needs before it can snapshot a dropped turn.
struct TurnEndContext {
    /// The session bound to the node; absent when the node row no longer exists.
    session_id: Option<SessionId>,
    /// The frozen node's output contract, which decides whether the reply satisfies the node.
    output_contract: Option<AgentOutputContract>,
}

impl TurnEndContext {
    /// Reads the node's bound session and frozen output contract before cleanup parks it.
    ///
    /// A missing node or a node that already left `Running` returns `None`: the park guard would
    /// reject the write anyway, so waiting for history and deriving a snapshot would be wasted.
    fn load(
        pool: &RepositoryPool,
        node_run_id: &WorkflowNodeRunId,
    ) -> Result<Option<Self>, BackendError> {
        let repository = SqliteWorkflowRunEngineRepository::new(pool.clone());
        let Some(node_run) = repository
            .find_node_run_by_id(node_run_id)
            .map_err(repository_error)?
        else {
            return Ok(None);
        };
        if node_run.status != WorkflowNodeStatus::Running {
            return Ok(None);
        }
        let output_contract = match repository
            .find_execution_context(&node_run.run_id)
            .map_err(repository_error)?
        {
            Some(context) => frozen_output_contract(&context.graph_json, &node_run.node_id),
            None => None,
        };
        Ok(Some(Self {
            session_id: node_run.session_id,
            output_contract,
        }))
    }
}

/// Returns the frozen node's declared output contract, or `None` when the graph cannot be read.
///
/// `begin_human_turn` already parsed the same frozen graph before granting the turn, so a parse
/// failure here can only come from corrupt persisted state; parking without a contract still
/// restores the node instead of stranding it at `Running`.
fn frozen_output_contract(graph_json: &str, node_id: &str) -> Option<AgentOutputContract> {
    let graph = match WorkflowGraph::parse(graph_json) {
        Ok(graph) => graph,
        Err(error) => {
            ora_warn!(
                node_id,
                error = %error,
                "parking an interactive node without its frozen output contract"
            );
            return None;
        }
    };
    graph
        .execution_node(node_id)?
        .agent_config
        .as_ref()?
        .output_contract
        .clone()
}

/// Derives the dropped turn's snapshot, waiting a bounded time for its durable `TurnEnded`.
///
/// Polling persisted history keeps cleanup independent of the runtime actor: the actor already
/// wrote the boundary by the time the stream cleanup runs, but a slow filesystem flush must not
/// park a node whose response the snapshot could still capture.
async fn settled_snapshot(
    sessions_root: &Path,
    session_id: &SessionId,
    output_contract: Option<&AgentOutputContract>,
) -> CompletionSnapshot {
    let deadline = tokio::time::Instant::now() + TURN_END_WAIT;
    loop {
        match read_history_blocking(sessions_root, session_id).await {
            Ok(history) => {
                let snapshot = derive_completion_snapshot(&history, output_contract, false);
                if snapshot.turn_key.is_some() {
                    return snapshot;
                }
            }
            Err(error) => {
                // A history that cannot be read can never yield a turn boundary, so waiting
                // would only delay the park; the provisional snapshot records why it is unknown.
                ora_warn!(
                    session_id = %session_id,
                    error = %error,
                    "failed to read session history when parking an interactive node"
                );
                return unknown_completion_snapshot(true);
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return unknown_completion_snapshot(true);
        }
        tokio::time::sleep(TURN_END_POLL_INTERVAL).await;
    }
}

/// Reads one session's history off the async runtime, since the read is blocking file I/O.
async fn read_history_blocking(
    sessions_root: &Path,
    session_id: &SessionId,
) -> Result<SessionHistory, BackendError> {
    let root = sessions_root.to_path_buf();
    let id = session_id.to_string();
    tokio::task::spawn_blocking(move || read_session_history(&root, &id))
        .await
        .map_err(|source| BackendError::internal("history read did not complete", source))?
        .map_err(|error| BackendError::internal("failed to read session history", error))
}

/// Renders the public rejection for a workflow node that cannot accept a prompt turn.
fn node_not_awaiting(node_id: &str) -> BackendError {
    BackendError::from(ApplicationError::WorkflowNodeNotAwaitingInput {
        node_id: node_id.to_string(),
    })
}

/// Maps a workflow engine repository failure onto the public backend error.
fn repository_error(source: RepositoryError) -> BackendError {
    BackendError::from(ApplicationError::WorkflowRunRepository { source })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::run::test_fixture::*;
    use ora_application::{NodeFailure, NodeFailureKind, WorkflowRunEngine, WorkflowRunRepository};
    use ora_db::SqliteWorkflowRunRepository;
    use pretty_assertions::assert_eq;

    /// A session not bound to any workflow node is an ordinary session prompt.
    #[test]
    fn session_without_bound_node_is_an_ordinary_prompt() {
        run_test(async {
            let (_temp, pool) = bootstrap();
            let (run_locks, completing) = locks();
            let (transitions, _recording) = recording_transitions(&pool);
            let result = begin_human_turn(
                &pool,
                &run_locks,
                &completing,
                &transitions,
                "unbound-session",
            )
            .await
            .unwrap();
            assert_eq!(result, HumanTurnAdmission::Ordinary);
        });
    }

    /// A session bound to a terminal node rejects the prompt instead of proceeding as ordinary.
    #[test]
    fn terminal_node_rejects_prompt() {
        run_test(async {
            let (temp, pool) = bootstrap();
            let (_run_id, node_runs) = started_run(&temp, &pool, AGENT_GRAPH);
            let agent = node_runs.iter().find(|n| n.node_id == "agent").unwrap();
            let repository = SqliteWorkflowRunEngineRepository::new(pool.clone());
            let session_id = SessionId::new("session-1");
            repository
                .bind_node_run_session(&agent.id, &session_id, 50)
                .unwrap();
            repository
                .transition_node_run_status(
                    &agent.id,
                    WorkflowNodeStatus::Running,
                    WorkflowNodeStatus::Succeeded,
                    50,
                )
                .unwrap();

            let (run_locks, completing) = locks();
            let (transitions, recording) = recording_transitions(&pool);
            assert!(
                begin_human_turn(&pool, &run_locks, &completing, &transitions, "session-1")
                    .await
                    .is_err(),
                "a terminal node must reject the prompt"
            );
            assert!(
                recording.published.lock().unwrap().is_empty(),
                "a rejected turn transition must not publish"
            );
        });
    }

    /// A node being manually completed rejects a concurrent prompt.
    #[test]
    fn completing_node_rejects_prompt() {
        run_test(async {
            let (temp, pool) = bootstrap();
            let (_run_id, node_runs) = started_run(&temp, &pool, AGENT_GRAPH);
            let agent = node_runs.iter().find(|n| n.node_id == "agent").unwrap();
            let (session_id, node_run_id) = bind_and_park(&pool, agent);

            let (run_locks, completing) = locks();
            let (transitions, _recording) = recording_transitions(&pool);
            completing.lock().unwrap().insert(node_run_id);

            assert!(
                begin_human_turn(
                    &pool,
                    &run_locks,
                    &completing,
                    &transitions,
                    session_id.as_ref()
                )
                .await
                .is_err(),
                "a completing node must reject the prompt"
            );
        });
    }

    /// An awaiting node flips to `Running` and admits the prompt.
    #[test]
    fn awaiting_node_flips_to_running() {
        run_test(async {
            let (temp, pool) = bootstrap();
            let (_run_id, node_runs) = started_run(&temp, &pool, AGENT_GRAPH);
            let agent = node_runs.iter().find(|n| n.node_id == "agent").unwrap();
            let (session_id, _node_run_id) = bind_and_park(&pool, agent);

            let (run_locks, completing) = locks();
            let (transitions, _recording) = recording_transitions(&pool);
            let result = begin_human_turn(
                &pool,
                &run_locks,
                &completing,
                &transitions,
                session_id.as_ref(),
            )
            .await
            .unwrap();
            assert_eq!(
                result,
                HumanTurnAdmission::Workflow {
                    node_run_id: _node_run_id,
                    prompt_inactivity: PromptInactivityPolicy::Timeout,
                }
            );

            let node_runs = SqliteWorkflowRunRepository::new(pool)
                .list_node_runs(&_run_id)
                .unwrap();
            let agent = node_runs.iter().find(|n| n.node_id == "agent").unwrap();
            assert_eq!(agent.status, WorkflowNodeStatus::Running);
        });
    }

    /// A `Pending` node in a failed run rejects the prompt (the run is no longer executing).
    #[test]
    fn non_running_run_rejects_prompt() {
        run_test(async {
            let (temp, pool) = bootstrap();
            let (run_id, node_runs) = started_run(&temp, &pool, TWO_AGENT_GRAPH);
            let left = node_runs.iter().find(|n| n.node_id == "l").unwrap();
            let right = node_runs.iter().find(|n| n.node_id == "r").unwrap();
            let (session_id, _node_run_id) = bind_and_park(&pool, left);

            // Failing the sibling node fails the run but leaves the parked node `Pending`.
            let engine = WorkflowRunEngine::new(
                SqliteWorkflowRunEngineRepository::new(pool.clone()),
                NoopExecutor,
                SeqGen::default(),
                ClockAt(40),
            );
            engine
                .fail_node(
                    &run_id,
                    &right.id,
                    NodeFailure::new(NodeFailureKind::Session, "boom"),
                )
                .unwrap();

            let (run_locks, completing) = locks();
            let (transitions, _recording) = recording_transitions(&pool);
            assert!(
                begin_human_turn(
                    &pool,
                    &run_locks,
                    &completing,
                    &transitions,
                    session_id.as_ref()
                )
                .await
                .is_err(),
                "a node in a non-running run must reject the prompt"
            );
            assert_ne!(run_id.as_ref(), "");
        });
    }

    /// A granted human turn publishes exactly one run invalidation (ADR D7): the awaiting
    /// dock is user-visible state, and observers re-query instead of waiting for the polling
    /// fallback.
    #[test]
    fn beginning_a_human_turn_publishes_one_run_invalidation() {
        run_test(async {
            let (temp, pool) = bootstrap();
            let (run_id, node_runs) = started_run(&temp, &pool, AGENT_GRAPH);
            let agent = node_runs.iter().find(|n| n.node_id == "agent").unwrap();
            let (session_id, _node_run_id) = bind_and_park(&pool, agent);

            let (run_locks, completing) = locks();
            let (transitions, recording) = recording_transitions(&pool);
            let result = begin_human_turn(
                &pool,
                &run_locks,
                &completing,
                &transitions,
                session_id.as_ref(),
            )
            .await
            .unwrap();
            assert_eq!(
                result,
                HumanTurnAdmission::Workflow {
                    node_run_id: _node_run_id,
                    prompt_inactivity: PromptInactivityPolicy::Timeout,
                }
            );
            assert_eq!(
                *recording.published.lock().unwrap(),
                vec![run_id.to_string()],
                "the granted Pending -> Running transition publishes exactly one invalidation"
            );
        });
    }

    /// Ending a turn commits the Running -> Pending flip and publishes the same invalidation,
    /// so the awaiting dock is observable the moment the agent stops generating.
    #[test]
    fn ending_a_human_turn_publishes_one_run_invalidation() {
        run_test(async {
            let (temp, pool) = bootstrap();
            let (run_id, node_runs) = started_run(&temp, &pool, AGENT_GRAPH);
            let agent = node_runs.iter().find(|n| n.node_id == "agent").unwrap();
            let (session_id, _node_run_id) = bind_and_park(&pool, agent);
            let sessions_root = temp.path().join("sessions");
            write_settled_turn(&sessions_root, session_id.as_ref(), "go", "done");

            let (run_locks, completing) = locks();
            let (transitions, recording) = recording_transitions(&pool);
            begin_human_turn(
                &pool,
                &run_locks,
                &completing,
                &transitions,
                session_id.as_ref(),
            )
            .await
            .unwrap();
            end_human_turn(&pool, &sessions_root, &transitions, &_node_run_id)
                .await
                .unwrap();

            assert_eq!(
                *recording.published.lock().unwrap(),
                vec![run_id.to_string(), run_id.to_string()],
                "the begun turn and the ended turn each publish one invalidation"
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

    /// An idempotently rejected turn-end (the node already left Running) commits nothing and
    /// publishes nothing, matching the engine's discipline for duplicate reports.
    #[test]
    fn a_rejected_turn_end_publishes_nothing() {
        run_test(async {
            let (temp, pool) = bootstrap();
            let (_run_id, node_runs) = started_run(&temp, &pool, AGENT_GRAPH);
            let agent = node_runs.iter().find(|n| n.node_id == "agent").unwrap();

            // Park the node directly, then end a turn that never began: cleanup finds the node
            // already outside `Running`, so its park guard has nothing to commit.
            let (_session_id, node_run_id) = bind_and_park(&pool, agent);
            let (transitions, recording) = recording_transitions(&pool);
            end_human_turn(
                &pool,
                &temp.path().join("sessions"),
                &transitions,
                &node_run_id,
            )
            .await
            .unwrap();

            assert!(
                recording.published.lock().unwrap().is_empty(),
                "a rejected turn-end transition must not publish"
            );
        });
    }

    /// A second completion claim against the same awaiting node is rejected, so two concurrent
    /// completes cannot both prepare against one node.
    #[test]
    fn second_completion_claim_is_rejected() {
        run_test(async {
            let (temp, pool) = bootstrap();
            let (run_id, node_runs) = started_run(&temp, &pool, AGENT_GRAPH);
            let agent = node_runs.iter().find(|n| n.node_id == "agent").unwrap();
            let (_session_id, _node_run_id) = bind_and_park(&pool, agent);

            let (run_locks, completing) = locks();

            assert!(
                super::super::completion::claim_node_for_completion(
                    &pool,
                    &run_locks,
                    &completing,
                    &run_id,
                    "agent",
                )
                .is_ok()
            );

            assert!(
                super::super::completion::claim_node_for_completion(
                    &pool,
                    &run_locks,
                    &completing,
                    &run_id,
                    "agent",
                )
                .is_err(),
                "a second claim against the same awaiting node must be rejected"
            );
        });
    }
}
