//! End-to-end aggregator scenarios over the engine and an in-memory repository.
//!
//! The harness mirrors the real repository's completion semantics (per-type pool writes inside
//! one payload update) so the scenarios exercise the same data flow as production: scheduling,
//! branch projection, swift completion, and the typed aggregator pool write.

use super::{
    AdvanceWorkflowRunResult, CancelWorkflowRunResult, ExecutionContext, FailurePropagation,
    FileChange, NodeExecutor, NodeFailure, NodeFailureKind, NodeRunToStart,
    RestartWorkflowRunResult, ResumeWorkflowRunResult, StartWorkflowRunResult, WorkflowGraphNode,
    WorkflowNodeRunIdGenerator, WorkflowRunEngine, WorkflowRunEngineRepository,
};
use crate::RepositoryError;
use crate::project::Clock;
use crate::workflow_run::engine::graph::WorkflowGraph;
use crate::workflow_run::engine::skill_delivery::WorkflowRunPayload;
use crate::workflow_run::engine::variable_pool::WorkflowVariablePool;
use ora_domain::{
    AuditFields, SessionId, WorkflowId, WorkflowNodeRun, WorkflowNodeRunId, WorkflowNodeStatus,
    WorkflowRun, WorkflowRunId, WorkflowRunStatus, WorkflowScopeId, WorkflowSnapshotId, Workspace,
    WorkspaceId, WorkspaceKind, WorkspaceLifecycle, WorkspaceLocation,
};
use pretty_assertions::assert_eq;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

/// The frozen branch-join workflow: condition routes to two sibling agents whose outputs feed
/// one aggregator, then one Output terminal.
const JOIN_GRAPH: &str = r#"{
    "nodes": [
        {"id":"start","data":{"kind":"start"}},
        {"id":"cond","data":{"kind":"condition","cases":[
            {"id":"yes","logic":"and","conditions":[
                {"variableSelector":["start","input"],"operator":"equals","value":"a"}]}]}},
        {"id":"a","data":{"kind":"agent","agentConfig":{"executor":{"agentCli":"c","modelId":"m"},"prompt":"a"}}},
        {"id":"b","data":{"kind":"agent","agentConfig":{"executor":{"agentCli":"c","modelId":"m"},"prompt":"b"}}},
        {"id":"agg","data":{"kind":"aggregator","aggregatorConfig":{
            "variables":[["a","output"],["b","output"]]}}},
        {"id":"out","data":{"kind":"output","outputs":[{"name":"text","variableSelector":["agg","output"]}]}}
    ],
    "edges": [
        {"source":"start","target":"cond"},
        {"source":"cond","sourceHandle":"yes","target":"a"},
        {"source":"cond","sourceHandle":"else","target":"b"},
        {"source":"a","target":"agg"},
        {"source":"b","target":"agg"},
        {"source":"agg","target":"out"}
    ]
}"#;

/// Parallel fan-in without a Condition: both branches always run, so the declaration order
/// decides the aggregated value.
const PARALLEL_GRAPH: &str = r#"{
    "nodes": [
        {"id":"start","data":{"kind":"start"}},
        {"id":"a","data":{"kind":"agent","agentConfig":{"executor":{"agentCli":"c","modelId":"m"},"prompt":"a"}}},
        {"id":"b","data":{"kind":"agent","agentConfig":{"executor":{"agentCli":"c","modelId":"m"},"prompt":"b"}}},
        {"id":"agg","data":{"kind":"aggregator","aggregatorConfig":{
            "variables":[["b","output"],["a","output"]]}}},
        {"id":"out","data":{"kind":"output"}}
    ],
    "edges": [
        {"source":"start","target":"a"},
        {"source":"start","target":"b"},
        {"source":"a","target":"agg"},
        {"source":"b","target":"agg"},
        {"source":"agg","target":"out"}
    ]
}"#;

/// Direct start → aggregator aggregating an unassigned optional Start variable, and a falsy
/// boolean Start variable for the pass-through boundary.
const START_EDGE_GRAPH: &str = r#"{
    "nodes": [
        {"id":"start","data":{"kind":"start","inputVariables":[
            {"name":"mode","valueType":"string","required":false},
            {"name":"flag","valueType":"boolean","required":false,"value":false}]}},
        {"id":"agg","data":{"kind":"aggregator","aggregatorConfig":{
            "variables":[["start","flag"]]}}},
        {"id":"out","data":{"kind":"output"}}
    ],
    "edges": [{"source":"start","target":"agg"},{"source":"agg","target":"out"}]
}"#;

/// Direct start → aggregator aggregating only an unassigned optional Start variable: the
/// stable no-match failure.
const NO_MATCH_GRAPH: &str = r#"{
    "nodes": [
        {"id":"start","data":{"kind":"start","inputVariables":[
            {"name":"mode","valueType":"string","required":false}]}},
        {"id":"agg","data":{"kind":"aggregator","aggregatorConfig":{
            "variables":[["start","mode"]]}}},
        {"id":"out","data":{"kind":"output"}}
    ],
    "edges": [{"source":"start","target":"agg"},{"source":"agg","target":"out"}]
}"#;

struct NoopExecutor;

impl NodeExecutor for NoopExecutor {
    fn dispatch(
        &self,
        _node_run_id: &WorkflowNodeRunId,
        _node: &WorkflowGraphNode,
        _graph: &WorkflowGraph,
        _context: &ExecutionContext,
        _scope_id: &WorkflowScopeId,
        _variable_pool: &WorkflowVariablePool,
    ) {
    }
}

struct SeqGen;

impl WorkflowNodeRunIdGenerator for SeqGen {
    fn generate_node_run_id(&self) -> WorkflowNodeRunId {
        WorkflowNodeRunId::new("unused")
    }
}

struct ClockAt(i64);

impl Clock for ClockAt {
    fn now_timestamp_millis(&self) -> i64 {
        self.0
    }
}

struct HarnessState {
    context: ExecutionContext,
    node_runs: Vec<WorkflowNodeRun>,
    started_ready: Vec<String>,
    finish_run_calls: usize,
    last_failure: Option<(String, NodeFailureKind, String)>,
    row_sequence: u32,
}

#[derive(Clone)]
struct Harness {
    state: Arc<Mutex<HarnessState>>,
}

impl Harness {
    fn lock(&self) -> std::sync::MutexGuard<'_, HarnessState> {
        self.state.lock().expect("harness state lock")
    }

    /// Builds a run over `graph_json` with the given kickoff input and Start-variable seeds.
    fn new(graph_json: &str, input: Option<&str>, seeds: &[(&str, Value)]) -> Self {
        let graph = WorkflowGraph::parse(graph_json).unwrap();
        let mut pool = WorkflowVariablePool::from_graph(&graph);
        for (selector, value) in seeds {
            let writer = selector.split('.').next().unwrap_or("global").to_string();
            pool.set(selector, &writer, value.clone()).unwrap();
        }
        let payload = WorkflowRunPayload {
            start_node_id: Some("start".to_string()),
            variable_pool: pool,
            ..Default::default()
        };
        let run = WorkflowRun::new(
            WorkflowRunId::new("run-1"),
            WorkspaceId::new("workspace-1"),
            WorkflowId::new("workflow-1"),
            WorkflowSnapshotId::new("snapshot-1"),
            "Aggregator",
            WorkflowRunStatus::Running,
            Some(r#"{"current_nodes":[]}"#.to_string()),
            input.map(str::to_string),
            None,
            None,
            Some(serde_json::to_string(&payload).unwrap()),
            Some(10),
            None,
            AuditFields::new(1, 1, false),
        );
        Self {
            state: Arc::new(Mutex::new(HarnessState {
                context: ExecutionContext {
                    root_scope_id: WorkflowScopeId::new("root:run-1"),
                    run,
                    workspace: Workspace::new(
                        WorkspaceId::new("workspace-1"),
                        ora_domain::ProjectId::new("project-1"),
                        WorkspaceKind::Main,
                        WorkspaceLocation::local_filesystem("/tmp/workspace"),
                        WorkspaceLifecycle::Active,
                        AuditFields::new(1, 1, false),
                    ),
                    graph_json: graph_json.to_string(),
                },
                node_runs: Vec::new(),
                started_ready: Vec::new(),
                finish_run_calls: 0,
                last_failure: None,
                row_sequence: 0,
            })),
        }
    }

    /// Runs the scheduling loop until the run drains or waits on in-flight agents.
    fn drive(&self) {
        let engine = WorkflowRunEngine::new(self.clone(), NoopExecutor, SeqGen, ClockAt(40));
        engine.resume(&WorkflowRunId::new("run-1")).unwrap();
    }

    /// Completes one running agent node through the engine's callback path.
    fn complete_agent(&self, node_id: &str, output: &str) {
        let node_run_id = self.node_run_id(node_id);
        let engine = WorkflowRunEngine::new(self.clone(), NoopExecutor, SeqGen, ClockAt(40));
        engine
            .complete_node(
                &WorkflowRunId::new("run-1"),
                &node_run_id,
                Some(output.to_string()),
                None,
                Some("end_turn".to_string()),
                Vec::new(),
            )
            .unwrap();
    }

    fn node_run_id(&self, node_id: &str) -> WorkflowNodeRunId {
        self.lock()
            .node_runs
            .iter()
            .find(|row| row.node_id == node_id)
            .map(|row| row.id.clone())
            .unwrap_or_else(|| panic!("no node run for {node_id}"))
    }

    fn node_status(&self, node_id: &str) -> WorkflowNodeStatus {
        self.lock()
            .node_runs
            .iter()
            .find(|row| row.node_id == node_id)
            .map(|row| row.status)
            .unwrap_or(WorkflowNodeStatus::Pending)
    }

    fn node_output(&self, node_id: &str) -> Option<String> {
        self.lock()
            .node_runs
            .iter()
            .find(|row| row.node_id == node_id)
            .and_then(|row| row.output.clone())
    }

    /// The committed pool value of one fully qualified selector after the last wave.
    fn pool_value(&self, selector: &str) -> Option<Value> {
        let state = self.lock();
        let payload: WorkflowRunPayload =
            serde_json::from_str(state.context.run.payload.as_deref().unwrap()).unwrap();
        payload.variable_pool.values.get(selector).cloned()
    }

    fn run_status(&self) -> WorkflowRunStatus {
        self.lock().context.run.status
    }
}

/// Mirrors the repository's completion semantics: node status, output, and the per-type pool
/// write commit against one payload update.
///
/// Aggregator graphs never schedule an automatic retry: their failures are definition-shaped
/// (no matching output), which the retry policy skips by kind.
impl crate::workflow_run::engine::WorkflowRetryRepository for Harness {
    fn schedule_node_retry(
        &self,
        _failed_node_run_id: &WorkflowNodeRunId,
        _failure: &crate::workflow_run::engine::NodeFailure,
        _retry: &crate::workflow_run::engine::NodeRetryToSchedule,
        _now: i64,
    ) -> Result<crate::workflow_run::engine::ScheduleNodeRetryResult, RepositoryError> {
        unreachable!("no automatic retry is scheduled in these tests")
    }

    fn begin_node_retry(
        &self,
        _node_run_id: &WorkflowNodeRunId,
        _now: i64,
    ) -> Result<crate::workflow_run::engine::BeginNodeRetryResult, RepositoryError> {
        unreachable!("no automatic retry is scheduled in these tests")
    }
}

impl WorkflowRunEngineRepository for Harness {
    fn find_active_loop_round(
        &self,
        _parent_loop_node_run_id: &WorkflowNodeRunId,
    ) -> Result<Option<ora_domain::WorkflowExecutionScope>, RepositoryError> {
        Ok(None)
    }

    fn list_node_runs_in_scope(
        &self,
        _scope_id: &WorkflowScopeId,
    ) -> Result<Vec<WorkflowNodeRun>, RepositoryError> {
        self.list_node_runs(&WorkflowRunId::new("run-1"))
    }

    fn start_loop_round(
        &self,
        _run_id: &WorkflowRunId,
        _round: &crate::workflow_run::engine::ports::LoopRoundToStart,
        _now: i64,
    ) -> Result<(), RepositoryError> {
        unreachable!("no Loop node in these graphs")
    }

    fn start_scope_ready_nodes(
        &self,
        _scope_id: &WorkflowScopeId,
        _node_runs: &[NodeRunToStart],
        _now: i64,
    ) -> Result<(), RepositoryError> {
        unreachable!("no Loop node in these graphs")
    }

    fn advance_loop_round(
        &self,
        _scope_id: &WorkflowScopeId,
        _advance: &crate::workflow_run::engine::ports::LoopRoundAdvance,
        _now: i64,
    ) -> Result<AdvanceWorkflowRunResult, RepositoryError> {
        unreachable!("no Loop node in these graphs")
    }

    fn find_execution_context(
        &self,
        _run_id: &WorkflowRunId,
    ) -> Result<Option<ExecutionContext>, RepositoryError> {
        Ok(Some(self.lock().context.clone()))
    }

    fn list_node_runs(
        &self,
        _run_id: &WorkflowRunId,
    ) -> Result<Vec<WorkflowNodeRun>, RepositoryError> {
        Ok(self.lock().node_runs.clone())
    }

    fn find_last_failed_attempt(
        &self,
        _run_id: &WorkflowRunId,
        _node_id: &str,
        _iteration: Option<u32>,
    ) -> Result<Option<WorkflowNodeRun>, RepositoryError> {
        Ok(None)
    }

    fn bind_node_run_session(
        &self,
        _node_run_id: &WorkflowNodeRunId,
        _session_id: &SessionId,
        _now: i64,
    ) -> Result<super::BindWorkflowNodeSessionResult, RepositoryError> {
        Ok(super::BindWorkflowNodeSessionResult::NotFound)
    }

    fn find_node_run_by_session_id(
        &self,
        _session_id: &SessionId,
    ) -> Result<Option<WorkflowNodeRun>, RepositoryError> {
        Ok(None)
    }

    fn find_node_run_by_id(
        &self,
        node_run_id: &WorkflowNodeRunId,
    ) -> Result<Option<WorkflowNodeRun>, RepositoryError> {
        Ok(self
            .lock()
            .node_runs
            .iter()
            .find(|node| node.id == *node_run_id)
            .cloned())
    }

    fn transition_node_run_status(
        &self,
        _node_run_id: &WorkflowNodeRunId,
        _from: WorkflowNodeStatus,
        _to: WorkflowNodeStatus,
        _now: i64,
    ) -> Result<AdvanceWorkflowRunResult, RepositoryError> {
        Ok(AdvanceWorkflowRunResult::NotFound)
    }

    fn start_run(
        &self,
        _run_id: &WorkflowRunId,
        _start_node_run: &NodeRunToStart,
        _now: i64,
    ) -> Result<StartWorkflowRunResult, RepositoryError> {
        Ok(StartWorkflowRunResult::Current)
    }

    fn start_ready_nodes(
        &self,
        _run_id: &WorkflowRunId,
        node_runs: &[NodeRunToStart],
        _now: i64,
    ) -> Result<(), RepositoryError> {
        let mut state = self.lock();
        for node_run in node_runs {
            state.row_sequence += 1;
            let row_id = WorkflowNodeRunId::new(format!("nr-{}", state.row_sequence));
            let node_id = node_run.node_id.clone();
            let node_type = node_run.node_type.clone();
            let input = node_run.input.clone();
            state.started_ready.push(node_id.clone());
            state.node_runs.push(WorkflowNodeRun::new(
                row_id,
                WorkflowRunId::new("run-1"),
                WorkflowScopeId::new("root:run-1"),
                node_id,
                node_type,
                None,
                WorkflowNodeStatus::Running,
                input,
                None,
                None,
                None,
                Some(1),
                None,
                AuditFields::new(1, 1, false),
            ));
        }
        Ok(())
    }

    fn complete_node(
        &self,
        node_run_id: &WorkflowNodeRunId,
        output: Option<String>,
        _structured_output: Option<Value>,
        _stop_reason: Option<String>,
        _file_changes: Vec<FileChange>,
        _payload_patch: Option<&Value>,
        _now: i64,
    ) -> Result<AdvanceWorkflowRunResult, RepositoryError> {
        let (node_id, node_type) = {
            let mut state = self.lock();
            let Some(node) = state
                .node_runs
                .iter_mut()
                .find(|node| node.id == *node_run_id)
            else {
                return Ok(AdvanceWorkflowRunResult::NotFound);
            };
            if !matches!(
                node.status,
                WorkflowNodeStatus::Running | WorkflowNodeStatus::Pending
            ) {
                return Ok(AdvanceWorkflowRunResult::NotRunning);
            }
            node.status = WorkflowNodeStatus::Succeeded;
            node.output = output.clone();
            node.finished_at = Some(40);
            (node.node_id.clone(), node.node_type.clone())
        };
        // Mirror the repository's per-type payload write (update_run_execution_state).
        let mut state = self.lock();
        let mut payload: WorkflowRunPayload =
            serde_json::from_str(state.context.run.payload.as_deref().unwrap()).unwrap();
        match node_type.as_str() {
            "condition" => {
                if let Some(output) = output.as_deref() {
                    payload
                        .condition_decisions
                        .insert(node_id.clone(), output.to_string());
                }
            }
            "aggregator" => {
                if let Some(output) = output.as_deref() {
                    let value: Value = serde_json::from_str(output).unwrap();
                    payload
                        .variable_pool
                        .set(&format!("{}.output", node_id), &node_id, value)
                        .unwrap();
                }
            }
            other => {
                if let Some(output) = output.as_deref() {
                    let selector = format!("{}.output", node_id);
                    if payload.variable_pool.catalog.contains_key(&selector) {
                        payload
                            .variable_pool
                            .set(&selector, &node_id, Value::String(output.to_string()))
                            .unwrap();
                    }
                }
                let _ = other;
            }
        }
        state.context.run.payload = Some(serde_json::to_string(&payload).unwrap());
        Ok(AdvanceWorkflowRunResult::Advanced)
    }

    fn fail_node(
        &self,
        node_run_id: &WorkflowNodeRunId,
        failure: NodeFailure,
        propagation: FailurePropagation,
        _now: i64,
    ) -> Result<AdvanceWorkflowRunResult, RepositoryError> {
        let mut state = self.lock();
        let Some(node) = state
            .node_runs
            .iter_mut()
            .find(|node| node.id == *node_run_id)
        else {
            return Ok(AdvanceWorkflowRunResult::NotFound);
        };
        if node.status != WorkflowNodeStatus::Running {
            return Ok(AdvanceWorkflowRunResult::NotRunning);
        }
        node.status = WorkflowNodeStatus::Failed;
        node.error = Some(failure.message.clone());
        state.last_failure = Some((node.node_id.clone(), failure.kind, failure.message.clone()));
        if propagation == FailurePropagation::Run {
            state.context.run.status = WorkflowRunStatus::Failed;
        }
        Ok(AdvanceWorkflowRunResult::Advanced)
    }

    fn start_iteration_round(
        &self,
        _run_id: &WorkflowRunId,
        _owner_node_id: &str,
        _round: u32,
        _item: &Value,
        _node_runs: &[NodeRunToStart],
        _now: i64,
    ) -> Result<AdvanceWorkflowRunResult, RepositoryError> {
        Ok(AdvanceWorkflowRunResult::NotFound)
    }

    fn settle_iteration_round(
        &self,
        _run_id: &WorkflowRunId,
        _owner_node_id: &str,
        _round: u32,
        _entry: crate::workflow_run::engine::RoundOutcome,
        _continuation: crate::workflow_run::engine::IterationRoundContinuation,
        _now: i64,
    ) -> Result<AdvanceWorkflowRunResult, RepositoryError> {
        Ok(AdvanceWorkflowRunResult::NotFound)
    }

    fn complete_iteration_node(
        &self,
        _node_run_id: &WorkflowNodeRunId,
        _owner_node_id: &str,
        _exposed: &[(String, Value)],
        _output: Option<String>,
        _now: i64,
    ) -> Result<AdvanceWorkflowRunResult, RepositoryError> {
        Ok(AdvanceWorkflowRunResult::NotFound)
    }

    fn record_node_checkpoint(
        &self,
        _node_run_id: &WorkflowNodeRunId,
        _snapshot_id: &str,
        _checkpoint: Option<&str>,
        _checkpoint_error: Option<&str>,
        _now: i64,
    ) -> Result<(), RepositoryError> {
        Ok(())
    }

    fn record_node_injected_failure(
        &self,
        _node_run_id: &WorkflowNodeRunId,
        _text: &str,
    ) -> Result<(), RepositoryError> {
        Ok(())
    }

    fn record_node_ai_diagnosis(
        &self,
        _node_run_id: &WorkflowNodeRunId,
        _diagnosis_json: &str,
        _now: i64,
    ) -> Result<(), RepositoryError> {
        Ok(())
    }

    fn finish_run(
        &self,
        _run_id: &WorkflowRunId,
        _output: Option<String>,
        _now: i64,
    ) -> Result<(), RepositoryError> {
        let mut state = self.lock();
        state.finish_run_calls += 1;
        state.context.run.status = WorkflowRunStatus::Succeeded;
        Ok(())
    }

    fn cancel_run(
        &self,
        _run_id: &WorkflowRunId,
        _now: i64,
    ) -> Result<CancelWorkflowRunResult, RepositoryError> {
        Ok(CancelWorkflowRunResult::NotFound)
    }

    fn restart_run(
        &self,
        _run_id: &WorkflowRunId,
        _now: i64,
    ) -> Result<RestartWorkflowRunResult, RepositoryError> {
        Ok(RestartWorkflowRunResult::NotFound)
    }

    fn resume_from_failure(
        &self,
        _run_id: &WorkflowRunId,
        _node_ids_to_clear: &[String],
        _now: i64,
    ) -> Result<ResumeWorkflowRunResult, RepositoryError> {
        Ok(ResumeWorkflowRunResult::NotResumable)
    }

    fn switch_run_snapshot(
        &self,
        _run_id: &WorkflowRunId,
        _snapshot_id: &WorkflowSnapshotId,
        _payload_json: &str,
        _now: i64,
    ) -> Result<bool, RepositoryError> {
        Ok(true)
    }

    fn update_run_input(
        &self,
        _run_id: &WorkflowRunId,
        _input: Option<String>,
        _variables: BTreeMap<String, Value>,
        _now: i64,
    ) -> Result<super::UpdateWorkflowRunInputResult, RepositoryError> {
        Ok(super::UpdateWorkflowRunInputResult::NotFound)
    }

    fn list_recoverable_runs(&self) -> Result<Vec<WorkflowRunId>, RepositoryError> {
        Ok(Vec::new())
    }

    fn fail_orphaned_node_runs(
        &self,
        _run_ids: &[WorkflowRunId],
        _now: i64,
    ) -> Result<(), RepositoryError> {
        Ok(())
    }

    fn fail_interrupted_node_runs(
        &self,
        _run_id: &WorkflowRunId,
        _node_run_ids: &[WorkflowNodeRunId],
        _now: i64,
    ) -> Result<(), RepositoryError> {
        Ok(())
    }
}

/// Branch A active, branch B inactive: the aggregator passes A's output through and the run
/// completes. Executed twice over the same frozen graph string to stand in for save → reload
/// → execute.
#[test]
fn active_branch_a_output_flows_through_the_aggregator() {
    for _ in 0..2 {
        let harness = Harness::new(JOIN_GRAPH, Some("a"), &[("start.input", json!("a"))]);
        harness.drive();
        assert_eq!(harness.node_status("a"), WorkflowNodeStatus::Running);
        assert_eq!(harness.node_status("b"), WorkflowNodeStatus::Pending);
        harness.complete_agent("a", "A-output");
        assert_eq!(harness.node_status("agg"), WorkflowNodeStatus::Succeeded);
        assert_eq!(harness.node_output("agg"), Some("\"A-output\"".to_string()));
        assert_eq!(harness.pool_value("agg.output"), Some(json!("A-output")));
        assert_eq!(harness.node_status("out"), WorkflowNodeStatus::Succeeded);
        assert_eq!(harness.run_status(), WorkflowRunStatus::Succeeded);
        assert_eq!(harness.lock().finish_run_calls, 1);
    }
}

/// Branch B active (condition selects else): the aggregator passes B's output through.
#[test]
fn active_branch_b_output_flows_through_the_aggregator() {
    let harness = Harness::new(JOIN_GRAPH, Some("b"), &[("start.input", json!("b"))]);
    harness.drive();
    assert_eq!(harness.node_status("b"), WorkflowNodeStatus::Running);
    assert_eq!(harness.node_status("a"), WorkflowNodeStatus::Pending);
    harness.complete_agent("b", "B-output");
    assert_eq!(harness.node_status("agg"), WorkflowNodeStatus::Succeeded);
    assert_eq!(harness.pool_value("agg.output"), Some(json!("B-output")));
    assert_eq!(harness.node_output("agg"), Some("\"B-output\"".to_string()));
    assert_eq!(harness.run_status(), WorkflowRunStatus::Succeeded);
}

/// Parallel fan-in with both candidates assigned: the declaration order decides, regardless of
/// which branch finished first.
#[test]
fn parallel_fan_in_uses_the_declared_priority() {
    let harness = Harness::new(PARALLEL_GRAPH, Some("kickoff"), &[]);
    harness.drive();
    harness.complete_agent("a", "A-output");
    // Only A has finished, so the aggregator must not have started yet.
    assert_eq!(harness.node_status("agg"), WorkflowNodeStatus::Pending);
    harness.complete_agent("b", "B-output");
    assert_eq!(harness.node_status("agg"), WorkflowNodeStatus::Succeeded);
    // The selector list declares `b` first even though `a` finished first.
    assert_eq!(harness.pool_value("agg.output"), Some(json!("B-output")));
    assert_eq!(harness.run_status(), WorkflowRunStatus::Succeeded);
}

/// A falsy boolean Start variable is an assigned candidate and passes through unchanged.
#[test]
fn falsy_boolean_passes_through_the_aggregator() {
    let harness = Harness::new(START_EDGE_GRAPH, Some("kickoff"), &[]);
    harness.drive();
    assert_eq!(harness.node_status("agg"), WorkflowNodeStatus::Succeeded);
    assert_eq!(harness.pool_value("agg.output"), Some(json!(false)));
    assert_eq!(harness.node_output("agg"), Some("false".to_string()));
    assert_eq!(harness.run_status(), WorkflowRunStatus::Succeeded);
}

/// Every candidate unassigned: the aggregator fails the run with the stable
/// `aggregator_no_match` kind.
#[test]
fn no_assigned_candidate_fails_with_the_aggregator_kind() {
    let harness = Harness::new(NO_MATCH_GRAPH, Some("kickoff"), &[]);
    harness.drive();
    assert_eq!(harness.node_status("agg"), WorkflowNodeStatus::Failed);
    assert_eq!(harness.run_status(), WorkflowRunStatus::Failed);
    let (node_id, kind, message) = harness.lock().last_failure.clone().unwrap();
    assert_eq!(node_id, "agg");
    assert_eq!(kind, NodeFailureKind::AggregatorNoMatch);
    assert!(
        message.contains("start.mode") && message.contains("no assigned variable"),
        "unexpected message: {message}"
    );
}
