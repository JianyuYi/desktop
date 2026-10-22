//! Limit policies exercise the production scheduler, SQLite completion, and restart replay.
use super::test_fixture::{ClockAt, NoopExecutor, bootstrap, started_run_with};
use ora_application::{
    UuidWorkflowNodeRunIdGenerator, WorkflowRunEngine, WorkflowRunEngineRepository,
    WorkflowRunRepository,
};
use ora_db::{SqliteWorkflowRunEngineRepository, SqliteWorkflowRunRepository};
use ora_domain::{WorkflowNodeStatus, WorkflowRunStatus};
use ora_logging::with_trace_logging;
use pretty_assertions::assert_eq;
use serde_json::{Value, json};

/// Uses a real downstream Output to prove the loop publishes its final round's value.
fn graph() -> Value {
    json!({
        "schemaVersion": 2,
        "nodes": [
            {"id":"start","data":{"kind":"start"}},
            {"id":"loop","data":{"kind":"loop","loopConfig":{
                "maxIterations":2,"maxIterationsBehavior":"succeed",
                "variables":[{"name":"draft","valueType":"string","initial":{"kind":"constant","value":"seed"},"feedback":["writer","output"]}],
                "until":{"logic":"and","conditions":[{"variableSelector":["writer","output"],"operator":"equals","value":"done"}]},
                "outputs":[{"name":"result","variableSelector":["writer","output"]}]
            }}},
            {"id":"entry","data":{"kind":"start","containerId":"loop"}},
            {"id":"writer","data":{"kind":"agent","containerId":"loop","agentConfig":{"retry":{"enabled":false,"maxRetries":0,"initialDelaySeconds":0},"executor":{"agentCli":"c","modelId":"m"},"prompt":"Revise {{#loop.draft#}}"}}},
            {"id":"out","data":{"kind":"output","outputs":[{"name":"result","variableSelector":["loop","result"]}]}}
        ],
        "edges":[{"source":"start","target":"loop"},{"source":"loop","target":"out"},{"source":"entry","target":"writer"}]
    })
}

/// A crash after the last child's commit still completes exactly N rounds and releases downstream.
#[test]
fn loop_limit_success_recovers_and_ignores_duplicate_callbacks() {
    with_trace_logging(|| {
        let (temp, pool) = bootstrap();
        let (id, nodes, engine) =
            started_run_with(&temp, &pool, &graph().to_string(), NoopExecutor);
        let first = nodes.iter().find(|node| node.node_id == "writer").unwrap();
        engine
            .complete_node(
                &id,
                &first.id,
                Some("first".into()),
                /*structured_output*/ None,
                /*stop_reason*/ None,
                vec![],
            )
            .unwrap();
        let repository = SqliteWorkflowRunEngineRepository::new(pool.clone());
        let nodes = repository.list_node_runs(&id).unwrap();
        let last = nodes
            .iter()
            .find(|node| node.node_id == "writer" && node.status == WorkflowNodeStatus::Running)
            .unwrap();
        repository
            .complete_node(
                &last.id,
                Some("last".into()),
                /*structured_output*/ None,
                /*stop_reason*/ None,
                vec![],
                /*payload_patch*/ None,
                /*now*/ 50,
            )
            .unwrap();
        // Only durable graph and round state survive; the fresh engine has no loop policy cache.
        drop(engine);
        let recovered = WorkflowRunEngine::new(
            repository.clone(),
            NoopExecutor,
            UuidWorkflowNodeRunIdGenerator,
            ClockAt(60),
        );
        recovered.resume(&id).unwrap();
        let run_repository = SqliteWorkflowRunRepository::new(pool);
        let run = run_repository.find_run(&id).unwrap().unwrap();
        assert_eq!(
            (run.status, run.output.clone(), run.error.clone()),
            (
                WorkflowRunStatus::Succeeded,
                Some(r#"{"result":"last"}"#.into()),
                None
            )
        );
        let nodes = repository.list_node_runs(&id).unwrap();
        assert_eq!(
            nodes.iter().filter(|node| node.node_id == "writer").count(),
            2
        );
        let parent = nodes.iter().find(|node| node.node_id == "loop").unwrap();
        let payload: Value = serde_json::from_str(parent.payload.as_deref().unwrap()).unwrap();
        assert_eq!(
            (
                parent.status,
                parent.output.clone(),
                payload["stop_reason"].clone()
            ),
            (
                WorkflowNodeStatus::Succeeded,
                Some(r#"{"result":"last"}"#.into()),
                json!("loop_max_iterations")
            )
        );
        assert_eq!(nodes.iter().filter(|node| node.node_id == "out" && node.status == WorkflowNodeStatus::Succeeded).count(), 1);
        recovered
            .complete_node(
                &id,
                &last.id,
                Some("late".into()),
                /*structured_output*/ None,
                /*stop_reason*/ None,
                vec![],
            )
            .unwrap();
        recovered.resume(&id).unwrap();
        assert_eq!(repository.list_node_runs(&id).unwrap(), nodes);
        assert_eq!(run_repository.find_run(&id).unwrap().unwrap(), run);
    });
}

/// Single-round exhaustion distinguishes default/explicit failure, success, and satisfied conditions.
#[test]
fn loop_limit_policy_controls_only_exhaustion() {
    with_trace_logging(|| {
        for (policy, reply, conditions, status, reason) in [
            (None, "again", true, WorkflowRunStatus::Failed, None),
            (Some("fail"), "again", true, WorkflowRunStatus::Failed, None),
            (
                Some("succeed"),
                "again",
                true,
                WorkflowRunStatus::Succeeded,
                Some("loop_max_iterations"),
            ),
            (
                Some("succeed"),
                "again",
                false,
                WorkflowRunStatus::Succeeded,
                Some("loop_max_iterations"),
            ),
            (
                Some("succeed"),
                "done",
                true,
                WorkflowRunStatus::Succeeded,
                Some("loop_succeeded"),
            ),
        ] {
            let (temp, pool) = bootstrap();
            let mut graph = graph();
            let config = &mut graph["nodes"][1]["data"]["loopConfig"];
            config["maxIterations"] = json!(1);
            if let Some(policy) = policy {
                config["maxIterationsBehavior"] = json!(policy);
            } else {
                config
                    .as_object_mut()
                    .unwrap()
                    .remove("maxIterationsBehavior");
            }
            if !conditions {
                config["until"]["conditions"] = json!([]);
            }
            let (id, nodes, engine) =
                started_run_with(&temp, &pool, &graph.to_string(), NoopExecutor);
            let writer = nodes.iter().find(|node| node.node_id == "writer").unwrap();
            engine
                .complete_node(
                    &id,
                    &writer.id,
                    Some(reply.into()),
                    /*structured_output*/ None,
                    /*stop_reason*/ None,
                    vec![],
                )
                .unwrap();
            let repository = SqliteWorkflowRunRepository::new(pool);
            let run = repository.find_run(&id).unwrap().unwrap();
            let nodes = repository.list_node_runs(&id).unwrap();
            let parent = nodes.iter().find(|node| node.node_id == "loop").unwrap();
            let payload: Value =
                serde_json::from_str(parent.payload.as_deref().unwrap_or("{}")).unwrap();
            assert_eq!(
                (run.status, payload["stop_reason"].as_str()),
                (status, reason)
            );
            assert_eq!(
                nodes.iter().filter(|node| node.node_id == "writer").count(),
                1
            );
            assert_eq!(
                nodes.iter().any(|node| node.node_id == "out"),
                status == WorkflowRunStatus::Succeeded
            );
        }
    });
}

/// Success on exhaustion cannot turn a failed child or an unavailable export into success.
#[test]
fn loop_limit_success_does_not_mask_execution_or_output_errors() {
    with_trace_logging(|| {
        for failure in ["execution", "output"] {
            let (temp, pool) = bootstrap();
            let mut graph = graph();
            let config = &mut graph["nodes"][1]["data"]["loopConfig"];
            config["maxIterations"] = json!(1);
            config["until"]["conditions"] = json!([]);
            config["variables"][0]["feedback"] = json!(["loop", "draft"]);
            let (id, nodes, engine) =
                started_run_with(&temp, &pool, &graph.to_string(), NoopExecutor);
            let writer = nodes.iter().find(|node| node.node_id == "writer").unwrap();
            if failure == "execution" {
                engine
                    .fail_node(
                        &id,
                        &writer.id,
                        ora_application::NodeFailure::new(
                            ora_application::NodeFailureKind::InvalidRunPayload,
                            "child failed",
                        ),
                    )
                    .unwrap();
            } else {
                engine
                    .complete_node(
                        &id,
                        &writer.id,
                        /*output*/ None,
                        /*structured_output*/ None,
                        /*stop_reason*/ None,
                        vec![],
                    )
                    .unwrap();
            }
            let repository = SqliteWorkflowRunRepository::new(pool);
            let run = repository.find_run(&id).unwrap().unwrap();
            let nodes = repository.list_node_runs(&id).unwrap();
            assert_eq!(
                (
                    run.status,
                    run.output,
                    nodes.iter().any(|node| node.node_id == "out")
                ),
                (WorkflowRunStatus::Failed, None, false)
            );
            assert!(run.error.is_some());
        }
    });
}
