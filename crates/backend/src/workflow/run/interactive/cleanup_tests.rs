//! A dropped human turn must park its node with the completion snapshot completion later reads.

use super::super::completion_snapshot::{
    CompletionBlocker, CompletionBlockerKind, CompletionSnapshot, TurnKey,
    parse_completion_snapshot,
};
use super::session::{begin_human_turn, end_human_turn};
use crate::workflow::run::test_fixture::{
    AGENT_GRAPH, bind_and_park, bootstrap, end_fixture_turn, locks, recording_transitions,
    run_test, started_run, write_open_turn,
};
use agent_client_protocol_schema::v1::StopReason;
use ora_application::WorkflowRunRepository;
use ora_db::{RepositoryPool, SqliteWorkflowRunRepository};
use ora_domain::{WorkflowNodeRun, WorkflowNodeStatus, WorkflowRunId};
use ora_utils::hash::sha256_hex;
use pretty_assertions::assert_eq;
use serde_json::Value;
use std::time::Duration;

/// Returns the named node's persisted row so its parked status and snapshot assert together.
fn parked_node(pool: &RepositoryPool, run_id: &WorkflowRunId, node_id: &str) -> WorkflowNodeRun {
    SqliteWorkflowRunRepository::new(pool.clone())
        .list_node_runs(run_id)
        .unwrap()
        .into_iter()
        .find(|node| node.node_id == node_id)
        .expect("agent node run")
}

/// A `TurnEnded` appended while cleanup polls still produces the settled snapshot.
///
/// The runtime may confirm the cancelled operation before its history flush lands, so cleanup
/// must not park an unknown snapshot while the turn boundary is already on its way.
#[test]
fn cleanup_polls_until_the_turn_ended_is_durable() {
    run_test(async {
        let (temp, pool) = bootstrap();
        let (run_id, node_runs) = started_run(&temp, &pool, AGENT_GRAPH);
        let agent = node_runs
            .iter()
            .find(|node| node.node_id == "agent")
            .unwrap();
        let (session_id, node_run_id) = bind_and_park(&pool, agent);
        let sessions_root = temp.path().join("sessions");
        write_open_turn(&sessions_root, session_id.as_ref(), "go", "done");
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

        let late_root = sessions_root.clone();
        let late_session = session_id.to_string();
        let late_boundary = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            end_fixture_turn(&late_root, &late_session, StopReason::EndTurn);
        });
        end_human_turn(&pool, &sessions_root, &transitions, &node_run_id)
            .await
            .unwrap();
        late_boundary.await.unwrap();

        let parked = parked_node(&pool, &run_id, "agent");
        assert_eq!(parked.status, WorkflowNodeStatus::Pending);
        assert_eq!(
            parse_completion_snapshot(parked.payload.as_deref()),
            Some(CompletionSnapshot {
                user_seq: Some(0),
                turn_key: Some(TurnKey {
                    user_seq: 0,
                    ended_seq: 2,
                }),
                stop_reason: Some("end_turn".to_string()),
                blockers: Vec::new(),
                warnings: Vec::new(),
                output_digest: Some(format!("sha256:{}", sha256_hex(b"done"))),
                provisional: false,
            })
        );
        assert_eq!(
            *recording.published.lock().unwrap(),
            vec![run_id.to_string(), run_id.to_string()],
            "beginning and parking each publish one run invalidation"
        );
    });
}

/// A turn that never writes `TurnEnded` parks provisionally instead of stranding the node.
///
/// The provisional snapshot records that the output is unknown, so the node leaves `Running` and
/// a later completion can repair the snapshot from history (or be explicitly forced).
#[test]
fn cleanup_parks_provisionally_when_the_turn_never_ends() {
    run_test(async {
        let (temp, pool) = bootstrap();
        let (run_id, node_runs) = started_run(&temp, &pool, AGENT_GRAPH);
        let agent = node_runs
            .iter()
            .find(|node| node.node_id == "agent")
            .unwrap();
        let (session_id, node_run_id) = bind_and_park(&pool, agent);
        let sessions_root = temp.path().join("sessions");
        write_open_turn(&sessions_root, session_id.as_ref(), "go", "still going");
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

        end_human_turn(&pool, &sessions_root, &transitions, &node_run_id)
            .await
            .unwrap();

        let parked = parked_node(&pool, &run_id, "agent");
        assert_eq!(parked.status, WorkflowNodeStatus::Pending);
        assert_eq!(
            parse_completion_snapshot(parked.payload.as_deref()),
            Some(CompletionSnapshot {
                user_seq: None,
                turn_key: None,
                stop_reason: None,
                blockers: vec![CompletionBlocker {
                    kind: CompletionBlockerKind::TurnUnknown,
                    params: Value::Null,
                }],
                warnings: vec!["history_unavailable".to_string()],
                output_digest: None,
                provisional: true,
            })
        );
        assert_eq!(
            *recording.published.lock().unwrap(),
            vec![run_id.to_string(), run_id.to_string()],
            "the provisional park still publishes its run invalidation"
        );
    });
}
