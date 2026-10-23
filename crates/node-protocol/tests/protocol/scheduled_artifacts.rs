//! Wire contract of the scheduled-artifact upload execution.

use super::support::*;
use ora_node_protocol::*;
use serde_json::json;

/// The upload names the ended session whose declaration it carries, and nothing else.
fn deliver() -> Case {
    Case {
        message: Message::Controller(ControllerToNodeMessage::DeliverScheduledArtifacts(
            DeliverScheduledArtifactsMessage {
                protocol_version: CURRENT_PROTOCOL_VERSION,
                operation_id: OperationId::new("run-1"),
                execution_id: ExecutionId::new("exec-upload"),
                payload: DeliverScheduledArtifacts {
                    spec: DeliverScheduledArtifactsSpec {
                        session_execution_id: ExecutionId::new("session-1"),
                    },
                },
            },
        )),
        wire: json!({"message_type": "deliver_scheduled_artifacts", "protocol_version": 1,
            "operation_id": "run-1", "execution_id": "exec-upload",
            "payload": {"spec": {"session_execution_id": "session-1"}}}),
    }
}

/// The upload command has a fixed wire shape and the shared envelope guarantees.
#[tokio::test]
async fn deliver_scheduled_artifacts_round_trips() -> Result<(), TestError> {
    let case = deliver();
    case.assert_wire().await?;
    case.assert_round_trip().await?;
    case.assert_envelope_rejections().await?;
    case.assert_fields(
        &["/payload/spec/session_execution_id"],
        &[
            ("/operation_id", "operation_id"),
            ("/execution_id", "execution_id"),
            ("/payload/spec/session_execution_id", "session_execution_id"),
        ],
    )
    .await
}

/// A delivered result reports what the Node measured; a failure names one of the closed reasons.
#[tokio::test]
async fn scheduled_artifacts_results_round_trip() -> Result<(), TestError> {
    let delivered = Case {
        message: Message::Node(NodeToControllerMessage::ScheduledArtifactsResult(
            ScheduledArtifactsResultMessage {
                protocol_version: CURRENT_PROTOCOL_VERSION,
                operation_id: OperationId::new("run-1"),
                execution_id: ExecutionId::new("exec-upload"),
                sequence: Sequence::new(/*value*/ 1),
                payload: ScheduledArtifactsExecutionResult::Delivered(
                    ScheduledArtifactsDelivered {
                        artifacts: vec![StoredObject {
                            key: ObjectKey::new("scheduled-runs/tenant/run/0"),
                            size: 5,
                            sha256: Sha256Digest::new("ab".repeat(32)),
                        }],
                    },
                ),
            },
        )),
        wire: json!({"message_type": "scheduled_artifacts_result", "protocol_version": 1,
            "operation_id": "run-1", "execution_id": "exec-upload", "sequence": 1,
            "payload": {"kind": "delivered", "result": {"artifacts": [{
                "key": "scheduled-runs/tenant/run/0", "size": 5, "sha256": "ab".repeat(32)}]}}}),
    };
    delivered.assert_wire().await?;
    delivered.assert_round_trip().await?;

    let failed = Case {
        message: Message::Node(NodeToControllerMessage::ScheduledArtifactsResult(
            ScheduledArtifactsResultMessage {
                protocol_version: CURRENT_PROTOCOL_VERSION,
                operation_id: OperationId::new("run-1"),
                execution_id: ExecutionId::new("exec-upload"),
                sequence: Sequence::new(/*value*/ 2),
                payload: ScheduledArtifactsExecutionResult::Failed(ScheduledArtifactsFailed {
                    reason: ScheduledArtifactsFailureReason::UploadFailed,
                }),
            },
        )),
        wire: json!({"message_type": "scheduled_artifacts_result", "protocol_version": 1,
            "operation_id": "run-1", "execution_id": "exec-upload", "sequence": 2,
            "payload": {"kind": "failed", "result": {"reason": "upload_failed"}}}),
    };
    failed.assert_wire().await?;
    failed.assert_round_trip().await?;
    Ok(())
}
