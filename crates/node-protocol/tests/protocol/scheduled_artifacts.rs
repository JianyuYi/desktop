//! Wire contract of the scheduled-artifact upload execution.

use super::support::*;
use ora_node_protocol::*;
use serde_json::json;
use time::macros::datetime;

/// A digest that satisfies the canonical lowercase spelling Cloud verifies against.
const DIGEST: &str = "abababababababababababababababababababababababababababababababab";

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

/// A controlled upload carries a runtime permit for exactly this execution and operation; the
/// input names no Node, so nothing else about the permit can be checked here.
#[tokio::test]
async fn controlled_upload_requires_a_matching_runtime_permit() -> Result<(), TestError> {
    let Message::Controller(ControllerToNodeMessage::DeliverScheduledArtifacts(command)) =
        deliver().message
    else {
        unreachable!("the upload case is a controller DeliverScheduledArtifacts message");
    };
    let binding = RuntimeBinding {
        tenant_id: "tenant".into(),
        workspace_id: "workspace".into(),
        sandbox_id: "sandbox".into(),
        runtime_generation: 1,
        node_id: "node".into(),
        node_incarnation_id: "incarnation".into(),
        node_instance_id: "instance".into(),
        controller_epoch: 1,
        control_epoch: 1,
        control_version: 1,
        session_id: "session".into(),
        actor_user_id: "actor".into(),
        operation_id: String::new(),
        execution_id: command.execution_id.as_str().into(),
        node_operation_id: command.operation_id.as_str().into(),
        input_closed: false,
        issued_at_ms: 1000,
        expires_at_ms: 31_000,
    };
    let envelope = ControlledDeliverScheduledArtifacts { binding, command };
    let message = ControllerToNodeMessage::ControlledDeliverScheduledArtifacts(envelope.clone());
    assert!(message.validate().is_ok());
    pretty_assertions::assert_eq!(round_trip_controller(message.clone()).await?, message);
    let mut changed = envelope.clone();
    changed.binding.node_operation_id = "wrong".into();
    assert!(changed.validate().is_err());
    changed = envelope.clone();
    changed.binding.execution_id = "wrong".into();
    assert!(changed.validate().is_err());
    changed = envelope;
    changed.binding.input_closed = true;
    assert!(changed.validate().is_err());
    Ok(())
}

/// Grants are requested by declaration index, because only Cloud may choose an object key.
#[tokio::test]
async fn scheduled_artifact_grants_are_addressed_by_declaration_index() -> Result<(), TestError> {
    let needed = Case {
        message: Message::Node(NodeToControllerMessage::ScheduledArtifactGrantNeeded(
            ScheduledArtifactGrantNeededMessage {
                protocol_version: CURRENT_PROTOCOL_VERSION,
                operation_id: OperationId::new("run-1"),
                execution_id: ExecutionId::new("exec-upload"),
                payload: ScheduledArtifactGrantNeeded {
                    node_id: NodeId::new("test-node"),
                    artifacts: vec![ScheduledArtifactUpload {
                        index: 0,
                        size_bytes: 5,
                        sha256: Sha256Digest::new(DIGEST),
                    }],
                },
            },
        )),
        wire: json!({"message_type": "scheduled_artifact_grant_needed", "protocol_version": 1,
            "operation_id": "run-1", "execution_id": "exec-upload",
            "payload": {"node_id": "test-node", "artifacts": [
                {"index": 0, "size_bytes": 5, "sha256": DIGEST}]}}),
    };
    needed.assert_wire().await?;
    needed.assert_round_trip().await?;
    needed.assert_envelope_rejections().await?;
    let mut wire = needed.wire.clone();
    replace(&mut wire, "/payload/artifacts", json!([]));
    reject_semantics(
        Peer::Node,
        &wire,
        "no artifacts",
        MessageValidationError::InvalidScheduledArtifactManifest,
    )
    .await?;
    let mut wire = needed.wire.clone();
    replace(
        &mut wire,
        "/payload/artifacts",
        json!([
            {"index": 0, "size_bytes": 5, "sha256": DIGEST},
            {"index": 0, "size_bytes": 5, "sha256": DIGEST}
        ]),
    );
    reject_semantics(
        Peer::Node,
        &wire,
        "duplicate index",
        MessageValidationError::InvalidScheduledArtifactManifest,
    )
    .await?;
    let mut wire = needed.wire.clone();
    replace(
        &mut wire,
        "/payload/artifacts/0/sha256",
        json!(DIGEST.to_uppercase()),
    );
    reject_semantics(
        Peer::Node,
        &wire,
        "uppercase digest",
        MessageValidationError::InvalidSha256,
    )
    .await?;

    let grants = Case {
        message: Message::Controller(ControllerToNodeMessage::ScheduledArtifactGrants(
            ScheduledArtifactGrantsMessage {
                protocol_version: CURRENT_PROTOCOL_VERSION,
                operation_id: OperationId::new("run-1"),
                execution_id: ExecutionId::new("exec-upload"),
                payload: ScheduledArtifactGrants {
                    grants: vec![ScheduledArtifactUploadGrant {
                        index: 0,
                        grant: ObjectUploadGrant {
                            object_key: ObjectKey::new("scheduled-runs/tenant/run/0"),
                            url: PresignedUrl::new("https://store.example.test/run/0?sig=secret"),
                            method: UploadMethod::Put,
                            headers: [("If-None-Match".to_owned(), "*".to_owned())].into(),
                            expires_at: datetime!(1970-01-01 0:00 UTC),
                        },
                    }],
                },
            },
        )),
        wire: json!({"message_type": "scheduled_artifact_grants", "protocol_version": 1,
            "operation_id": "run-1", "execution_id": "exec-upload",
            "payload": {"grants": [{"index": 0, "grant": {
                "object_key": "scheduled-runs/tenant/run/0",
                "url": "https://store.example.test/run/0?sig=secret",
                "method": "PUT",
                "headers": {"If-None-Match": "*"},
                "expires_at": "1970-01-01T00:00:00Z"}}]}}),
    };
    grants.assert_wire().await?;
    grants.assert_round_trip().await?;
    grants.assert_envelope_rejections().await?;
    let mut wire = grants.wire.clone();
    replace(&mut wire, "/payload/grants", json!([]));
    reject_semantics(
        Peer::Controller,
        &wire,
        "no grants",
        MessageValidationError::EmptyUploadGrant,
    )
    .await?;
    let mut wire = grants.wire.clone();
    replace(
        &mut wire,
        "/payload/grants/0/grant/url",
        json!("relative/path"),
    );
    reject_semantics(
        Peer::Controller,
        &wire,
        "relative upload URL",
        MessageValidationError::InvalidUploadGrant,
    )
    .await?;
    let mut wire = grants.wire.clone();
    replace(
        &mut wire,
        "/payload/grants/0/grant/object_key",
        json!("/absolute"),
    );
    reject_semantics(
        Peer::Controller,
        &wire,
        "absolute object key",
        MessageValidationError::InvalidObjectKey,
    )
    .await
}

/// A finished upload answers status queries with the evidence it retained; Cloud still settles on
/// the sequenced terminal event, never on a query.
#[tokio::test]
async fn completed_upload_status_carries_its_evidence() -> Result<(), TestError> {
    let evidence = ScheduledArtifactsEvidence {
        node: NodeRuntimeIdentity {
            node_id: NodeId::new("test-node"),
            incarnation_id: NodeIncarnationId::new("first"),
        },
        result: ScheduledArtifactsExecutionResult::Delivered(ScheduledArtifactsDelivered {
            artifacts: vec![StoredObject {
                key: ObjectKey::new("scheduled-runs/tenant/run/0"),
                size: 5,
                sha256: Sha256Digest::new(DIGEST),
            }],
        }),
    };
    let case = Case {
        message: Message::Node(NodeToControllerMessage::ExecutionStatus(
            ExecutionStatusMessage {
                protocol_version: CURRENT_PROTOCOL_VERSION,
                operation_id: OperationId::new("run-1"),
                execution_id: ExecutionId::new("exec-upload"),
                payload: ExecutionStatus {
                    stop_unconfirmed: Vec::new(),
                    node: evidence.node.clone(),
                    state: ExecutionState::Completed(ExecutionResult::ScheduledArtifacts(
                        Box::new(evidence),
                    )),
                },
            },
        )),
        wire: json!({"message_type": "execution_status", "protocol_version": 1,
            "operation_id": "run-1", "execution_id": "exec-upload",
            "payload": {"node": {"node_id": "test-node", "incarnation_id": "first"},
                "state": {"state": "completed", "result": {
                    "node": {"node_id": "test-node", "incarnation_id": "first"},
                    "result": {"kind": "delivered", "result": {"artifacts": [{
                        "key": "scheduled-runs/tenant/run/0", "size": 5, "sha256": DIGEST}]}}}}}}),
    };
    case.assert_wire().await?;
    case.assert_round_trip().await?;
    Ok(())
}

/// Cloud withdraws the grants of one upload with an identity-only message: a refusal grants nothing
/// and must not be mistaken for a grant reply with an empty set.
#[tokio::test]
async fn a_refused_upload_grant_round_trips() -> Result<(), TestError> {
    let case = Case {
        message: Message::Controller(ControllerToNodeMessage::ScheduledArtifactGrantsRefused(
            ScheduledArtifactGrantsRefusedMessage {
                protocol_version: CURRENT_PROTOCOL_VERSION,
                operation_id: OperationId::new("run-1"),
                execution_id: ExecutionId::new("exec-upload"),
            },
        )),
        wire: json!({"message_type": "scheduled_artifact_grants_refused", "protocol_version": 1,
            "operation_id": "run-1", "execution_id": "exec-upload"}),
    };
    case.assert_wire().await?;
    case.assert_round_trip().await?;
    // The refusal names an upload and nothing else, so the shared envelope checks are applied here
    // instead of through the fixture helper, which mutates a payload field this message has none of.
    reject_structure(Peer::Node, &case.wire, "opposite direction").await;
    let mut wire = case.wire.clone();
    replace(&mut wire, "/protocol_version", json!(2));
    reject_semantics(
        Peer::Controller,
        &wire,
        "protocol_version",
        MessageValidationError::UnsupportedProtocolVersion {
            actual: 2,
            expected: 1,
        },
    )
    .await?;
    let mut wire = case.wire.clone();
    replace(&mut wire, "/execution_id", json!(""));
    reject_semantics(
        Peer::Controller,
        &wire,
        "execution_id",
        MessageValidationError::EmptyField {
            field: "execution_id",
        },
    )
    .await
}
