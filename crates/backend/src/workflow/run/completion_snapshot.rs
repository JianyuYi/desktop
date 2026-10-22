//! Derives the completion snapshot persisted when an interactive node parks.
//!
//! The snapshot captures the turn that just settled — its identity, final assistant text digest,
//! stop reason, and declared-output blockers — so the read projection can show the same readiness
//! that `completeWorkflowNode` later re-validates. All inputs come from the session's durable
//! history so the park path and the completion path share one source.

use agent_client_protocol_schema::v1::{ContentBlock, SessionUpdate, StopReason};
use ora_application::AgentOutputContract;
use ora_history::{HistoryIntegrity, HistoryRecord, SessionHistory};
use ora_utils::hash::sha256_hex;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// Identifies one settled turn by the positions of its user message and its `TurnEnded`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TurnKey {
    pub user_seq: u32,
    pub ended_seq: u32,
}

/// A stable reason that an awaiting interactive node may not complete without an explicit force.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CompletionBlockerKind {
    TurnStopped,
    TurnTruncated,
    TurnRefused,
    TurnUnknown,
    StructuredInvalid,
    OutputMissing,
}

/// One blocker, with params carrying implementation detail the UI can localize.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CompletionBlocker {
    pub kind: CompletionBlockerKind,
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub params: Value,
}

impl CompletionBlocker {
    /// Builds a blocker without extra params.
    fn new(kind: CompletionBlockerKind) -> Self {
        Self {
            kind,
            params: Value::Null,
        }
    }

    /// Builds a blocker with a human-readable detail for the matching UI reason.
    fn with_message(kind: CompletionBlockerKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            params: json!({ "message": message.into() }),
        }
    }
}

/// The persisted readiness record for one parked interactive turn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CompletionSnapshot {
    /// The last user message that opened this turn; always present when the history has one.
    pub user_seq: Option<u32>,
    /// The settled turn identity, or `None` when this turn has no `TurnEnded`.
    pub turn_key: Option<TurnKey>,
    /// Snake-case stop reason of the settled turn, or `None` when the turn did not settle.
    pub stop_reason: Option<String>,
    pub blockers: Vec<CompletionBlocker>,
    pub warnings: Vec<String>,
    /// SHA-256 of the final assistant text; `None` when there is no settled turn.
    pub output_digest: Option<String>,
    /// True when cleanup timed out waiting for the actor and a later `prepare` may repair it.
    pub provisional: bool,
}

/// Wraps one snapshot in the node-run payload shape the projection parses.
pub(crate) fn completion_payload(snapshot: &CompletionSnapshot) -> Value {
    json!({ "completion": snapshot })
}

/// Builds the conservative snapshot used when the session history cannot be read at park time.
pub(crate) fn unknown_completion_snapshot(provisional: bool) -> CompletionSnapshot {
    CompletionSnapshot {
        user_seq: None,
        turn_key: None,
        stop_reason: None,
        blockers: vec![CompletionBlocker::new(CompletionBlockerKind::TurnUnknown)],
        warnings: vec!["history_unavailable".to_string()],
        output_digest: None,
        provisional,
    }
}

/// Parses the completion snapshot stored on a node-run payload, if it is present and valid.
pub(crate) fn parse_completion_snapshot(payload: Option<&str>) -> Option<CompletionSnapshot> {
    let payload: Value = serde_json::from_str(payload?).ok()?;
    serde_json::from_value(payload.get("completion")?.clone()).ok()
}

/// Returns the final assistant text of the last settled turn, using the same window as the
/// snapshot derivation so the completion commit and its digest cannot disagree.
pub(crate) fn completion_final_text(history: &SessionHistory) -> Option<String> {
    let user_seq = last_user_message_seq(history)?;
    let (ended_seq, _) = settled_turn(history, user_seq)?;
    final_assistant_text(history, user_seq, ended_seq)
}

/// Derives the completion snapshot for the last settled turn in `history`.
///
/// The final text window is `(userSeq, endedSeq]` and takes the last assistant message by `seq`,
/// including an empty one, matching the contract rather than the legacy "last non-empty" helper.
/// Retries have no attempt boundary in the current history format, so an abandoned attempt's text
/// can fall inside the window; that limitation is documented by the ADR and surfaced here rather
/// than hidden by a different text picker.
pub(crate) fn derive_completion_snapshot(
    history: &SessionHistory,
    output_contract: Option<&AgentOutputContract>,
    provisional: bool,
) -> CompletionSnapshot {
    let user_seq = last_user_message_seq(history);
    let turn = user_seq.and_then(|user_seq| settled_turn(history, user_seq));
    let mut warnings = Vec::new();
    let mut blockers = Vec::new();

    let (turn_key, stop_reason) = match turn {
        Some((ended_seq, reason)) => (
            Some(TurnKey {
                user_seq: user_seq.expect("a settled turn has a user message"),
                ended_seq,
            }),
            Some(super::executor::stop_reason_label(reason)),
        ),
        None => (None, None),
    };

    if let HistoryIntegrity::Damaged { unreadable_lines } = history.integrity {
        warnings.push(format!(
            "history_integrity_gap: {unreadable_lines} unreadable history line(s)"
        ));
        blockers.push(CompletionBlocker::new(CompletionBlockerKind::TurnUnknown));
    } else if turn.is_none() {
        blockers.push(CompletionBlocker::new(CompletionBlockerKind::TurnUnknown));
    }

    let final_text = turn.and_then(|(ended_seq, _)| {
        final_assistant_text(
            history,
            user_seq.expect("a settled turn has a user message"),
            ended_seq,
        )
    });
    let output_digest = turn.map(|_| {
        format!(
            "sha256:{}",
            sha256_hex(final_text.as_deref().unwrap_or_default().as_bytes())
        )
    });

    if let Some(stop_reason) = turn.map(|(_, reason)| reason) {
        match stop_reason {
            StopReason::EndTurn => {}
            StopReason::MaxTokens | StopReason::MaxTurnRequests => {
                blockers.push(CompletionBlocker::new(CompletionBlockerKind::TurnTruncated));
            }
            StopReason::Cancelled => {
                blockers.push(CompletionBlocker::new(CompletionBlockerKind::TurnStopped));
            }
            StopReason::Refusal => {
                blockers.push(CompletionBlocker::new(CompletionBlockerKind::TurnRefused));
            }
            _ => blockers.push(CompletionBlocker::new(CompletionBlockerKind::TurnUnknown)),
        }
    }

    // A missing final message is an empty final text; the contract decides whether that blocks.
    let final_text = final_text.unwrap_or_default();
    match output_contract {
        None | Some(AgentOutputContract::None) => {
            if final_text.is_empty() {
                warnings.push("empty_output".to_string());
            }
        }
        Some(AgentOutputContract::Text) => {
            if final_text.trim().is_empty() {
                blockers.push(CompletionBlocker::new(CompletionBlockerKind::OutputMissing));
            }
        }
        Some(AgentOutputContract::Structured { .. }) => {
            if let Err(error) = super::executor::apply_output_contract(
                output_contract,
                Some(final_text),
                "interactive-node",
            ) {
                blockers.push(CompletionBlocker::with_message(
                    CompletionBlockerKind::StructuredInvalid,
                    error.to_string(),
                ));
            }
        }
    }

    CompletionSnapshot {
        user_seq,
        turn_key,
        stop_reason,
        blockers,
        warnings,
        output_digest,
        provisional,
    }
}

/// Returns the `seq` of the last user message, which opens the turn under evaluation.
fn last_user_message_seq(history: &SessionHistory) -> Option<u32> {
    history
        .lines
        .iter()
        .filter(|line| {
            matches!(
                &line.record,
                HistoryRecord::Update { update, .. }
                    if matches!(update.as_ref(), SessionUpdate::UserMessageChunk(_))
            )
        })
        .map(|line| line.seq)
        .max()
}

/// Returns the last `TurnEnded` after `user_seq` with its position.
fn settled_turn(history: &SessionHistory, user_seq: u32) -> Option<(u32, StopReason)> {
    history
        .lines
        .iter()
        .filter_map(|line| {
            let HistoryRecord::TurnEnded { stop_reason } = &line.record else {
                return None;
            };
            (line.seq > user_seq).then_some((line.seq, *stop_reason))
        })
        .max_by_key(|(seq, _)| *seq)
}

/// Returns the last assistant message inside `(user_seq, ended_seq]`, including an empty one.
fn final_assistant_text(history: &SessionHistory, user_seq: u32, ended_seq: u32) -> Option<String> {
    history
        .lines
        .iter()
        .filter_map(|line| {
            if line.seq <= user_seq || line.seq > ended_seq {
                return None;
            }
            let HistoryRecord::Update { update, .. } = &line.record else {
                return None;
            };
            assistant_text(update.as_ref()).map(str::to_string)
        })
        .last()
}

/// Extracts the text block of one settled assistant message.
fn assistant_text(update: &SessionUpdate) -> Option<&str> {
    let SessionUpdate::AgentMessageChunk(chunk) = update else {
        return None;
    };
    let ContentBlock::Text(text) = &chunk.content else {
        return None;
    };
    Some(&text.text)
}

#[cfg(test)]
mod tests {
    use super::{CompletionBlockerKind, TurnKey, completion_payload, derive_completion_snapshot};
    use agent_client_protocol_schema::v1::{
        ContentBlock, ContentChunk, SessionUpdate, StopReason, TextContent,
    };
    use ora_application::{AgentOutputContract, StructuredTextExposure};
    use ora_history::{HistoryIntegrity, HistoryLine, HistoryRecord, SessionHistory};
    use ora_utils::hash::sha256_hex;
    use pretty_assertions::assert_eq;
    use serde_json::json;
    use std::num::NonZeroUsize;

    /// Builds a history with a complete timeline at zero next seq.
    fn history(lines: Vec<HistoryLine>, integrity: HistoryIntegrity) -> SessionHistory {
        SessionHistory {
            lines,
            next_seq: 0,
            integrity,
        }
    }

    /// Builds one user message line at `seq`.
    fn user(seq: u32, text: &str) -> HistoryLine {
        HistoryLine::new(
            "2026-10-22T10:00:00+08:00",
            seq,
            HistoryRecord::Update {
                update: Box::new(SessionUpdate::UserMessageChunk(ContentChunk::new(
                    ContentBlock::Text(TextContent::new(text.to_string())),
                ))),
                tool_timing: None,
            },
        )
    }

    /// Builds one assistant message line at `seq`.
    fn assistant(seq: u32, text: &str) -> HistoryLine {
        HistoryLine::new(
            "2026-10-22T10:00:00+08:00",
            seq,
            HistoryRecord::Update {
                update: Box::new(SessionUpdate::AgentMessageChunk(ContentChunk::new(
                    ContentBlock::Text(TextContent::new(text.to_string())),
                ))),
                tool_timing: None,
            },
        )
    }

    /// Builds one `TurnEnded` line at `seq`.
    fn ended(seq: u32, stop_reason: StopReason) -> HistoryLine {
        HistoryLine::new(
            "2026-10-22T10:00:00+08:00",
            seq,
            HistoryRecord::TurnEnded { stop_reason },
        )
    }

    /// A normal completed turn is ready with a full turn key and digest.
    #[test]
    fn completed_turn_is_ready() {
        let snapshot = derive_completion_snapshot(
            &history(
                vec![
                    user(1, "go"),
                    assistant(2, "done"),
                    ended(3, StopReason::EndTurn),
                ],
                HistoryIntegrity::Complete,
            ),
            None,
            false,
        );
        assert_eq!(snapshot.user_seq, Some(1));
        assert_eq!(
            snapshot.turn_key,
            Some(TurnKey {
                user_seq: 1,
                ended_seq: 3
            })
        );
        assert_eq!(snapshot.stop_reason.as_deref(), Some("end_turn"));
        assert!(snapshot.blockers.is_empty());
        assert_eq!(
            snapshot.output_digest,
            Some(format!("sha256:{}", sha256_hex(b"done")))
        );
        assert!(!snapshot.provisional);
    }

    /// Stopped, truncated and refused turns each report their own blocker.
    #[test]
    fn non_end_turns_are_blocked_by_kind() {
        for (reason, expected) in [
            (StopReason::Cancelled, CompletionBlockerKind::TurnStopped),
            (StopReason::MaxTokens, CompletionBlockerKind::TurnTruncated),
            (
                StopReason::MaxTurnRequests,
                CompletionBlockerKind::TurnTruncated,
            ),
            (StopReason::Refusal, CompletionBlockerKind::TurnRefused),
        ] {
            let snapshot = derive_completion_snapshot(
                &history(
                    vec![user(1, "go"), assistant(2, "partial"), ended(3, reason)],
                    HistoryIntegrity::Complete,
                ),
                None,
                false,
            );
            assert_eq!(
                snapshot.blockers.first().map(|blocker| blocker.kind),
                Some(expected),
                "unexpected blocker for {reason:?}"
            );
        }
    }

    /// A history without a user message has no turn and cannot be ready.
    #[test]
    fn missing_user_message_is_unknown() {
        let snapshot = derive_completion_snapshot(
            &history(vec![assistant(1, "hello")], HistoryIntegrity::Complete),
            None,
            false,
        );
        assert_eq!(snapshot.user_seq, None);
        assert_eq!(snapshot.turn_key, None);
        assert_eq!(snapshot.output_digest, None);
        assert_eq!(
            snapshot.blockers.first().map(|blocker| blocker.kind),
            Some(CompletionBlockerKind::TurnUnknown)
        );
    }

    /// A user message without a `TurnEnded` is unknown and has no digest.
    #[test]
    fn missing_turn_ended_is_unknown() {
        let snapshot = derive_completion_snapshot(
            &history(
                vec![user(1, "go"), assistant(2, "still going")],
                HistoryIntegrity::Complete,
            ),
            None,
            false,
        );
        assert_eq!(snapshot.turn_key, None);
        assert_eq!(snapshot.output_digest, None);
        assert_eq!(
            snapshot.blockers.first().map(|blocker| blocker.kind),
            Some(CompletionBlockerKind::TurnUnknown)
        );
    }

    /// The final text is the last assistant message in the window, even when it is empty, not the
    /// last non-empty message.
    #[test]
    fn final_text_is_the_last_assistant_message_even_when_empty() {
        let snapshot = derive_completion_snapshot(
            &history(
                vec![
                    user(1, "go"),
                    assistant(2, "progress"),
                    assistant(3, ""),
                    ended(4, StopReason::EndTurn),
                ],
                HistoryIntegrity::Complete,
            ),
            Some(&AgentOutputContract::Text),
            false,
        );
        assert_eq!(
            snapshot.blockers.first().map(|blocker| blocker.kind),
            Some(CompletionBlockerKind::OutputMissing)
        );
        assert_eq!(
            snapshot.output_digest,
            Some(format!("sha256:{}", sha256_hex(b"")))
        );
    }

    /// Empty output is only a warning for the legacy no-contract node.
    #[test]
    fn none_contract_empty_output_is_a_warning() {
        let snapshot = derive_completion_snapshot(
            &history(
                vec![user(1, "go"), ended(3, StopReason::EndTurn)],
                HistoryIntegrity::Complete,
            ),
            Some(&AgentOutputContract::None),
            false,
        );
        assert!(snapshot.blockers.is_empty());
        assert_eq!(snapshot.warnings, vec!["empty_output".to_string()]);
    }

    /// A structured contract failure is a readable blocker, not a terminal error.
    #[test]
    fn structured_failure_is_a_blocker() {
        let schema = json!({
            "type": "object",
            "required": ["answer"],
            "properties": { "answer": { "type": "string" } }
        });
        let snapshot = derive_completion_snapshot(
            &history(
                vec![
                    user(1, "go"),
                    assistant(2, "not json"),
                    ended(3, StopReason::EndTurn),
                ],
                HistoryIntegrity::Complete,
            ),
            Some(&AgentOutputContract::Structured {
                schema,
                text_exposure: StructuredTextExposure::IncludeFinalText,
            }),
            false,
        );
        assert_eq!(
            snapshot.blockers.first().map(|blocker| blocker.kind),
            Some(CompletionBlockerKind::StructuredInvalid)
        );
    }

    /// Damaged history keeps the found turn key but still blocks as unknown with a warning.
    #[test]
    fn damaged_history_stays_unknown() {
        let unreadable = NonZeroUsize::new(1).unwrap();
        let snapshot = derive_completion_snapshot(
            &history(
                vec![
                    user(1, "go"),
                    assistant(2, "done"),
                    ended(3, StopReason::EndTurn),
                ],
                HistoryIntegrity::Damaged {
                    unreadable_lines: unreadable,
                },
            ),
            None,
            false,
        );
        assert_eq!(
            snapshot.turn_key,
            Some(TurnKey {
                user_seq: 1,
                ended_seq: 3
            })
        );
        assert_eq!(
            snapshot.blockers.first().map(|blocker| blocker.kind),
            Some(CompletionBlockerKind::TurnUnknown)
        );
        assert!(
            snapshot
                .warnings
                .iter()
                .any(|warning| warning.starts_with("history_integrity_gap"))
        );
    }

    /// The persisted payload carries the snapshot under the `completion` key.
    #[test]
    fn payload_wraps_the_snapshot() {
        let snapshot = derive_completion_snapshot(
            &history(
                vec![
                    user(1, "go"),
                    assistant(2, "done"),
                    ended(3, StopReason::EndTurn),
                ],
                HistoryIntegrity::Complete,
            ),
            None,
            true,
        );
        let payload = completion_payload(&snapshot);
        assert_eq!(payload["completion"]["userSeq"], 1);
        assert_eq!(payload["completion"]["provisional"], true);
    }
}
