# Interactive Workflow Nodes

This module coordinates human interaction with workflow nodes that pause for input.

## Responsibilities

- `session.rs` validates follow-up turns, parks an ended turn with its completion snapshot, and
  moves an awaiting node between pending and running.
- `completion.rs` claims a node, reconstructs its result, repairs or materializes its snapshot, and
  prepares the durable engine transition.
- `turns.rs` prefixes each human turn with the frozen step boundary while recording the user's text.
- `interactive.rs` owns the transient completion gate shared by both paths.

## Invariants

A terminal node cannot accept another prompt, a node being completed cannot begin a concurrent
turn, and completion is revalidated after session shutdown before the engine transition commits.
Every park takes the per-run gate and commits its snapshot with the status flip in one transaction;
the completion gate reads the same snapshot the park wrote, including for nodes that parked before
snapshots existed.
