# Pipeline Runtime / DAG Component Framework

## Purpose

Build a small, deterministic, project-neutral runtime that executes an already
validated graph. It is a standalone component first. AppSDK may later consume it,
but the first implementation must not depend on AppSDK crates, contracts, CLI,
filesystem layout, or lifecycle records.

The framework addresses repeated project-level execution mechanics: graph
compilation and validation, operator resolution, ARC data transfer, lifecycle
events and state transitions, capability checks, retry boundaries, hooks, and
execution journaling. It does not decide whether a project design is appropriate
or whether its evidence is sufficient.

## Business responsibilities

The three owners have separate responsibilities:

| Owner | Responsibility | Must not own |
|---|---|---|
| AppSDK / governance host | Define and admit project policy; validate design intent; bind evidence and project lifecycle; approve a compiled design for use | Operator business behavior or hidden runtime routing |
| Base framework | Compile the declared structure into an immutable graph; enforce declared dependencies/capabilities; execute that graph deterministically; record execution facts | Project policy, arbitrary design interpretation at runtime, or project-specific operators |
| Project | Implement and register operators; provide graph configuration, schemas, and declared effect capabilities | Direct scheduling, graph mutation, undeclared ARC access, or state mutation |

Intended path:

```text
Project graph definition + operator registry
                  │
                  ▼
        Compiler validates and freezes
                  │
                  ▼
             CompiledGraph
                  │
                  ▼
      Runtime executes declared topology
                  │
                  ▼
       Project operators perform work
```

AppSDK may later provide the design/governance/evidence stages around this path.
That integration is explicitly outside the standalone MVP.

## Core model

Keep six first-class concepts: `Graph`, `Node`, `Operator`, `ARC`, `Event`, and
`Runtime`. Identity, state machine, selector, iterator, hooks, retry, checkpoint,
schema, capability, and registry are fields, policies, or extensions of those
concepts; they are not additional top-level subsystems in the MVP.

- `Graph`: authoring structure of nodes, directed edges, graph identity/version,
  and declared input/output contracts.
- `Node`: stable ID, operator binding, input ARC bindings, output ARC bindings,
  selector/iterator policy, and optional lifecycle policy. A node is data-oriented
  or control-oriented by its declared contract, not by introducing many node
  subclasses.
- `Operator`: project-provided implementation plus stable name/version, input
  and output contracts, and effect/replay declarations.
- `ARC`: a versioned data artifact with ID, schema, and payload or payload
  reference. Runtime access is scoped to the node's compiled read/write grants.
- `Event`: small control-plane fact referring to execution, graph, node, state,
  and ARC IDs/versions. Events never carry business payloads.
- `Runtime`: accepts only a compiled graph and an execution input/identity;
  schedules eligible nodes, calls operators, enforces grants, and appends facts.

`Identity` is a stable value independent from process, connection, session,
route, or worker. At minimum it binds project ID, graph ID/version, execution ID,
and optional node/instance ID. Reconnect or process movement does not silently
change execution identity.

## Execution and data flow

The compiler derives a deterministic topological order and exact ARC dependency
sets. For every node, graph topology, data dependency, and runtime scheduling
dependency must agree. A node reads only ARC references declared as its inputs
and writes only its declared outputs. There is no general `arc.get(anything)` /
`arc.set(anything)` shared-memory API.

The standard data node path is:

```text
declared input ARCs → input selector → iterator → operator → output selector
                  → declared output ARCs → lifecycle events
```

Selectors use one common include/exclude/predicate/schema mechanism on either
side of the operator. Iterators define processing granularity (field, item,
record, batch, or stream); they do not contain business transformation logic.
The first implementation should ship only the smallest useful traversal modes
needed by the demos, not a speculative iterator plugin system.

Pure operators transform inputs without external effects. Effect operators use
the same operator interface but declare required capabilities such as
`filesystem.read`, `filesystem.write`, `network.http`, or `git.commit`. The
runtime checks the compiled declaration before invoking an operator. Effect
declarations enable audit and replay policy; they do not themselves provide a
sandbox or permission broker.

Control nodes may produce declared events or decisions. They cannot select an
arbitrary next node. A declared state machine applies
`current_state + event -> next_state` and rejects undeclared transitions. The
graph remains a DAG for each execution. Retry creates a new execution attempt
with explicit attempt identity; it never creates a back edge or mutates the
compiled graph.

## Compile-time and runtime boundary

Runtime never executes raw YAML/JSON or an authoring `Graph`. Compilation owns:

1. Parse and structural/schema validation.
2. Operator name/version resolution against the supplied registry.
3. Operator and ARC schema/contract compatibility checks.
4. Edge endpoint validation and cycle rejection.
5. Reachability/dead-node analysis under an explicit graph entry/exit contract.
6. ARC read/write grant validation against graph edges and node declarations.
7. Capability and state-machine transition validation.
8. Canonicalization, graph version/fingerprint, and freeze into `CompiledGraph`.

Compilation returns explicit diagnostics on failure and an immutable compiled
value on success. The runtime accepts only that value. Runtime behavior cannot
add nodes/edges, choose undeclared routes, skip nodes based on hidden field
values, or reinterpret project configuration.

## Lifecycle, errors, and journal

Execution lifecycle is separate from graph topology. Events report facts such as
`ExecutionStarted`, `NodeScheduled`, `NodeStarted`, `ArcRead`, `OperatorStarted`,
`OperatorCompleted`, `ArcWritten`, `NodeCompleted`, `NodeFailed`,
`StateChanged`, `Cancelled`, and terminal execution outcomes. Payloads stay in
ARCs; events carry references, IDs, versions, timestamps, and bounded metadata.

The execution journal is append-only and is the record of what happened. Current
state is a projection of accepted events, not a second mutable truth. A journal
entry should bind the stable identity, graph fingerprint, node/operator version,
input/output ARC references, event sequence, and error classification as
applicable. Errors remain explicit and classifiable as graph, input, operator,
output, transition, or effect errors.

MVP retry policy is deliberately bounded: pure operations may be replayed from
the same compiled graph and input ARC versions. Effect operations declare
whether they are replayable, idempotent, non-replayable, or require confirmation.
The runtime must not blindly retry an uncertain external side effect. Durable
checkpoint storage, distributed replay, and exactly-once effects are not MVP
claims.

Hooks are observational and bounded to `before_node`, `after_node`, and
`on_error`. Hooks cannot mutate graph topology, ARC grants, lifecycle state, or
operator output. If a hook fails, the runtime records and surfaces that failure
according to a documented hook error contract; it must not silently turn a
failed execution into success.

## MVP scope

### Included

- Operator registry and stable operator metadata/version.
- Graph authoring model and compiler to immutable `CompiledGraph`.
- DAG validation, deterministic topological scheduling, and dead-node checks.
- In-memory ARC store with per-node read/write grants and schema/version refs.
- Data nodes, shared selector mechanism, and a small set of iterator modes.
- Bounded control events, stable execution identity, and declarative state
  transition validation.
- Single-process, single-machine runtime with append-only in-memory execution
  journal exposed to callers.
- Basic node hooks, declared effect capabilities, cancel/failure outcomes, and
  explicit attempt identity for retry demonstrations.
- Three executable demos described below.

### Excluded

AppSDK integration, project governance, approval/evidence policy, distributed
scheduling, remote workers, plugin/DI framework, expression language, dynamic
routing, visual editor, complex persistence, durable checkpoint recovery,
exactly-once side effects, and project-specific operators.

Keep interfaces independent of in-memory storage so persistent ARC/journal
implementations can be considered later, but do not add storage abstractions
without an MVP caller that proves they are needed.

## Required demos

1. **Data pipeline**: `Load → Normalize → Filter → Transform → Validate →
   Output`. Demonstrate operator binding, selector, iterator, ARC references,
   topology, and deterministic output.
2. **Control lifecycle**: `READY → RUNNING → FAILED → retry/new attempt → READY
   → RUNNING → COMPLETED`. Demonstrate stable project/graph identity, distinct
   execution attempt IDs, legal event-driven transitions, and an acyclic graph.
3. **Effect pipeline**: `Read File → Transform → Write File → Emit Event`.
   Demonstrate pure/effect operators in one graph, declared capabilities,
   effect journaling, and explicit replay behavior. Use a temporary demo-owned
   file and clean it up within the demo.

## Invariant-led acceptance

Compiler must reject cycles, invalid/missing edge endpoints, missing operators,
incompatible contracts, unreachable/dead nodes when prohibited by the entry/exit
contract, undeclared ARC reads/writes, missing effect capabilities, and invalid
state transitions. Diagnostics identify the graph/node/operator and failed
contract without embedding business payloads.

Runtime acceptance must prove it cannot execute a node outside the compiled
graph, mutate topology, access undeclared ARCs, or accept a direct state set.
Operator APIs expose only input/context/output plus bounded event emission; they
do not expose graph mutation or arbitrary scheduling. Failure and cancellation
must produce observable terminal facts, and uncertain effects must not be
silently retried.

The three demos are executable acceptance paths, not documentation-only
examples. Tests should focus on externally observable invariants and outcomes,
not private scheduler implementation details.

## Implementation sequence and ownership

This document records design intent only; it does not add code to AppSDK or
select AppSDK as the runtime owner. Before implementation, establish a standalone
component repository/worktree and choose its implementation language from the
actual target and distribution constraints. Preserve the dependency boundary:
the framework must run without AppSDK installed.

Implementation order:

1. Lock the public core model and compiled graph contract.
2. Implement compiler validation and invariant tests.
3. Implement ARC grants, deterministic in-memory scheduler, and data operators.
4. Add identity, events, state transitions, failure/cancel, and attempt semantics.
5. Add bounded hooks and capability enforcement.
6. Finish all three demos and their acceptance evidence.
7. Review the public API for unnecessary concepts before considering persistence,
   checkpoint recovery, or AppSDK integration.

Completion means the standalone component can be imported and used by a small
project that registers operators, compiles a graph, runs it, reads ARC results,
and inspects the execution journal without importing AppSDK code.
