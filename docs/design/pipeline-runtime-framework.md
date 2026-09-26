# Pipeline Runtime / DAG Component Framework

## Purpose

Build a small, deterministic, project-neutral runtime that executes an already
compiled graph, accompanied by a small CLI for framework-level graph governance.
It is a standalone component first. AppSDK may later consume it, but the first
implementation must not depend on AppSDK crates, contracts, filesystem layout,
or lifecycle records.

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
| DAGpipe CLI | Validate and inspect graph topology and exact node-to-operator bindings using static built-in governance modules | Execute project operators, resolve project registries, or replace AppSDK approval/evidence governance |
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

The CLI and SDK share the graph JSON format and the same DAG topology analyzer.
The CLI catches structural graph failures (invalid ARC wiring, cycles, and dead
nodes) and presents the pinned `operator@version` bindings. It intentionally
does not duplicate the project's Rust `Registry`: the SDK `compile()` call is
the authoritative check for operator resolution, schema compatibility, and
effect capabilities. Project code remains the only place that executes those
operators.

## Core model

Keep six first-class concepts: `Graph`, `Node`, `Operator`, `ARC`, `Event`, and
`Runtime`. Identity, state machine, selector, iterator, hooks, retry, checkpoint,
schema, capability, and registry are fields, policies, or extensions of those
concepts; they are not additional top-level subsystems in the MVP.

- `Graph`: authoring structure of nodes, directed edges, graph identity/version,
  and declared input/output contracts.
- `Node`: stable ID, exact operator name/version binding, one output ARC, one or
  more input ARC bindings, and selector/iterator policy. The same node shape can bind operators
  that transform data or emit bounded control events; lifecycle transitions stay
  in the state-machine policy.
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

The compiler derives deterministic topological waves and exact ARC dependency
sets. Nodes in one wave form a ready antichain: all their predecessors have
completed, and they have no dependency on each other. For every node, graph
topology, data dependency, and runtime scheduling dependency must agree. A node
reads only ARC references declared as its inputs and writes only its declared
output. There is no general `arc.get(anything)` / `arc.set(anything)` shared-
memory API.

### SESE module boundary and per-object audit

Project-internal organization is not constrained to be a DAG: a module may use
whatever implementation structure it needs. The project's externally exposed
module/object dependencies are represented as a DAG. That boundary follows
SESE (Single Entry, Single Exit): each audited Graph represents one object or
feature flow and declares exactly one input ARC and exactly one output ARC. When
auditing a project with multiple object sources, traverse every source and
validate its corresponding Graph separately; do not combine independent flows
into a multi-entry/multi-exit Graph. Internal module details are opaque to this
external graph and must not be expanded into function-call nodes merely to
satisfy DAG validation.

Branching and joining are valid within the Graph, while a second entry or exit
is rejected. Every Node must be reachable from the sole declared source and
must reach the sole declared output.
Each exposed Node is one module boundary with one execution entry and one
declared output ARC; multiple input ARCs are bundled into that single Operator
invocation rather than modeled as multiple control-flow entries. Its internal
implementation may still contain loops, early returns, or state machines.
The Runtime follows only this compiled external DAG and does not add synthetic
routing Nodes or merge independent object flows implicitly.

```text
项目审计：逐对象遍历
  对象 ARC A ──► A 的 SESE DAG（可分支/汇合）──► A 的唯一出口 ARC
  对象 ARC B ──► B 的 SESE DAG（可分支/汇合）──► B 的唯一出口 ARC
                （各模块内部实现不要求是 DAG）
```

### Concurrency contract

The single-process Runtime supports opt-in, bounded parallel execution of
independent nodes. `max_parallelism` defaults to `1`; callers can raise it
explicitly. The compiler divides the stable topological order into waves. Within
each wave the Runtime dispatches nodes in node-ID order, runs no more than the
configured worker limit at once, and waits for the whole wave before advancing.
This wave barrier favors predictable failure and journal semantics over
work-stealing or minimum latency. Physical worker start/completion timing may
still vary.

Each worker receives cloned input ARC values and immutable compiled node/operator
references. It cannot access or mutate the shared ARC store, schedule another
node, or change topology. It returns a node outcome and private event facts. The
coordinator commits outputs and appends journal facts in stable node-ID order;
parallel completion timing never determines journal order or the selected
primary error. Operators and hooks may be called concurrently and therefore
must be safe for concurrent calls; the existing `Send + Sync` trait bound is
part of that contract.

If a node fails, the Runtime lets every already-dispatched node in that wave
finish, records each outcome in stable order, calls error hooks in stable order,
and does not start later waves. The primary execution error is the failure of
the lowest node ID in that wave (unless its error hook fails, which is reported
as a hook error). Successful sibling ARC writes remain journaled, but no
downstream node consumes them in the failed execution. A cancellation request
does not interrupt a running operator: the current wave drains, then cancellation
is observed before the next wave or successful execution completion. Thus
effect operators already running may complete even if a sibling fails or
cancellation is requested. Use explicit ARC
dependencies to serialize nodes that touch a shared external resource; declared
capabilities do not provide resource locks.

The journal is a deterministic logical record, not a wall-clock trace: it orders
wave scheduling events, then each node's local events by node ID. It intentionally
does not claim to preserve physical start/completion timing. Distributed
scheduling, work stealing, per-item parallel iteration, and automatic retries
remain out of scope.

The current JSON value path for a data node is:

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

Runtime never executes raw YAML/JSON or an authoring `Graph`. Compilation accepts
the typed Rust `Graph` or parses its JSON representation, then owns:

1. Parse and structural/schema validation.
2. Exact operator name/version resolution against the supplied registry.
3. Operator and ARC schema/contract compatibility checks.
4. Edge endpoint validation and cycle rejection.
5. Reachability/dead-node analysis under an explicit graph entry/exit contract.
6. ARC read/write grant validation against graph edges and node declarations.
7. Capability validation. The standalone `StateMachine::new` helper validates
   its own declared state/transition table; it is not currently embedded in the
   Graph or validated as part of `compile`.
8. Preserve the declared graph ID/version and freeze into `CompiledGraph`.

YAML parsing and cryptographic graph fingerprints are deferred. Graph identity
uses the explicit version supplied by the design owner.

Compilation returns explicit diagnostics on failure and an immutable compiled
value on success. The runtime accepts only that value. Runtime behavior cannot
add nodes/edges, choose undeclared routes, skip nodes based on hidden field
values, or reinterpret project configuration.

## Lifecycle, errors, and journal

Execution lifecycle is separate from graph topology. Events report facts such as
`ExecutionStarted`, `NodeScheduled`, `NodeStarted`, `ArcRead`, `OperatorStarted`,
`OperatorCompleted`, `ArcWritten`, `NodeCompleted`, `NodeFailed`,
`StateChanged`, `Cancelled`, and terminal execution outcomes. Payloads stay in
ARCs; events carry references, IDs, versions, and bounded metadata. Error text
stays on the returned failure value; control events carry only classification.

The runtime returns an ordered in-memory journal of execution facts on success
and failure. Its vector order is the event sequence; persistence is left to the
caller in this MVP. State is derived from state-machine transitions, not stored
as a separately mutable journal projection. Events bind execution identity,
graph ID/version, node/operator version, ARC references, and error kind as
applicable. Errors remain explicit and classifiable as graph, input, operator,
output, transition, effect, cancellation, or hook errors.

`OperatorStarted` and `OperatorCompleted` correspond to actual
`Operator::execute` calls. Whole-value execution uses a missing invocation
index; item iteration records the original input-array index for each selected
item that reaches the operator. Empty or fully filtered item input emits no
operator invocation facts, while the node itself can still complete with an
empty output array.

MVP retry orchestration belongs to the caller's state machine: one call to
`Runtime::run` is one attempt, and retry starts another call with new execution
and attempt IDs. Effect operations declare whether they are replayable,
idempotent, non-replayable, or require confirmation; the runtime records that
declaration but does not retry automatically. Durable checkpoint storage,
distributed replay, and exactly-once effects are not MVP claims.

Hooks are observational and bounded to `before_node`, `after_node`, and
`on_error`. Hooks cannot mutate graph topology, ARC grants, lifecycle state, or
operator output. If a hook fails, the runtime records and surfaces that failure
according to a documented hook error contract; it must not silently turn a
failed execution into success.

## MVP scope

### Included

- Operator registry and stable operator metadata/version.
- Graph authoring model and compiler to immutable `CompiledGraph`.
- DAG validation, deterministic topological waves, bounded parallel scheduling,
  and dead-node checks. Parallelism is opt-in; the default worker limit is one.
- In-memory ARC store with per-node read/write grants and schema/version refs.
- Data nodes, shared selector mechanism, and a small set of iterator modes.
- Bounded control events, stable execution identity, and declarative state
  transition validation.
- Single-process, single-machine runtime with append-only in-memory execution
  journal exposed to callers.
- Basic node hooks, declared effect capabilities, cancel/failure outcomes, and
  explicit attempt identity for retry demonstrations.
- Four executable demos described below.
- A concurrency demo proving independent nodes overlap while dependent nodes wait
  for the next wave and the journal remains stable.

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
4. **Concurrent pipeline**: independent sibling nodes with a dependent join.
   Demonstrate bounded overlap, wave barriers, stable ARC/journal ordering, and
   sibling-drain behavior on failure.

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

The four demos are executable acceptance paths, not documentation-only
examples. Tests should focus on externally observable invariants and outcomes,
not private scheduler implementation details.

## Implementation sequence and ownership

This document records the standalone component design. Implementation lives in
the `DAGpipe` Rust 2021 crate and has no AppSDK dependency. The AppSDK design tree
does not own the runtime implementation; a future integration would consume the
crate through an adapter.

Implementation order:

1. Current cut: lock public model, compiler, ARC grants, deterministic bounded
   scheduler, identity/events/state-machine helper, bounded hooks, capabilities,
   and demos.
2. Document the public authoring and run path, including concurrency and effect
   ordering semantics, and provide a project-local usage skill.
3. Verify invariant tests and executable examples from the isolated candidate.
4. Review the public API before considering persistence, checkpoint recovery, or
   AppSDK integration.

Completion means the standalone component can be imported and used by a small
project that registers operators, compiles a graph, runs it, reads ARC results,
and inspects the execution journal without importing AppSDK code.
