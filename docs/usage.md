# DAGpipe usage

## SDK and governance CLI

Third-party projects use the Rust SDK for Operator registration and runtime
execution, and the global `dagpipe` CLI for framework governance of graph
design. The CLI contains a fixed set of built-in governance modules; it does
not load project plugins or execute project Operators.

Install the binary and its bundled Skill from a DAGpipe checkout:

```sh
./scripts/install.sh
```

The installer runs `cargo install --path . --locked --force`, copies the crate
source to `~/.local/share/dagpipe/sdk`, then installs the packaged Skill to
`~/.agents/skills/dagpipe-runtime/SKILL.md`. It updates an existing DAGpipe
Skill and refuses to overwrite a differently named Skill. Get the absolute
Cargo dependency path with `dagpipe sdk path` (`Cargo.toml` does not expand
`~`):

```toml
[dependencies]
pipeline_runtime = { path = "/Users/<your-user>/.local/share/dagpipe/sdk" }
serde_json = "1"
```

Use the CLI against the same graph JSON consumed by the SDK:

```sh
dagpipe modules list
dagpipe sdk path
dagpipe graph validate examples/governance_graph.json
dagpipe graph inspect examples/governance_graph.json
```

`validate` checks the external dependency DAG is acyclic, every node can reach
a declared output, and each Graph declares exactly one source/input ARC and one
output ARC (SESE). The input ARC schema is project-defined; a business-object
source does not have to be a JSON object payload. Validate each project's
business-object source as a separate Graph. The project's internal module
implementation is not required to be a DAG. `inspect` reports `operator@version`
bindings, deterministic execution
waves, and ARC edges. These checks are governance evidence only: they do not
resolve the project's Rust `Registry`, Operator contracts, or capabilities.
The project's `compile(graph, &registry, &capabilities)` remains the complete
compile gate before `Runtime::run`.

DAGpipe is a Rust library for running a declared, acyclic graph in one process.
The project owns its operators and graph; DAGpipe compiles the graph, enforces
its ARC grants and capabilities, schedules eligible nodes, and returns outputs
plus an execution journal.

## Release builds and version bumping

The first release stays at `0.1.0`:

```sh
./scripts/release.sh initial
```

Subsequent release builds bump patch by default before compiling and installing
the global CLI, SDK source, and Skill. Pass `minor` or `major` for those SemVer
increments. This release entrypoint requires Cargo Edit's `cargo set-version`
command (`cargo install cargo-edit --locked`). Normal development builds/tests
do not change package versions.

## Implement and register an operator

An operator owns business behavior. It receives a selected input value and
immutable identity/node context; it cannot read the ARC store or manipulate the
graph. Its input/output types and effects are part of its compile-time contract.

```rust
use pipeline_runtime::{Operator, OperatorContext, ValueType};
use serde_json::Value;

struct Normalize;

impl Operator for Normalize {
    fn name(&self) -> &'static str { "normalize" }
    fn version(&self) -> &'static str { "1" }
    fn input_type(&self) -> ValueType { ValueType::Object }
    fn output_type(&self) -> ValueType { ValueType::Object }

    fn execute(&self, mut input: Value, _: &OperatorContext) -> Result<Value, String> {
        let name = input["name"].as_str().ok_or("missing name")?.trim().to_lowercase();
        input["name"] = name.into();
        Ok(input)
    }
}
```

Register each operator once, then compile the graph against the exact effect
capabilities allowed by the design:

```rust
use pipeline_runtime::*;

let mut registry = Registry::default();
registry.register(Normalize)?;
let graph = parse_graph_json(graph_json)?;
let allowed_effects = BTreeSet::new();
let compiled = compile(graph, &registry, &allowed_effects)?;
```

Compilation rejects malformed edges, cycles, missing operators, incompatible
ARC/operator contracts, undeclared ARC reads, duplicate outputs, dead nodes and
effects not granted to the graph. Keep and reuse the resulting `CompiledGraph`;
the Runtime does not accept raw graph configuration.

Each graph node must include `operator_version` alongside `operator`; registry
entries are keyed by that exact name/version pair. Multiple versions of the same
operator name may coexist, but a graph cannot silently bind to whichever
version happens to be registered.

## Run a graph

Provide every declared graph input ARC and stable execution/attempt identity.
Configure Runtime capabilities to match the compiled declarations:

```rust
use pipeline_runtime::*;
use std::{collections::{BTreeSet, HashMap}, num::NonZeroUsize};
use serde_json::json;

let runtime = Runtime::new(BTreeSet::new())
    .with_max_parallelism(NonZeroUsize::new(4).expect("4 is nonzero"));
let result = runtime.run(
    &compiled,
    Identity {
        project_id: "sample-project".into(),
        graph_id: "customer-import".into(),
        graph_version: "v1".into(),
        execution_id: "exec-2026-001".into(),
        attempt_id: "attempt-1".into(),
    },
    HashMap::from([("records".into(), json!([{"name":" Ada "}]))]),
    &Cancellation::default(),
)?;

let output_arc = &result.outputs["normalized-records"];
println!("{}", output_arc.payload);
println!("{} journal facts", result.journal.len());
```

An `ExecutionFailure` also contains the error classification and journal up to
the failed/cancelled terminal fact. A retry is a new `Runtime::run` call with a
new `execution_id` and `attempt_id`; the Runtime never loops a DAG or silently
retries effects.

## Concurrency and side effects

Runtime defaults to one worker. Set `with_max_parallelism(NonZeroUsize)` to opt
into bounded concurrency. The compiler groups nodes into deterministic
topological waves; independent siblings in a wave may overlap, while a node in a
later wave waits for all predecessors. The worker limit is respected within
each wave.

The Runtime gives each worker cloned input ARC values and accepts outputs only
after the wave drains. Journal events are ordered by wave and node ID, not by
thread timing. Operators and `before_node`/`after_node` hooks can be invoked
concurrently and must be safe for concurrent calls. `on_error` hooks run in
stable node-ID order after the wave completes.

If any node fails, all nodes in that already-scheduled wave finish; the Runtime
records every outcome, chooses the lowest node ID's error as the primary error,
and does not start later waves. A cancellation request is observed after a wave
drains, before another wave or successful execution completion; it cannot
interrupt a blocking operator. Consequently an effect already
running may complete even when a sibling fails or cancellation is requested.
Declare an ARC dependency to serialize operators that touch the same external
resource. Effect capability declarations are audit/compile-time boundaries, not
resource locks, OS permissions, or a sandbox. Use `Replayable`, `Idempotent`,
`NonReplayable`, and `RequiresConfirmation` deliberately; automatic effect
retry is not implemented.

Operators that emit control events must list bounded event identifiers in
`declared_events()`. Emitted names must match one of those lowercase identifiers
(up to 64 bytes); arbitrary output-derived strings are rejected before journal
emission so event facts cannot carry business payloads. Keep event data in the
output ARC.

`StateMachine::new` separately validates the declared state and transition
table, and `apply(current, event, journal)` accepts only declared transitions.
In this version a state machine is caller-owned; it is not embedded into
`Graph::compile` and the Runtime does not automatically react to control events.
See `examples/control_lifecycle.rs` for explicit caller-side lifecycle handling.

## Selectors and iterators

Input and output selectors share `include`, `exclude`, and bounded predicates
(`exists`, `equals`, `is_type`). `IteratorKind::Whole` calls an operator once
with the complete input (multiple ARC inputs are bundled as an array).
`IteratorKind::Items` calls it once per selected array item and gathers results
as an array. Item iteration itself is sequential; DAG-level node concurrency
does not parallelize items.

## Examples and verification

Run from the repository root:

```sh
cargo test --all-targets
cargo run --example data_pipeline
cargo run --example control_lifecycle
cargo run --example effect_pipeline
cargo run --example concurrent_pipeline
cargo clippy --all-targets -- -D warnings
```

The concurrent demo uses a barrier so both independent operators must overlap
to complete. It then joins their ARC outputs and prints the journal's stable
scheduling order. Tests additionally verify bounded configuration, wave failure
draining, stable error selection, and that dependent nodes do not start after a
failed wave.

This version is single-process and in-memory. It does not provide durable ARC or
journal storage, checkpoint recovery, distributed workers, work stealing,
parallel item iteration, or exactly-once effects.
