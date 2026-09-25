# DAGpipe

Standalone Rust SDK plus a governance CLI for small deterministic DAGs. The
crate has no AppSDK dependency. A project implements and registers its
Operators in Rust, describes a `Graph`, compiles it, then passes only
`CompiledGraph` to `Runtime`. The global `dagpipe` CLI inspects graph
configuration and exact Operator name/version bindings without loading or
executing project code.

```rust
let mut registry = pipeline_runtime::Registry::default();
registry.register(MyOperator)?;
let compiled = pipeline_runtime::compile(graph, &registry, &capabilities)?;
let runtime = pipeline_runtime::Runtime::new(capabilities);
let result = runtime.run(&compiled, identity, input_arcs, &cancellation)?;
```

`Node.inputs` and `Node.output` are the ARC access grant. Operators receive
selected input values and a stable execution/node context. They do not receive
the runtime, graph, scheduler, or ARC store. Compile rejects missing operators,
invalid edges, cycles, undeclared ARC reads, incompatible schemas, dead nodes,
and missing capabilities. Each node pins both operator name and version;
registry versions coexist and compilation rejects an unavailable binding.
Failure returns both the classified error and its execution journal.

## Install the global CLI and Skill

From a DAGpipe checkout, run:

```sh
./scripts/install.sh
```

This builds and installs the `dagpipe` binary with Cargo, installs the SDK
crate source to `~/.local/share/dagpipe/sdk`, then installs its packaged usage
Skill to `~/.agents/skills/dagpipe-runtime/SKILL.md`. It updates an existing
DAGpipe Skill in place and refuses to overwrite a differently named Skill. The
script uses `cargo install --force` to update the DAGpipe binary. Run
`dagpipe sdk path` to print the absolute SDK path for a consuming project's
Cargo.toml (`~` is not expanded in TOML).

The CLI's built-in governance modules are static:

```sh
dagpipe modules list
dagpipe sdk path
dagpipe graph validate examples/governance_graph.json
dagpipe graph inspect examples/governance_graph.json
```

CLI validation checks DAG acyclicity and that every node can reach a declared
output. Inspection lists each node's `operator@version` binding and deterministic
topological waves. These commands do not prove that the project has registered
those Operators or that schemas/effects match; the project's SDK `compile()` is
the authoritative gate for those contracts.

Run the executable examples:

```sh
cargo run --example data_pipeline
cargo run --example control_lifecycle
cargo run --example effect_pipeline
cargo run --example concurrent_pipeline
```

The compiler parses typed JSON configuration with `parse_graph_json`; it does
not load YAML. The current selector supports include/exclude fields and bounded
record predicates (`exists`, `equals`, and `is_type`). ARC schemas support primitive
JSON types and arrays with an item type. Iterator modes are whole-value and
array-item. A general expression language, durable checkpoints, distributed
scheduling, and persistent ARC storage remain outside this first cut.

The compiled graph is opaque and immutable to callers. Runtime defaults to one
worker; `with_max_parallelism(NonZeroUsize)` opts into bounded parallel execution
of independent nodes. Scheduling uses deterministic topological waves, and
journal order is stable by wave and node ID. Operators and node hooks must be
safe for concurrent calls when parallelism is enabled. A failed wave drains its
already-scheduled nodes and prevents later waves from starting.

An attempt is one `Runtime::run`; callers create a new `execution_id` and
`attempt_id` for retry. The runtime does not automatically repeat operators or
external effects. Effect and replay declarations are recorded in the journal
for the caller's policy layer.

See [docs/usage.md](docs/usage.md) for the authoring/run guide, concurrency and
side-effect semantics. The project-local Codex skill is in
`.agents/skills/dagpipe-runtime/`.

## Release builds and versioning

The initial release is fixed at `0.1.0`:

```sh
./scripts/release.sh initial
```

For subsequent releases, `scripts/release.sh` bumps the patch version before
building and globally installing the CLI, SDK source, and Skill. Choose
`minor` or `major` when that SemVer increment is intended. Release bumping uses
the Cargo-specific `cargo set-version` command from `cargo-edit`; install it
once with `cargo install cargo-edit --locked`. Ordinary `cargo test`,
`cargo build`, and `cargo clippy` do not change the version.
