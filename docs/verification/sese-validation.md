# SESE Candidate Verification

This receipt binds the SESE validation change to base commit
`d88070c9a2ebaaf06d19ebc00ae313bd5f44fd8b` and runtime-source diff SHA-256
`bc0fe41caeae1e13d931d6bfa8f978f9ad1cce8ca2bc1a2c9dba9453731a2e84`
(`src/lib.rs`, `src/bin/dagpipe.rs`, `examples/effect_pipeline.rs`).

## Candidate gates

All commands ran from the candidate worktree
`playground/dag-single-source-sink-20260925`:

- `cargo fmt -- --check` — exit 0.
- `cargo test --all-targets` — exit 0; 28 passed, 0 failed.
- The topology unit test accepts a scalar (`String`) source schema, confirming
  SESE constrains graph boundaries rather than the JSON payload type.
- `cargo clippy --all-targets -- -D warnings` — exit 0.
- `cargo run --example data_pipeline` — exit 0; produced two transformed records.
- `cargo run --example control_lifecycle` — exit 0; first attempt failed, retry completed.
- `cargo run --example effect_pipeline` — exit 0; file written, 4 effect/event facts.
- `cargo run --example concurrent_pipeline` — exit 0; maximum overlap 2, stable schedule,
  two branch results joined at the sole Graph output.
- `cargo run --bin dagpipe -- graph validate examples/governance_graph.json` — exit 0;
  accepted `project-normalization@1` (2 nodes, 1 edge, 2 waves).
- `cargo run --bin dagpipe -- graph inspect examples/governance_graph.json` — exit 0;
  reported the two deterministic waves and declared ARC edge.
- `dagpipe graph validate examples/governance_graph.json` after global install — exit 0.
- `dagpipe graph validate examples/.review-invalid-sese.json` after global install — exit 2;
  rejected the two-exit Graph with `an audited SESE Graph must declare exactly one output ARC`.

## Installed artifact

`./scripts/install.sh` completed at version `0.1.0`. The installed CLI and candidate
`target/release/dagpipe` had matching SHA-256
`da4f4b1d936b8ca11b1f8747e97833e943c44063fbec34a41d1c1d47c0e859b5`. The installed
SDK source, Cargo manifest, and usage Skill matched their candidate files.
