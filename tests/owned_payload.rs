//! Public consumer tests for owned business-payload transfer through
//! `Registry -> compile -> Runtime::run -> Operator::execute`.
//!
//! Each test observes real heap allocations (String bytes, Vec buffers) rather
//! than stack addresses of `Value`, so a deep copy shows up as a different
//! pointer.

use pipeline_runtime::{
    compile, edge, ArcContract, Cancellation, Graph, Identity, IteratorKind, Node, Operator,
    OperatorContext, Registry, Runtime, Selector, ValueType,
};
use serde_json::{json, Value};
use std::collections::{BTreeSet, HashMap};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Pointers {
    array: usize,
    name: usize,
    tags: usize,
}

fn capture_array(value: &Value) -> Pointers {
    Pointers {
        array: value.as_array().expect("array value").as_ptr() as usize,
        name: value[0]["name"].as_str().expect("name string").as_ptr() as usize,
        tags: value[0]["tags"]
            .as_array()
            .expect("nested tags array")
            .as_ptr() as usize,
    }
}

fn sample_input() -> Value {
    json!([
        {"name": "alpha", "tags": ["x", "y"]},
        {"name": "beta", "tags": ["z"]}
    ])
}

fn identity(graph: &Graph) -> Identity {
    Identity {
        project_id: "owned-payload".into(),
        graph_id: graph.id.clone(),
        graph_version: graph.version.clone(),
        execution_id: "exec-1".into(),
        attempt_id: "attempt-1".into(),
    }
}

struct Observe {
    seen: Arc<Mutex<Option<Pointers>>>,
}

impl Operator for Observe {
    fn name(&self) -> &'static str {
        "observe"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn input_type(&self) -> ValueType {
        ValueType::Array
    }
    fn output_type(&self) -> ValueType {
        ValueType::Array
    }
    fn execute(&self, input: Value, _: &OperatorContext) -> Result<Value, String> {
        *self.seen.lock().unwrap() = Some(capture_array(&input));
        Ok(input)
    }
}

fn linear_graph() -> Graph {
    Graph {
        id: "owned-linear".into(),
        version: "1".into(),
        inputs: vec![ArcContract {
            id: "source".into(),
            schema: ValueType::Array,
        }],
        nodes: vec![Node {
            id: "observe".into(),
            operator: "observe".into(),
            operator_version: "1".into(),
            inputs: vec!["source".into()],
            output: ArcContract {
                id: "result".into(),
                schema: ValueType::Array,
            },
            input_selector: Selector::default(),
            output_selector: Selector::default(),
            iterator: IteratorKind::Whole,
        }],
        edges: vec![],
        outputs: vec!["result".into()],
    }
}

fn assert_linear_preserves_allocations(parallelism: NonZeroUsize) {
    let seen = Arc::new(Mutex::new(None));
    let mut registry = Registry::default();
    registry.register(Observe { seen: seen.clone() }).unwrap();
    let graph = linear_graph();
    let compiled = compile(graph.clone(), &registry, &BTreeSet::new()).unwrap();

    let input = sample_input();
    let expected = capture_array(&input);
    let result = Runtime::new(BTreeSet::new())
        .with_max_parallelism(parallelism)
        .run(
            &compiled,
            identity(&graph),
            HashMap::from([("source".to_owned(), input)]),
            &Cancellation::default(),
        )
        .unwrap();

    let observed = seen.lock().unwrap().expect("operator executed");
    let produced = capture_array(&result.outputs["result"].payload);
    assert_eq!(
        observed, expected,
        "operator did not receive the original String/Vec allocations"
    );
    assert_eq!(
        produced, expected,
        "runtime output did not preserve the operator's allocations"
    );
}

#[test]
fn linear_whole_payload_keeps_business_allocations() {
    assert_linear_preserves_allocations(NonZeroUsize::new(1).unwrap());
}

#[test]
fn parallel_linear_whole_payload_keeps_business_allocations() {
    // A single node still enters the parallel branch; this catches `task.clone()`.
    assert_linear_preserves_allocations(NonZeroUsize::new(4).unwrap());
}

struct Mutate;
impl Operator for Mutate {
    fn name(&self) -> &'static str {
        "mutate"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn input_type(&self) -> ValueType {
        ValueType::Array
    }
    fn output_type(&self) -> ValueType {
        ValueType::Array
    }
    fn execute(&self, mut input: Value, _: &OperatorContext) -> Result<Value, String> {
        input[0]["name"] = json!("changed");
        input
            .as_array_mut()
            .expect("array input")
            .push(json!({"name": "added", "tags": ["w"]}));
        Ok(input)
    }
}

struct Join;
impl Operator for Join {
    fn name(&self) -> &'static str {
        "join"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn input_type(&self) -> ValueType {
        ValueType::Array
    }
    fn output_type(&self) -> ValueType {
        ValueType::Array
    }
    fn execute(&self, input: Value, _: &OperatorContext) -> Result<Value, String> {
        Ok(input)
    }
}

fn fanout_graph() -> Graph {
    Graph {
        id: "owned-fanout".into(),
        version: "1".into(),
        inputs: vec![ArcContract {
            id: "source".into(),
            schema: ValueType::Array,
        }],
        nodes: vec![
            Node {
                id: "a-mutate".into(),
                operator: "mutate".into(),
                operator_version: "1".into(),
                inputs: vec!["source".into()],
                output: ArcContract {
                    id: "mutated".into(),
                    schema: ValueType::Array,
                },
                input_selector: Selector::default(),
                output_selector: Selector::default(),
                iterator: IteratorKind::Whole,
            },
            Node {
                id: "z-observe".into(),
                operator: "observe".into(),
                operator_version: "1".into(),
                inputs: vec!["source".into()],
                output: ArcContract {
                    id: "observed".into(),
                    schema: ValueType::Array,
                },
                input_selector: Selector::default(),
                output_selector: Selector::default(),
                iterator: IteratorKind::Whole,
            },
            Node {
                id: "join".into(),
                operator: "join".into(),
                operator_version: "1".into(),
                inputs: vec!["mutated".into(), "observed".into()],
                output: ArcContract {
                    id: "result".into(),
                    schema: ValueType::Array,
                },
                input_selector: Selector::default(),
                output_selector: Selector::default(),
                iterator: IteratorKind::Whole,
            },
        ],
        edges: vec![
            edge("a-mutate", "join", "mutated"),
            edge("z-observe", "join", "observed"),
        ],
        outputs: vec!["result".into()],
    }
}

#[test]
fn fanout_siblings_receive_independent_payloads() {
    let seen = Arc::new(Mutex::new(None));
    let mut registry = Registry::default();
    registry.register(Observe { seen: seen.clone() }).unwrap();
    registry.register(Mutate).unwrap();
    registry.register(Join).unwrap();
    let graph = fanout_graph();
    let compiled = compile(graph.clone(), &registry, &BTreeSet::new()).unwrap();

    let input = sample_input();
    let original = input.clone();
    let expected = capture_array(&input);
    let result = Runtime::new(BTreeSet::new())
        .with_max_parallelism(NonZeroUsize::new(2).unwrap())
        .run(
            &compiled,
            identity(&graph),
            HashMap::from([("source".to_owned(), input)]),
            &Cancellation::default(),
        )
        .unwrap();

    // The last sibling reader gets the moved original; the first gets a copy.
    let observed = seen.lock().unwrap().expect("observer executed");
    assert_eq!(
        observed, expected,
        "last fanout reader did not receive the moved source allocation"
    );

    let payload = &result.outputs["result"].payload;
    let mutated = &payload[0];
    let untouched = &payload[1];
    assert_eq!(
        untouched, &original,
        "mutation in one sibling contaminated the other sibling"
    );
    assert_eq!(mutated[0]["name"], json!("changed"));
    assert_eq!(mutated.as_array().unwrap().len(), 3);
    assert_eq!(untouched.as_array().unwrap().len(), 2);
}

struct ObserveFirst {
    seen: Arc<Mutex<Option<Pointers>>>,
}

impl Operator for ObserveFirst {
    fn name(&self) -> &'static str {
        "observe_first"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn input_type(&self) -> ValueType {
        ValueType::Array
    }
    fn output_type(&self) -> ValueType {
        ValueType::Array
    }
    fn execute(&self, input: Value, _: &OperatorContext) -> Result<Value, String> {
        *self.seen.lock().unwrap() = Some(capture_array(&input));
        Ok(input)
    }
}

struct ObserveBundle {
    seen: Arc<Mutex<Option<Pointers>>>,
}

impl Operator for ObserveBundle {
    fn name(&self) -> &'static str {
        "observe_bundle"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn input_type(&self) -> ValueType {
        ValueType::Array
    }
    fn output_type(&self) -> ValueType {
        ValueType::Array
    }
    fn execute(&self, input: Value, _: &OperatorContext) -> Result<Value, String> {
        // Two-input Whole node: input is `[source, first_out]`.
        *self.seen.lock().unwrap() = Some(capture_array(&input[0]));
        Ok(input)
    }
}

fn cross_wave_graph() -> Graph {
    Graph {
        id: "owned-cross-wave".into(),
        version: "1".into(),
        inputs: vec![ArcContract {
            id: "source".into(),
            schema: ValueType::Array,
        }],
        nodes: vec![
            Node {
                id: "a-first".into(),
                operator: "observe_first".into(),
                operator_version: "1".into(),
                inputs: vec!["source".into()],
                output: ArcContract {
                    id: "first-out".into(),
                    schema: ValueType::Array,
                },
                input_selector: Selector::default(),
                output_selector: Selector::default(),
                iterator: IteratorKind::Whole,
            },
            Node {
                id: "m-second".into(),
                operator: "observe_bundle".into(),
                operator_version: "1".into(),
                inputs: vec!["source".into(), "first-out".into()],
                output: ArcContract {
                    id: "second-out".into(),
                    schema: ValueType::Array,
                },
                input_selector: Selector::default(),
                output_selector: Selector::default(),
                iterator: IteratorKind::Whole,
            },
            Node {
                id: "join".into(),
                operator: "join".into(),
                operator_version: "1".into(),
                inputs: vec!["first-out".into(), "second-out".into()],
                output: ArcContract {
                    id: "result".into(),
                    schema: ValueType::Array,
                },
                input_selector: Selector::default(),
                output_selector: Selector::default(),
                iterator: IteratorKind::Whole,
            },
        ],
        edges: vec![
            edge("a-first", "m-second", "first-out"),
            edge("a-first", "join", "first-out"),
            edge("m-second", "join", "second-out"),
        ],
        outputs: vec!["result".into()],
    }
}

#[test]
fn cross_wave_last_reader_receives_owned_source() {
    let first_seen = Arc::new(Mutex::new(None));
    let second_seen = Arc::new(Mutex::new(None));
    let mut registry = Registry::default();
    registry
        .register(ObserveFirst {
            seen: first_seen.clone(),
        })
        .unwrap();
    registry
        .register(ObserveBundle {
            seen: second_seen.clone(),
        })
        .unwrap();
    registry.register(Join).unwrap();
    let graph = cross_wave_graph();
    let compiled = compile(graph.clone(), &registry, &BTreeSet::new()).unwrap();

    let input = sample_input();
    let original = input.clone();
    let expected = capture_array(&input);
    let result = Runtime::new(BTreeSet::new())
        .run(
            &compiled,
            identity(&graph),
            HashMap::from([("source".to_owned(), input)]),
            &Cancellation::default(),
        )
        .unwrap();

    // The later-wave reader is the last consumer and must own the source.
    let second = second_seen.lock().unwrap().expect("second reader executed");
    assert_eq!(
        second, expected,
        "cross-wave last reader did not receive the moved source allocation"
    );
    // The earlier-wave reader got an independent allocation; the later reader
    // owns the moved original, so neither can contaminate the other.
    let first = first_seen.lock().unwrap().expect("first reader executed");
    assert_ne!(
        first.array, expected.array,
        "earlier and later readers must not share one mutable allocation"
    );

    let payload = &result.outputs["result"].payload;
    assert_eq!(&payload[0], &original);
    assert_eq!(&payload[1][0], &original);
    assert_eq!(&payload[1][1], &original);
}
