use pipeline_runtime::*;
use serde_json::{json, Value};
use std::{
    collections::{BTreeSet, HashMap},
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Barrier,
    },
};

struct ParallelTransform {
    barrier: Arc<Barrier>,
    active: Arc<AtomicUsize>,
    max_active: Arc<AtomicUsize>,
}

impl Operator for ParallelTransform {
    fn name(&self) -> &'static str {
        "parallel_transform"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn input_type(&self) -> ValueType {
        ValueType::Number
    }
    fn output_type(&self) -> ValueType {
        ValueType::Object
    }
    fn execute(&self, input: Value, context: &OperatorContext) -> Result<Value, String> {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_active.fetch_max(active, Ordering::SeqCst);
        self.barrier.wait();
        self.active.fetch_sub(1, Ordering::SeqCst);
        Ok(json!({ "node": context.node_id, "value": input }))
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let graph = Graph {
        id: "concurrent-demo".into(),
        version: "1".into(),
        inputs: vec![ArcContract {
            id: "source".into(),
            schema: ValueType::Number,
        }],
        nodes: vec![
            Node {
                id: "right-transform".into(),
                operator: "parallel_transform".into(),
                operator_version: "1".into(),
                inputs: vec!["source".into()],
                output: ArcContract {
                    id: "right".into(),
                    schema: ValueType::Object,
                },
                input_selector: Selector::default(),
                output_selector: Selector::default(),
                iterator: IteratorKind::Whole,
            },
            Node {
                id: "left-transform".into(),
                operator: "parallel_transform".into(),
                operator_version: "1".into(),
                inputs: vec!["source".into()],
                output: ArcContract {
                    id: "left".into(),
                    schema: ValueType::Object,
                },
                input_selector: Selector::default(),
                output_selector: Selector::default(),
                iterator: IteratorKind::Whole,
            },
            Node {
                id: "join".into(),
                operator: "join".into(),
                operator_version: "1".into(),
                inputs: vec!["left".into(), "right".into()],
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
            edge("left-transform", "join", "left"),
            edge("right-transform", "join", "right"),
        ],
        outputs: vec!["result".into()],
    };
    let barrier = Arc::new(Barrier::new(2));
    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::default();
    registry.register(ParallelTransform {
        barrier,
        active,
        max_active: max_active.clone(),
    })?;
    registry.register(Join)?;

    let compiled = compile(graph.clone(), &registry, &BTreeSet::new())?;
    let runtime = Runtime::new(BTreeSet::new())
        .with_max_parallelism(NonZeroUsize::new(2).expect("two is nonzero"));
    let result = runtime.run(
        &compiled,
        Identity {
            project_id: "demo".into(),
            graph_id: graph.id,
            graph_version: graph.version,
            execution_id: "execution-1".into(),
            attempt_id: "attempt-1".into(),
        },
        HashMap::from([("source".into(), json!(21))]),
        &Cancellation::default(),
    )?;

    let scheduled: Vec<_> = result
        .journal
        .iter()
        .filter_map(|event| match event {
            Event::NodeScheduled { node_id } => Some(node_id.as_str()),
            _ => None,
        })
        .collect();
    println!(
        "maximum overlapping operators = {}",
        max_active.load(Ordering::SeqCst)
    );
    println!("scheduled in stable order = {scheduled:?}");
    println!("joined ARC = {}", result.outputs["result"].payload);
    Ok(())
}
