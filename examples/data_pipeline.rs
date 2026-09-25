use pipeline_runtime::*;
use serde_json::{json, Value};
use std::collections::{BTreeSet, HashMap};

fn rows_type() -> ValueType {
    ValueType::ArrayOf(Box::new(ValueType::Object))
}

struct Load;
impl Operator for Load {
    fn name(&self) -> &'static str {
        "load"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn input_type(&self) -> ValueType {
        rows_type()
    }
    fn output_type(&self) -> ValueType {
        rows_type()
    }
    fn execute(&self, input: Value, _: &OperatorContext) -> Result<Value, String> {
        Ok(input)
    }
}

struct Normalize;
impl Operator for Normalize {
    fn name(&self) -> &'static str {
        "normalize"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn input_type(&self) -> ValueType {
        ValueType::Object
    }
    fn output_type(&self) -> ValueType {
        ValueType::Object
    }
    fn execute(&self, mut input: Value, _: &OperatorContext) -> Result<Value, String> {
        let name = input
            .get("name")
            .and_then(Value::as_str)
            .ok_or("missing name")?
            .trim()
            .to_lowercase();
        input["name"] = json!(name);
        Ok(input)
    }
}

struct Filter;
impl Operator for Filter {
    fn name(&self) -> &'static str {
        "filter"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn input_type(&self) -> ValueType {
        rows_type()
    }
    fn output_type(&self) -> ValueType {
        rows_type()
    }
    fn execute(&self, input: Value, _: &OperatorContext) -> Result<Value, String> {
        let rows = input.as_array().ok_or("expected array")?;
        Ok(Value::Array(
            rows.iter()
                .filter(|row| row["score"].as_i64().unwrap_or_default() >= 50)
                .cloned()
                .collect(),
        ))
    }
}

struct Transform;
impl Operator for Transform {
    fn name(&self) -> &'static str {
        "transform"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn input_type(&self) -> ValueType {
        ValueType::Object
    }
    fn output_type(&self) -> ValueType {
        ValueType::Object
    }
    fn execute(&self, mut input: Value, _: &OperatorContext) -> Result<Value, String> {
        let score = input["score"].as_i64().ok_or("missing score")?;
        input["score"] = json!(score * 2);
        input["transformed"] = json!(true);
        Ok(input)
    }
}

struct Validate;
impl Operator for Validate {
    fn name(&self) -> &'static str {
        "validate"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn input_type(&self) -> ValueType {
        rows_type()
    }
    fn output_type(&self) -> ValueType {
        rows_type()
    }
    fn execute(&self, input: Value, _: &OperatorContext) -> Result<Value, String> {
        let rows = input.as_array().ok_or("expected array")?;
        if rows
            .iter()
            .any(|row| row["name"].as_str().is_none() || row["score"].as_i64().is_none())
        {
            return Err("invalid transformed row".into());
        }
        Ok(input)
    }
}

struct Output;
impl Operator for Output {
    fn name(&self) -> &'static str {
        "output"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn input_type(&self) -> ValueType {
        rows_type()
    }
    fn output_type(&self) -> ValueType {
        rows_type()
    }
    fn execute(&self, input: Value, _: &OperatorContext) -> Result<Value, String> {
        Ok(input)
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let arc = |id: &str, schema| ArcContract {
        id: id.into(),
        schema,
    };
    let node = |id: &str, operator: &str, inputs: &[&str], output: &str, schema, iterator| Node {
        id: id.into(),
        operator: operator.into(),
        operator_version: "1".into(),
        inputs: inputs.iter().map(|s| (*s).into()).collect(),
        output: arc(output, schema),
        input_selector: Selector::default(),
        output_selector: Selector::default(),
        iterator,
    };
    let graph = Graph {
        id: "data-pipeline".into(),
        version: "1".into(),
        inputs: vec![arc("source", rows_type())],
        nodes: vec![
            node(
                "load",
                "load",
                &["source"],
                "loaded",
                rows_type(),
                IteratorKind::Whole,
            ),
            Node {
                input_selector: Selector {
                    include: BTreeSet::from(["name".into(), "score".into()]),
                    exclude: BTreeSet::new(),
                    predicate: None,
                },
                ..node(
                    "normalize",
                    "normalize",
                    &["loaded"],
                    "normalized",
                    rows_type(),
                    IteratorKind::Items,
                )
            },
            node(
                "filter",
                "filter",
                &["normalized"],
                "filtered",
                rows_type(),
                IteratorKind::Whole,
            ),
            node(
                "transform",
                "transform",
                &["filtered"],
                "transformed",
                rows_type(),
                IteratorKind::Items,
            ),
            node(
                "validate",
                "validate",
                &["transformed"],
                "validated",
                rows_type(),
                IteratorKind::Whole,
            ),
            node(
                "output",
                "output",
                &["validated"],
                "result",
                rows_type(),
                IteratorKind::Whole,
            ),
        ],
        edges: vec![
            edge("load", "normalize", "loaded"),
            edge("normalize", "filter", "normalized"),
            edge("filter", "transform", "filtered"),
            edge("transform", "validate", "transformed"),
            edge("validate", "output", "validated"),
        ],
        outputs: vec!["result".into()],
    };
    let mut registry = Registry::default();
    registry.register(Load)?;
    registry.register(Normalize)?;
    registry.register(Filter)?;
    registry.register(Transform)?;
    registry.register(Validate)?;
    registry.register(Output)?;
    let compiled = compile(graph.clone(), &registry, &BTreeSet::new())?;
    let runtime = Runtime::new(BTreeSet::new());
    let result = runtime.run(&compiled, Identity { project_id: "demo".into(), graph_id: graph.id, graph_version: graph.version,
        execution_id: "data-1".into(), attempt_id: "attempt-1".into() }, HashMap::from([("source".into(), json!([
            {"name":" ALICE ","score":80,"private":"discarded"}, {"name":"Bob","score":20,"private":"discarded"}, {"name":"Cara","score":55,"private":"discarded"}
        ]))]), &Cancellation::default())?;
    println!("output = {}", result.outputs["result"].payload);
    println!("journal events = {}", result.journal.len());
    Ok(())
}
