use pipeline_runtime::*;
use serde_json::{json, Value};
use std::{
    collections::{BTreeSet, HashMap},
    fs::{self, File, OpenOptions},
    io::Write,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

struct ReadFile;
impl Operator for ReadFile {
    fn name(&self) -> &'static str {
        "read_file"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn input_type(&self) -> ValueType {
        ValueType::String
    }
    fn output_type(&self) -> ValueType {
        ValueType::String
    }
    fn effects(&self) -> &'static [&'static str] {
        &["filesystem.read"]
    }
    fn replay(&self) -> EffectReplay {
        EffectReplay::NonReplayable
    }
    fn execute(&self, input: Value, _: &OperatorContext) -> Result<Value, String> {
        let path = input.as_str().ok_or("path must be a string")?;
        fs::read_to_string(path)
            .map(Value::String)
            .map_err(|error| error.to_string())
    }
}

struct Uppercase;
impl Operator for Uppercase {
    fn name(&self) -> &'static str {
        "uppercase"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn input_type(&self) -> ValueType {
        ValueType::String
    }
    fn output_type(&self) -> ValueType {
        ValueType::String
    }
    fn execute(&self, input: Value, _: &OperatorContext) -> Result<Value, String> {
        Ok(Value::String(
            input.as_str().ok_or("expected string")?.to_uppercase(),
        ))
    }
}

struct WriteFile;
impl Operator for WriteFile {
    fn name(&self) -> &'static str {
        "write_file"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn effects(&self) -> &'static [&'static str] {
        &["filesystem.write"]
    }
    fn replay(&self) -> EffectReplay {
        EffectReplay::RequiresConfirmation
    }
    fn execute(&self, input: Value, _: &OperatorContext) -> Result<Value, String> {
        let args = input.as_array().ok_or("expected [path, content]")?;
        let path = args
            .first()
            .and_then(Value::as_str)
            .ok_or("missing output path")?;
        let content = args
            .get(1)
            .and_then(Value::as_str)
            .ok_or("missing content")?;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|error| error.to_string())?;
        file.write_all(content.as_bytes())
            .map_err(|error| error.to_string())?;
        Ok(json!({"path":path,"bytes":content.len()}))
    }
}

struct EmitEvent;
impl Operator for EmitEvent {
    fn name(&self) -> &'static str {
        "emit_event"
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
    fn effects(&self) -> &'static [&'static str] {
        &["event.emit"]
    }
    fn declared_events(&self) -> &'static [&'static str] {
        &["file_written"]
    }
    fn emitted_events(&self, _: &Value) -> Vec<OperatorEvent> {
        vec![OperatorEvent {
            name: "file_written".into(),
        }]
    }
    fn execute(&self, input: Value, _: &OperatorContext) -> Result<Value, String> {
        Ok(input)
    }
}

struct TempArea {
    root: PathBuf,
    source: PathBuf,
    output: PathBuf,
}
impl TempArea {
    fn create() -> Result<Self, Box<dyn std::error::Error>> {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let root =
            std::env::temp_dir().join(format!("dagpipe-demo-{}-{nonce}", std::process::id()));
        fs::create_dir(&root)?;
        let source = root.join("input.txt");
        let output = root.join("output.txt");
        let mut file = File::create(&source)?;
        file.write_all(b"dag runtime\n")?;
        Ok(Self {
            root,
            source,
            output,
        })
    }
}
impl Drop for TempArea {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.output);
        let _ = fs::remove_file(&self.source);
        let _ = fs::remove_dir(&self.root);
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let area = TempArea::create()?;
    let arc = |id: &str, schema| ArcContract {
        id: id.into(),
        schema,
    };
    let node = |id: &str, operator: &str, inputs: &[&str], output: &str, schema| Node {
        id: id.into(),
        operator: operator.into(),
        operator_version: "1".into(),
        inputs: inputs.iter().map(|s| (*s).into()).collect(),
        output: arc(output, schema),
        input_selector: Selector::default(),
        output_selector: Selector::default(),
        iterator: IteratorKind::Whole,
    };
    let graph = Graph {
        id: "effect-pipeline".into(),
        version: "1".into(),
        inputs: vec![
            arc("source_path", ValueType::String),
            arc("output_path", ValueType::String),
        ],
        nodes: vec![
            node(
                "read",
                "read_file",
                &["source_path"],
                "contents",
                ValueType::String,
            ),
            node(
                "transform",
                "uppercase",
                &["contents"],
                "transformed",
                ValueType::String,
            ),
            node(
                "write",
                "write_file",
                &["output_path", "transformed"],
                "written",
                ValueType::Object,
            ),
            node(
                "emit",
                "emit_event",
                &["written"],
                "result",
                ValueType::Object,
            ),
        ],
        edges: vec![
            edge("read", "transform", "contents"),
            edge("transform", "write", "transformed"),
            edge("write", "emit", "written"),
        ],
        outputs: vec!["result".into()],
    };
    let mut registry = Registry::default();
    registry.register(ReadFile)?;
    registry.register(Uppercase)?;
    registry.register(WriteFile)?;
    registry.register(EmitEvent)?;
    let capabilities = BTreeSet::from([
        "filesystem.read".into(),
        "filesystem.write".into(),
        "event.emit".into(),
    ]);
    let compiled = compile(graph.clone(), &registry, &capabilities)?;
    let runtime = Runtime::new(capabilities);
    let result = runtime.run(
        &compiled,
        Identity {
            project_id: "demo".into(),
            graph_id: graph.id,
            graph_version: graph.version,
            execution_id: "effect-1".into(),
            attempt_id: "attempt-1".into(),
        },
        HashMap::from([
            ("source_path".into(), json!(area.source.to_string_lossy())),
            ("output_path".into(), json!(area.output.to_string_lossy())),
        ]),
        &Cancellation::default(),
    )?;
    assert_eq!(fs::read_to_string(&area.output)?, "DAG RUNTIME\n");
    assert!(result.journal.iter().any(
        |event| matches!(event, Event::OperatorEventEmitted { name, .. } if name == "file_written")
    ));
    println!("file written; effect and event facts = {}", result.journal.iter().filter(|event| matches!(event, Event::OperatorStarted { effects, .. } if !effects.is_empty()) || matches!(event, Event::OperatorEventEmitted { .. })).count());
    Ok(())
}
