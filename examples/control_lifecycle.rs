use pipeline_runtime::*;
use serde_json::{json, Value};
use std::collections::{BTreeSet, HashMap};

struct FailFirstAttempt;
impl Operator for FailFirstAttempt {
    fn name(&self) -> &'static str {
        "fail_first_attempt"
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
    fn execute(&self, input: Value, context: &OperatorContext) -> Result<Value, String> {
        if context.identity.attempt_id == "attempt-1" {
            Err("planned first-attempt failure".into())
        } else {
            Ok(input)
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let graph = Graph {
        id: "retry-demo".into(),
        version: "1".into(),
        inputs: vec![ArcContract {
            id: "source".into(),
            schema: ValueType::Object,
        }],
        nodes: vec![Node {
            id: "single-node".into(),
            operator: "fail_first_attempt".into(),
            operator_version: "1".into(),
            inputs: vec!["source".into()],
            output: ArcContract {
                id: "result".into(),
                schema: ValueType::Object,
            },
            input_selector: Selector::default(),
            output_selector: Selector::default(),
            iterator: IteratorKind::Whole,
        }],
        edges: vec![],
        outputs: vec!["result".into()],
    };
    let mut registry = Registry::default();
    registry.register(FailFirstAttempt)?;
    let compiled = compile(graph.clone(), &registry, &BTreeSet::new())?;
    let runtime = Runtime::new(BTreeSet::new());
    let machine = StateMachine::new(
        [
            "ready".into(),
            "running".into(),
            "failed".into(),
            "completed".into(),
        ],
        vec![
            Transition {
                from: "ready".into(),
                event: "start".into(),
                to: "running".into(),
            },
            Transition {
                from: "running".into(),
                event: "fail".into(),
                to: "failed".into(),
            },
            Transition {
                from: "failed".into(),
                event: "retry".into(),
                to: "ready".into(),
            },
            Transition {
                from: "running".into(),
                event: "complete".into(),
                to: "completed".into(),
            },
        ],
    )?;
    let input = || HashMap::from([("source".into(), json!({"work":"payload"}))]);
    let identity = |execution_id: &str, attempt_id: &str| Identity {
        project_id: "demo".into(),
        graph_id: graph.id.clone(),
        graph_version: graph.version.clone(),
        execution_id: execution_id.into(),
        attempt_id: attempt_id.into(),
    };
    let mut lifecycle_journal = Vec::new();
    let mut state = machine.apply("ready", "start", &mut lifecycle_journal)?;
    let first = runtime
        .run(
            &compiled,
            identity("execution-1", "attempt-1"),
            input(),
            &Cancellation::default(),
        )
        .unwrap_err();
    assert!(first
        .journal
        .iter()
        .any(|event| matches!(event, Event::NodeFailed { .. })));
    assert_eq!(first.error.kind, ErrorKind::Operator);
    lifecycle_journal.extend(first.journal);
    state = machine.apply(&state, "fail", &mut lifecycle_journal)?;
    state = machine.apply(&state, "retry", &mut lifecycle_journal)?;
    state = machine.apply(&state, "start", &mut lifecycle_journal)?;
    let mut second = runtime.run(
        &compiled,
        identity("execution-2", "attempt-2"),
        input(),
        &Cancellation::default(),
    )?;
    lifecycle_journal.append(&mut second.journal);
    state = machine.apply(&state, "complete", &mut lifecycle_journal)?;

    assert_eq!(state, "completed");
    assert_eq!(second.identity.execution_id, "execution-2");
    assert!(matches!(
        lifecycle_journal[lifecycle_journal.len() - 2],
        Event::ExecutionCompleted
    ));
    assert!(
        matches!(lifecycle_journal.last(), Some(Event::StateChanged { to, .. }) if to == "completed")
    );
    assert_eq!(compiled.node_ids().count(), 1); // retry created no graph edge or node
    println!(
        "first attempt failed; second execution completed; state={state}; journal events={}",
        lifecycle_journal.len()
    );
    Ok(())
}
