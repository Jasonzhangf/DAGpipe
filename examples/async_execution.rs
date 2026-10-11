use pipeline_runtime::{
    compile, ArcContract, AsyncExecution, AsyncExecutionFailure, AsyncOperator, Cancellation,
    Event, ExecutionResult, Graph, Identity, IteratorKind, Node, OperatorContext, OperatorFailure,
    Registry, Runtime, Selector, ValueType,
};
use serde_json::{json, Value};
use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::task::{Context, Poll, Wake, Waker};

struct AsyncCopy;

impl AsyncOperator for AsyncCopy {
    fn name(&self) -> &'static str {
        "async-copy"
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

    fn execute<'a>(
        &'a self,
        input: Value,
        _: OperatorContext,
    ) -> Pin<Box<dyn Future<Output = Result<Value, OperatorFailure>> + Send + 'a>> {
        Box::pin(async move { Ok(input) })
    }
}

struct PendingFuture {
    dropped: Arc<AtomicUsize>,
}

impl Future for PendingFuture {
    type Output = Result<Value, OperatorFailure>;

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}

impl Drop for PendingFuture {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

struct PendingOp {
    dropped: Arc<AtomicUsize>,
}

impl AsyncOperator for PendingOp {
    fn name(&self) -> &'static str {
        "pending"
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

    fn execute<'a>(
        &'a self,
        _: Value,
        _: OperatorContext,
    ) -> Pin<Box<dyn Future<Output = Result<Value, OperatorFailure>> + Send + 'a>> {
        Box::pin(PendingFuture {
            dropped: self.dropped.clone(),
        })
    }
}

struct CountingWake(Arc<AtomicUsize>);

impl Wake for CountingWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn graph(operator: &str) -> Graph {
    Graph {
        id: "async-example".into(),
        version: "1".into(),
        inputs: vec![ArcContract {
            id: "source".into(),
            schema: ValueType::Object,
        }],
        nodes: vec![Node {
            id: "step".into(),
            operator: operator.into(),
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
    }
}

fn identity(graph: &Graph, execution_id: &str) -> Identity {
    Identity {
        project_id: "example".into(),
        graph_id: graph.id.clone(),
        graph_version: graph.version.clone(),
        execution_id: execution_id.into(),
        attempt_id: "attempt-1".into(),
    }
}

fn poll_once(
    execution: &mut AsyncExecution,
    waker: &Waker,
) -> Poll<Result<ExecutionResult, AsyncExecutionFailure>> {
    let mut context = Context::from_waker(waker);
    Pin::new(&mut *execution).poll(&mut context)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let wakes = Arc::new(AtomicUsize::new(0));
    let waker = Waker::from(Arc::new(CountingWake(wakes.clone())));

    let mut registry = Registry::default();
    registry.register_async(AsyncCopy)?;
    let copy_graph = graph("async-copy");
    let compiled = compile(copy_graph.clone(), &registry, &BTreeSet::new())?;
    let mut execution = Runtime::new(BTreeSet::new()).start_async(
        &compiled,
        identity(&copy_graph, "async-success"),
        HashMap::from([("source".into(), json!({"ok": true}))]),
        Cancellation::default(),
    )?;
    let result = match poll_once(&mut execution, &waker) {
        Poll::Ready(result) => result?,
        Poll::Pending => return Err("ready async operator stayed pending".into()),
    };
    assert_eq!(result.outputs["result"].payload, json!({"ok": true}));
    println!("async success: {}", result.outputs["result"].payload);

    let dropped = Arc::new(AtomicUsize::new(0));
    let mut pending_registry = Registry::default();
    pending_registry.register_async(PendingOp {
        dropped: dropped.clone(),
    })?;
    let pending_graph = graph("pending");
    let compiled = compile(pending_graph.clone(), &pending_registry, &BTreeSet::new())?;
    let cancellation = Cancellation::default();
    let mut execution = Runtime::new(BTreeSet::new()).start_async(
        &compiled,
        identity(&pending_graph, "async-cancel"),
        HashMap::from([("source".into(), json!({"ok": true}))]),
        cancellation.clone(),
    )?;
    assert!(poll_once(&mut execution, &waker).is_pending());
    execution.cancel();
    assert!(wakes.load(Ordering::SeqCst) > 0);
    let failure = match poll_once(&mut execution, &waker) {
        Poll::Ready(Err(failure)) => failure,
        Poll::Ready(Ok(_)) => return Err("cancel completed successfully".into()),
        Poll::Pending => return Err("cancel did not wake the pending execution".into()),
    };
    assert_eq!(failure.error.kind, pipeline_runtime::ErrorKind::Cancelled);
    println!("cancel terminal: {:?}", failure.error.kind);

    let dropped = Arc::new(AtomicUsize::new(0));
    let mut drop_registry = Registry::default();
    drop_registry.register_async(PendingOp {
        dropped: dropped.clone(),
    })?;
    let drop_graph = graph("pending");
    let compiled = compile(drop_graph.clone(), &drop_registry, &BTreeSet::new())?;
    let mut execution = Runtime::new(BTreeSet::new()).start_async(
        &compiled,
        identity(&drop_graph, "async-drop"),
        HashMap::from([("source".into(), json!({"ok": true}))]),
        Cancellation::default(),
    )?;
    let journal = execution.journal_handle();
    assert!(poll_once(&mut execution, &waker).is_pending());
    drop(execution);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    let snapshot = journal.snapshot();
    assert!(snapshot
        .iter()
        .any(|event| matches!(event, Event::ExecutionDropped)));
    assert!(!snapshot
        .iter()
        .any(|event| matches!(event, Event::ArcWritten { .. })));
    println!("drop journal facts: {}", snapshot.len());

    Ok(())
}
