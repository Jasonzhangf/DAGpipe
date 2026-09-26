//! Small deterministic DAG runtime.
//!
//! The compiler is the only path from an authoring [`Graph`] to execution. The
//! resulting [`CompiledGraph`] owns a frozen topology and resolved operators;
//! operators receive values, not access to the ARC store or scheduler.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::any::Any;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::num::NonZeroUsize;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;

pub type NodeId = String;
pub type ArcId = String;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Identity {
    pub project_id: String,
    pub graph_id: String,
    pub graph_version: String,
    pub execution_id: String,
    pub attempt_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum ValueType {
    Any,
    Null,
    Boolean,
    Number,
    String,
    Array,
    ArrayOf(Box<ValueType>),
    Object,
}

impl ValueType {
    fn accepts(&self, value: &Value) -> bool {
        match self {
            Self::Any => true,
            Self::Null => value.is_null(),
            Self::Boolean => value.is_boolean(),
            Self::Number => value.is_number(),
            Self::String => value.is_string(),
            Self::Array => value.is_array(),
            Self::ArrayOf(item_type) => value
                .as_array()
                .is_some_and(|items| items.iter().all(|item| item_type.accepts(item))),
            Self::Object => value.is_object(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArcContract {
    pub id: ArcId,
    pub schema: ValueType,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Edge {
    pub from: NodeId,
    pub to: NodeId,
    pub arc_id: ArcId,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct Selector {
    /// Top-level object keys to retain. Empty means retain all keys.
    pub include: BTreeSet<String>,
    /// Top-level object keys to remove after inclusion.
    pub exclude: BTreeSet<String>,
    #[serde(default)]
    pub predicate: Option<FieldPredicate>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum FieldPredicate {
    Exists {
        field: String,
    },
    Equals {
        field: String,
        value: Value,
    },
    IsType {
        field: String,
        value_type: ValueType,
    },
}

impl FieldPredicate {
    fn matches(&self, value: &Value) -> Result<bool, RuntimeError> {
        let object = value.as_object().ok_or_else(|| {
            RuntimeError::new(ErrorKind::Input, "selector predicate requires an object")
        })?;
        Ok(match self {
            Self::Exists { field } => object.contains_key(field),
            Self::Equals { field, value } => object.get(field) == Some(value),
            Self::IsType { field, value_type } => object
                .get(field)
                .is_some_and(|value| value_type.accepts(value)),
        })
    }
}

impl Selector {
    fn apply(&self, value: Value) -> Result<Option<Value>, RuntimeError> {
        if let Some(predicate) = &self.predicate {
            if !predicate.matches(&value)? {
                return Ok(None);
            }
        }
        if self.include.is_empty() && self.exclude.is_empty() {
            return Ok(Some(value));
        }
        let mut object = value.as_object().cloned().ok_or_else(|| {
            RuntimeError::new(ErrorKind::Input, "selector requires an object value")
        })?;
        object.retain(|key, _| {
            (self.include.is_empty() || self.include.contains(key)) && !self.exclude.contains(key)
        });
        Ok(Some(Value::Object(object)))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum IteratorKind {
    Whole,
    Items,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Node {
    pub id: NodeId,
    pub operator: String,
    pub operator_version: String,
    pub inputs: Vec<ArcId>,
    pub output: ArcContract,
    pub input_selector: Selector,
    pub output_selector: Selector,
    pub iterator: IteratorKind,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Graph {
    pub id: String,
    pub version: String,
    pub inputs: Vec<ArcContract>,
    pub nodes: Vec<Node>,
    pub edges: Vec<Edge>,
    pub outputs: Vec<ArcId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphTopology {
    pub order: Vec<NodeId>,
    pub waves: Vec<Vec<NodeId>>,
}

/// Validates the graph's DAG shape and returns its deterministic topological plan.
/// Operator registration, ARC schema compatibility, and effect capabilities are
/// validated separately by `compile` against the project's `Registry`.
pub fn graph_topology(graph: &Graph) -> Result<GraphTopology, CompileError> {
    if graph.id.is_empty() || graph.version.is_empty() {
        return Err(CompileError::new(
            "graph id and version are required".into(),
        ));
    }
    if graph.nodes.is_empty() {
        return Err(CompileError::new(
            "graph must contain at least one node".into(),
        ));
    }
    let mut input_ids = BTreeSet::new();
    for input in &graph.inputs {
        if input.id.is_empty() || !input_ids.insert(input.id.clone()) {
            return Err(CompileError::new(format!(
                "empty or duplicate input ARC `{}`",
                input.id
            )));
        }
    }
    if input_ids.len() != 1 {
        return Err(CompileError::new(
            "an audited SESE Graph must declare exactly one input ARC; validate each object flow as a separate Graph".into(),
        ));
    }
    let mut nodes = BTreeSet::new();
    let mut output_owner = HashMap::new();
    for node in &graph.nodes {
        if node.id.is_empty() || !nodes.insert(node.id.clone()) {
            return Err(CompileError::new(format!(
                "empty or duplicate node id `{}`",
                node.id
            )));
        }
        if node.operator.is_empty() || node.operator_version.is_empty() {
            return Err(CompileError::new(format!(
                "node `{}` must bind an Operator name and version",
                node.id
            )));
        }
        if node.inputs.is_empty() {
            return Err(CompileError::new(format!(
                "node `{}` must declare at least one input ARC",
                node.id
            )));
        }
        if node.inputs.iter().collect::<BTreeSet<_>>().len() != node.inputs.len() {
            return Err(CompileError::new(format!(
                "node `{}` declares duplicate input ARCs",
                node.id
            )));
        }
        if node.iterator == IteratorKind::Items && node.inputs.len() != 1 {
            return Err(CompileError::new(format!(
                "items iterator on `{}` requires exactly one input ARC",
                node.id
            )));
        }
        if node.output.id.is_empty() || input_ids.contains(&node.output.id) {
            return Err(CompileError::new(format!(
                "ARC `{}` has multiple declarations",
                node.output.id
            )));
        }
        if output_owner
            .insert(node.output.id.clone(), node.id.clone())
            .is_some()
        {
            return Err(CompileError::new(format!(
                "ARC `{}` has multiple node producers",
                node.output.id
            )));
        }
    }
    if graph.outputs.is_empty() {
        return Err(CompileError::new(
            "graph must declare at least one output ARC".into(),
        ));
    }
    let output_ids: BTreeSet<_> = graph.outputs.iter().collect();
    if output_ids.len() != graph.outputs.len() {
        return Err(CompileError::new(
            "graph declares a duplicate output ARC".into(),
        ));
    }
    if output_ids.len() != 1 {
        return Err(CompileError::new(
            "an audited SESE Graph must declare exactly one output ARC; validate each object flow as a separate Graph".into(),
        ));
    }
    for output in &graph.outputs {
        if !output_owner.contains_key(output) {
            return Err(CompileError::new(format!(
                "graph output `{output}` is not produced by a node"
            )));
        }
    }

    let mut incoming: HashMap<NodeId, usize> =
        nodes.iter().cloned().map(|node| (node, 0)).collect();
    let mut dependents = HashMap::<NodeId, Vec<NodeId>>::new();
    let mut predecessors = HashMap::<NodeId, Vec<NodeId>>::new();
    let mut edge_keys = BTreeSet::new();
    for edge in &graph.edges {
        if !nodes.contains(&edge.from) {
            return Err(CompileError::new(format!(
                "edge references missing source node `{}`",
                edge.from
            )));
        }
        if !nodes.contains(&edge.to) {
            return Err(CompileError::new(format!(
                "edge references missing target node `{}`",
                edge.to
            )));
        }
        let from = graph
            .nodes
            .iter()
            .find(|node| node.id == edge.from)
            .expect("validated source");
        let to = graph
            .nodes
            .iter()
            .find(|node| node.id == edge.to)
            .expect("validated target");
        if from.output.id != edge.arc_id {
            return Err(CompileError::new(format!(
                "edge ARC `{}` is not output of `{}`",
                edge.arc_id, edge.from
            )));
        }
        if !to.inputs.contains(&edge.arc_id) {
            return Err(CompileError::new(format!(
                "edge ARC `{}` is not declared input of `{}`",
                edge.arc_id, edge.to
            )));
        }
        if !edge_keys.insert((edge.to.clone(), edge.arc_id.clone())) {
            return Err(CompileError::new(format!(
                "duplicate dependency edge into `{}` for ARC `{}`",
                edge.to, edge.arc_id
            )));
        }
        *incoming.get_mut(&edge.to).expect("validated target") += 1;
        dependents
            .entry(edge.from.clone())
            .or_default()
            .push(edge.to.clone());
        predecessors
            .entry(edge.to.clone())
            .or_default()
            .push(edge.from.clone());
    }
    let mut external_consumers = HashMap::<ArcId, Vec<NodeId>>::new();
    for node in &graph.nodes {
        for input in &node.inputs {
            if output_owner.contains_key(input) {
                if !edge_keys.contains(&(node.id.clone(), input.clone())) {
                    return Err(CompileError::new(format!(
                        "node `{}` reads `{input}` without a graph edge",
                        node.id
                    )));
                }
            } else if !input_ids.contains(input) {
                return Err(CompileError::new(format!(
                    "node `{}` reads undeclared ARC `{input}`",
                    node.id
                )));
            } else {
                external_consumers
                    .entry(input.clone())
                    .or_default()
                    .push(node.id.clone());
            }
        }
    }
    for children in dependents.values_mut() {
        children.sort();
    }

    let mut ready: BTreeSet<NodeId> = incoming
        .iter()
        .filter(|(_, count)| **count == 0)
        .map(|(id, _)| id.clone())
        .collect();
    let mut order = Vec::with_capacity(nodes.len());
    let mut waves = Vec::new();
    while !ready.is_empty() {
        let wave: Vec<_> = std::mem::take(&mut ready).into_iter().collect();
        for next in &wave {
            order.push(next.clone());
            if let Some(children) = dependents.get(next) {
                for child in children {
                    let count = incoming.get_mut(child).expect("validated target");
                    *count -= 1;
                    if *count == 0 {
                        ready.insert(child.clone());
                    }
                }
            }
        }
        waves.push(wave);
    }
    if order.len() != nodes.len() {
        return Err(CompileError::new("graph contains a cycle".into()));
    }

    for source in &graph.inputs {
        let mut pending = external_consumers
            .get(&source.id)
            .cloned()
            .unwrap_or_default();
        if pending.is_empty() {
            return Err(CompileError::new(format!(
                "source ARC `{}` is not consumed by any node",
                source.id
            )));
        }
        let mut reachable = BTreeSet::new();
        while let Some(node) = pending.pop() {
            if reachable.insert(node.clone()) {
                if let Some(children) = dependents.get(&node) {
                    pending.extend(children.iter().cloned());
                }
            }
        }
        let reached_outputs: Vec<_> = graph
            .outputs
            .iter()
            .filter(|arc| {
                output_owner
                    .get(*arc)
                    .is_some_and(|owner| reachable.contains(owner))
            })
            .collect();
        if reached_outputs.len() != 1 {
            return Err(CompileError::new(format!(
                "source ARC `{}` reaches {} graph outputs; each source must reach exactly one",
                source.id,
                reached_outputs.len()
            )));
        }
    }

    let mut can_reach_output: BTreeSet<NodeId> = graph
        .outputs
        .iter()
        .filter_map(|arc| output_owner.get(arc).cloned())
        .collect();
    let mut pending: Vec<_> = can_reach_output.iter().cloned().collect();
    while let Some(node) = pending.pop() {
        if let Some(parents) = predecessors.get(&node) {
            for parent in parents {
                if can_reach_output.insert(parent.clone()) {
                    pending.push(parent.clone());
                }
            }
        }
    }
    if let Some(dead) = nodes.iter().find(|id| !can_reach_output.contains(*id)) {
        return Err(CompileError::new(format!(
            "node `{dead}` cannot reach a declared graph output"
        )));
    }
    Ok(GraphTopology { order, waves })
}

pub fn parse_graph_json(source: &str) -> Result<Graph, CompileError> {
    serde_json::from_str(source)
        .map_err(|error| CompileError::new(format!("invalid graph JSON: {error}")))
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EffectReplay {
    Replayable,
    Idempotent,
    NonReplayable,
    RequiresConfirmation,
}

const MAX_OPERATOR_EVENT_COUNT: usize = 16;
const MAX_OPERATOR_EVENT_ID_BYTES: usize = 64;

fn valid_operator_event_id(name: &str) -> bool {
    let mut bytes = name.bytes();
    matches!(bytes.next(), Some(b'a'..=b'z'))
        && name.len() <= MAX_OPERATOR_EVENT_ID_BYTES
        && bytes.all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-' | b'.')
        })
}

pub trait Operator: Send + Sync {
    fn name(&self) -> &'static str;
    fn version(&self) -> &'static str;
    fn input_type(&self) -> ValueType {
        ValueType::Any
    }
    fn output_type(&self) -> ValueType {
        ValueType::Any
    }
    fn effects(&self) -> &'static [&'static str] {
        &[]
    }
    fn replay(&self) -> EffectReplay {
        if self.effects().is_empty() {
            EffectReplay::Replayable
        } else {
            EffectReplay::RequiresConfirmation
        }
    }
    fn declared_events(&self) -> &'static [&'static str] {
        &[]
    }
    fn emitted_events(&self, _output: &Value) -> Vec<OperatorEvent> {
        Vec::new()
    }
    fn execute(&self, input: Value, context: &OperatorContext) -> Result<Value, String>;
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct OperatorEvent {
    pub name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OperatorContext {
    pub identity: Identity,
    pub node_id: NodeId,
}

#[derive(Default, Clone)]
pub struct Registry {
    operators: HashMap<(String, String), Arc<dyn Operator>>,
}

impl Registry {
    pub fn register(&mut self, operator: impl Operator + 'static) -> Result<(), CompileError> {
        let name = operator.name().to_owned();
        let version = operator.version().to_owned();
        let key = (name.clone(), version.clone());
        if self.operators.contains_key(&key) {
            return Err(CompileError::new(format!(
                "duplicate operator `{name}@{version}`"
            )));
        }
        self.operators.insert(key, Arc::new(operator));
        Ok(())
    }
}

#[derive(Clone)]
struct CompiledNode {
    node: Node,
    operator: Arc<dyn Operator>,
    effects: Vec<String>,
    declared_events: BTreeSet<String>,
}

/// Opaque immutable graph. Fields stay private so callers cannot mutate runtime topology.
#[derive(Clone)]
pub struct CompiledGraph {
    id: String,
    version: String,
    inputs: Vec<ArcContract>,
    nodes: BTreeMap<NodeId, CompiledNode>,
    order: Vec<NodeId>,
    waves: Vec<Vec<NodeId>>,
    outputs: Vec<ArcId>,
}

impl CompiledGraph {
    pub fn id(&self) -> &str {
        &self.id
    }
    pub fn version(&self) -> &str {
        &self.version
    }
    pub fn node_ids(&self) -> impl Iterator<Item = &str> {
        self.order.iter().map(String::as_str)
    }
}

impl fmt::Debug for CompiledGraph {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompiledGraph")
            .field("id", &self.id)
            .field("version", &self.version)
            .field("node_ids", &self.order)
            .field("waves", &self.waves)
            .field("outputs", &self.outputs)
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompileError {
    pub message: String,
}
impl CompileError {
    fn new(message: String) -> Self {
        Self { message }
    }
}
impl fmt::Display for CompileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for CompileError {}

pub fn compile(
    graph: Graph,
    registry: &Registry,
    capabilities: &BTreeSet<String>,
) -> Result<CompiledGraph, CompileError> {
    let topology = graph_topology(&graph)?;
    let mut nodes = BTreeMap::new();
    for node in &graph.nodes {
        let operator_key = (node.operator.clone(), node.operator_version.clone());
        let operator = registry
            .operators
            .get(&operator_key)
            .cloned()
            .ok_or_else(|| {
                CompileError::new(format!(
                    "node `{}` references missing operator `{}@{}`",
                    node.id, node.operator, node.operator_version
                ))
            })?;
        let output_contract_ok = match node.iterator {
            IteratorKind::Whole => type_compatible(&operator.output_type(), &node.output.schema),
            IteratorKind::Items => match &node.output.schema {
                ValueType::Any | ValueType::Array => true,
                ValueType::ArrayOf(item_type) => {
                    type_compatible(&operator.output_type(), item_type)
                }
                _ => false,
            },
        };
        if !output_contract_ok {
            return Err(CompileError::new(format!(
                "node `{}` output contract is incompatible with operator `{}`",
                node.id, node.operator
            )));
        }
        if operator.declared_events().len() > MAX_OPERATOR_EVENT_COUNT {
            return Err(CompileError::new(format!(
                "node `{}` declares too many operator events",
                node.id
            )));
        }
        let mut declared_events = BTreeSet::new();
        for event_name in operator.declared_events() {
            if !valid_operator_event_id(event_name)
                || !declared_events.insert((*event_name).to_owned())
            {
                return Err(CompileError::new(format!(
                    "node `{}` declares an invalid or duplicate operator event identifier",
                    node.id
                )));
            }
        }
        for effect in operator.effects() {
            if !capabilities.contains(*effect) {
                return Err(CompileError::new(format!(
                    "node `{}` requires undeclared capability `{effect}`",
                    node.id
                )));
            }
        }
        let effects = operator_effects(&operator);
        nodes.insert(
            node.id.clone(),
            CompiledNode {
                node: node.clone(),
                operator,
                effects,
                declared_events,
            },
        );
    }
    for node in nodes.values() {
        let mut source_types = Vec::with_capacity(node.node.inputs.len());
        for input in &node.node.inputs {
            let source_type = nodes
                .values()
                .find(|candidate| candidate.node.output.id == *input)
                .map(|candidate| candidate.node.output.schema.clone())
                .or_else(|| {
                    graph
                        .inputs
                        .iter()
                        .find(|decl| decl.id == *input)
                        .map(|decl| decl.schema.clone())
                })
                .expect("input validated");
            source_types.push(source_type);
        }
        let operator_input = node.operator.input_type();
        let input_contract_ok = match node.node.iterator {
            IteratorKind::Whole if source_types.len() == 1 => {
                type_compatible(&source_types[0], &operator_input)
            }
            IteratorKind::Whole => match &operator_input {
                ValueType::Any | ValueType::Array => true,
                ValueType::ArrayOf(item_type) => source_types
                    .iter()
                    .all(|source_type| type_compatible(source_type, item_type)),
                _ => false,
            },
            IteratorKind::Items => match source_types.first() {
                Some(ValueType::Any | ValueType::Array) => true,
                Some(ValueType::ArrayOf(item_type)) => type_compatible(item_type, &operator_input),
                _ => false,
            },
        };
        if !input_contract_ok {
            return Err(CompileError::new(format!(
                "node `{}` input binding is incompatible with operator `{}` input",
                node.node.id, node.node.operator
            )));
        }
    }
    Ok(CompiledGraph {
        id: graph.id,
        version: graph.version,
        inputs: graph.inputs,
        nodes,
        order: topology.order,
        waves: topology.waves,
        outputs: graph.outputs,
    })
}

fn operator_effects(operator: &Arc<dyn Operator>) -> Vec<String> {
    operator.effects().iter().map(|s| (*s).to_owned()).collect()
}

fn panic_message(payload: &(dyn Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".into()
    }
}

fn call_user<T>(
    kind: ErrorKind,
    label: &str,
    call: impl FnOnce() -> Result<T, String>,
) -> Result<T, RuntimeError> {
    catch_unwind(AssertUnwindSafe(call))
        .map_err(|panic| {
            RuntimeError::new(
                kind,
                format!("{label} panicked: {}", panic_message(&*panic)),
            )
        })?
        .map_err(|message| RuntimeError::new(kind, message))
}

fn call_user_value<T>(
    kind: ErrorKind,
    label: &str,
    call: impl FnOnce() -> T,
) -> Result<T, RuntimeError> {
    catch_unwind(AssertUnwindSafe(call)).map_err(|panic| {
        RuntimeError::new(
            kind,
            format!("{label} panicked: {}", panic_message(&*panic)),
        )
    })
}

fn output_selector_error(error: RuntimeError) -> RuntimeError {
    RuntimeError::new(ErrorKind::Output, error.message)
}

fn type_compatible(source: &ValueType, target: &ValueType) -> bool {
    match (source, target) {
        (ValueType::Any, _) | (_, ValueType::Any) => true,
        (ValueType::ArrayOf(source_item), ValueType::ArrayOf(target_item)) => {
            type_compatible(source_item, target_item)
        }
        (ValueType::ArrayOf(_), ValueType::Array) => true,
        _ => source == target,
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ArcValue {
    pub id: ArcId,
    pub version: u64,
    pub schema: ValueType,
    pub payload: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Event {
    ExecutionStarted {
        identity: Identity,
        graph_id: String,
        graph_version: String,
    },
    NodeScheduled {
        node_id: NodeId,
    },
    NodeStarted {
        node_id: NodeId,
        input_arc_refs: Vec<String>,
    },
    ArcRead {
        node_id: NodeId,
        arc_ref: String,
    },
    OperatorStarted {
        node_id: NodeId,
        invocation_index: Option<usize>,
        operator: String,
        operator_version: String,
        effects: Vec<String>,
        replay: EffectReplay,
    },
    OperatorEventEmitted {
        node_id: NodeId,
        name: String,
        arc_refs: Vec<String>,
    },
    OperatorCompleted {
        node_id: NodeId,
        invocation_index: Option<usize>,
    },
    ArcWritten {
        node_id: NodeId,
        arc_ref: String,
    },
    NodeCompleted {
        node_id: NodeId,
        output_arc_ref: String,
    },
    NodeFailed {
        node_id: NodeId,
        error_kind: ErrorKind,
    },
    StateChanged {
        from: String,
        event: String,
        to: String,
    },
    Cancelled,
    ExecutionCompleted,
    ExecutionFailed {
        error_kind: ErrorKind,
    },
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ErrorKind {
    Graph,
    Input,
    Operator,
    Output,
    Transition,
    Effect,
    Cancelled,
    Hook,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeError {
    pub kind: ErrorKind,
    pub message: String,
}
impl RuntimeError {
    fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}
impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for RuntimeError {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionFailure {
    pub error: RuntimeError,
    pub journal: Vec<Event>,
}

impl ExecutionFailure {
    fn new(error: RuntimeError, journal: Vec<Event>) -> Self {
        Self { error, journal }
    }
}

impl fmt::Display for ExecutionFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(f)
    }
}

impl std::error::Error for ExecutionFailure {}

pub trait Hook: Send + Sync {
    fn before_node(&self, _node: &str) -> Result<(), String> {
        Ok(())
    }
    fn after_node(&self, _node: &str) -> Result<(), String> {
        Ok(())
    }
    fn on_error(&self, _node: &str, _error: &RuntimeError) -> Result<(), String> {
        Ok(())
    }
}

#[derive(Default)]
pub struct Cancellation {
    cancelled: AtomicBool,
}
impl Cancellation {
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }
    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

#[derive(Clone)]
struct NodeTask {
    node_id: NodeId,
    inputs: Result<Vec<(Value, String)>, RuntimeError>,
}

struct NodeOutcome {
    node_id: NodeId,
    events: Vec<Event>,
    output: Option<ArcValue>,
    error: Option<RuntimeError>,
}

impl NodeOutcome {
    fn failed(node_id: NodeId, events: Vec<Event>, error: RuntimeError) -> Self {
        Self {
            node_id,
            events,
            output: None,
            error: Some(error),
        }
    }
}

fn execute_node(
    compiled: &CompiledNode,
    task: NodeTask,
    identity: &Identity,
    capabilities: &BTreeSet<String>,
    hooks: &[Arc<dyn Hook>],
) -> NodeOutcome {
    let node_id = task.node_id;
    for effect in &compiled.effects {
        if !capabilities.contains(effect) {
            return NodeOutcome::failed(
                node_id,
                Vec::new(),
                RuntimeError::new(
                    ErrorKind::Effect,
                    format!("runtime lacks capability `{effect}`"),
                ),
            );
        }
    }
    let mut events = Vec::new();
    for hook in hooks {
        if let Err(error) = call_user(ErrorKind::Hook, "before_node hook", || {
            hook.before_node(&node_id)
        }) {
            return NodeOutcome::failed(node_id, events, error);
        }
    }
    let (mut input_values, input_refs): (Vec<Value>, Vec<String>) = match task.inputs {
        Ok(inputs) => inputs.into_iter().unzip(),
        Err(error) => return NodeOutcome::failed(node_id, events, error),
    };
    events.push(Event::NodeStarted {
        node_id: node_id.clone(),
        input_arc_refs: input_refs.clone(),
    });
    for arc_ref in input_refs {
        events.push(Event::ArcRead {
            node_id: node_id.clone(),
            arc_ref,
        });
    }
    if compiled.node.iterator == IteratorKind::Whole {
        for value in &mut input_values {
            match compiled.node.input_selector.apply(value.clone()) {
                Ok(Some(selected)) => *value = selected,
                Ok(None) => {
                    return NodeOutcome::failed(
                        node_id,
                        events,
                        RuntimeError::new(
                            ErrorKind::Input,
                            "input selector predicate rejected node input",
                        ),
                    );
                }
                Err(error) => return NodeOutcome::failed(node_id, events, error),
            }
        }
    }
    let input = if input_values.len() == 1 {
        input_values.remove(0)
    } else {
        Value::Array(input_values)
    };
    let replay = match call_user_value(ErrorKind::Operator, "operator replay declaration", || {
        compiled.operator.replay()
    }) {
        Ok(replay) => replay,
        Err(error) => return NodeOutcome::failed(node_id, events, error),
    };
    let context = OperatorContext {
        identity: identity.clone(),
        node_id: node_id.clone(),
    };
    let result = match compiled.node.iterator {
        IteratorKind::Whole => {
            let operator_input_type =
                match call_user_value(ErrorKind::Operator, "operator input contract", || {
                    compiled.operator.input_type()
                }) {
                    Ok(value_type) => value_type,
                    Err(error) => return NodeOutcome::failed(node_id, events, error),
                };
            if !operator_input_type.accepts(&input) {
                return NodeOutcome::failed(
                    node_id,
                    events,
                    RuntimeError::new(
                        ErrorKind::Input,
                        format!(
                            "selected input violates operator `{}` input contract",
                            compiled.node.operator
                        ),
                    ),
                );
            }
            events.push(Event::OperatorStarted {
                node_id: node_id.clone(),
                invocation_index: None,
                operator: compiled.node.operator.clone(),
                operator_version: compiled.node.operator_version.clone(),
                effects: compiled.effects.clone(),
                replay: replay.clone(),
            });
            let value = match call_user(ErrorKind::Operator, "operator execute", || {
                compiled.operator.execute(input, &context)
            }) {
                Ok(value) => {
                    events.push(Event::OperatorCompleted {
                        node_id: node_id.clone(),
                        invocation_index: None,
                    });
                    value
                }
                Err(error) => return NodeOutcome::failed(node_id, events, error),
            };
            let operator_output_type =
                match call_user_value(ErrorKind::Operator, "operator output contract", || {
                    compiled.operator.output_type()
                }) {
                    Ok(value_type) => value_type,
                    Err(error) => return NodeOutcome::failed(node_id, events, error),
                };
            if !operator_output_type.accepts(&value) {
                return NodeOutcome::failed(
                    node_id,
                    events,
                    RuntimeError::new(
                        ErrorKind::Output,
                        format!(
                            "operator `{}` output violates its declared output contract",
                            compiled.node.operator
                        ),
                    ),
                );
            }
            Ok(value)
        }
        IteratorKind::Items => execute_items(
            compiled,
            input,
            &context,
            &compiled.node.input_selector,
            &replay,
            &mut events,
        ),
    };
    let value = match result {
        Ok(value) => value,
        Err(error) => return NodeOutcome::failed(node_id, events, error),
    };
    if !compiled.node.output.schema.accepts(&value) {
        return NodeOutcome::failed(
            node_id,
            events,
            RuntimeError::new(
                ErrorKind::Output,
                format!(
                    "operator `{}` output violates ARC `{}` schema",
                    compiled.node.operator, compiled.node.output.id
                ),
            ),
        );
    }
    let value = match select_output(
        value,
        &compiled.node.output_selector,
        &compiled.node.iterator,
    ) {
        Ok(value) => value,
        Err(error) => return NodeOutcome::failed(node_id, events, error),
    };
    if !compiled.node.output.schema.accepts(&value) {
        return NodeOutcome::failed(
            node_id,
            events,
            RuntimeError::new(
                ErrorKind::Output,
                format!(
                    "output selector result violates ARC `{}` schema",
                    compiled.node.output.id
                ),
            ),
        );
    }
    let output = ArcValue {
        id: compiled.node.output.id.clone(),
        version: 1,
        schema: compiled.node.output.schema.clone(),
        payload: value,
    };
    let output_ref = format!(
        "arc://{}/{}@{}",
        identity.execution_id, output.id, output.version
    );
    let emitted_events = match catch_unwind(AssertUnwindSafe(|| {
        compiled.operator.emitted_events(&output.payload)
    })) {
        Ok(events) => events,
        Err(panic) => {
            return NodeOutcome {
                node_id,
                events,
                output: Some(output),
                error: Some(RuntimeError::new(
                    ErrorKind::Operator,
                    format!(
                        "operator event emission panicked: {}",
                        panic_message(&*panic)
                    ),
                )),
            };
        }
    };
    let mut emitted_event_names = BTreeSet::new();
    if emitted_events.len() > MAX_OPERATOR_EVENT_COUNT
        || emitted_events.iter().any(|event| {
            !valid_operator_event_id(&event.name)
                || !compiled.declared_events.contains(&event.name)
                || !emitted_event_names.insert(event.name.as_str())
        })
    {
        return NodeOutcome::failed(
            node_id,
            events,
            RuntimeError::new(
                ErrorKind::Operator,
                "operator emitted an undeclared or invalid event identifier",
            ),
        );
    }
    events.push(Event::ArcWritten {
        node_id: node_id.clone(),
        arc_ref: output_ref.clone(),
    });
    for event in emitted_events {
        events.push(Event::OperatorEventEmitted {
            node_id: node_id.clone(),
            name: event.name,
            arc_refs: vec![output_ref.clone()],
        });
    }
    for hook in hooks {
        if let Err(error) = call_user(ErrorKind::Hook, "after_node hook", || {
            hook.after_node(&node_id)
        }) {
            return NodeOutcome {
                node_id,
                events,
                output: Some(output),
                error: Some(error),
            };
        }
    }
    events.push(Event::NodeCompleted {
        node_id: node_id.clone(),
        output_arc_ref: output_ref,
    });
    NodeOutcome {
        node_id,
        events,
        output: Some(output),
        error: None,
    }
}

fn execute_node_guarded(
    compiled: &CompiledNode,
    task: NodeTask,
    identity: &Identity,
    capabilities: &BTreeSet<String>,
    hooks: &[Arc<dyn Hook>],
) -> NodeOutcome {
    let node_id = task.node_id.clone();
    match catch_unwind(AssertUnwindSafe(|| {
        execute_node(compiled, task, identity, capabilities, hooks)
    })) {
        Ok(outcome) => outcome,
        Err(panic) => NodeOutcome::failed(
            node_id,
            Vec::new(),
            RuntimeError::new(
                ErrorKind::Operator,
                format!(
                    "node execution panicked outside a guarded callback: {}",
                    panic_message(&*panic)
                ),
            ),
        ),
    }
}

pub struct Runtime {
    capabilities: BTreeSet<String>,
    hooks: Vec<Arc<dyn Hook>>,
    max_parallelism: NonZeroUsize,
}
impl Runtime {
    pub fn new(capabilities: BTreeSet<String>) -> Self {
        Self {
            capabilities,
            hooks: Vec::new(),
            max_parallelism: NonZeroUsize::new(1).expect("one is nonzero"),
        }
    }
    /// Sets the maximum number of independent nodes executed concurrently.
    /// The default is one, so parallel execution must be explicitly enabled.
    pub fn with_max_parallelism(mut self, max_parallelism: NonZeroUsize) -> Self {
        self.max_parallelism = max_parallelism;
        self
    }
    pub fn add_hook(&mut self, hook: impl Hook + 'static) {
        self.hooks.push(Arc::new(hook));
    }

    pub fn run(
        &self,
        graph: &CompiledGraph,
        identity: Identity,
        inputs: HashMap<ArcId, Value>,
        cancellation: &Cancellation,
    ) -> Result<ExecutionResult, ExecutionFailure> {
        if identity.project_id.is_empty()
            || identity.execution_id.is_empty()
            || identity.attempt_id.is_empty()
        {
            return Err(fail_execution(
                RuntimeError::new(
                    ErrorKind::Graph,
                    "project, execution, and attempt identity are required",
                ),
                vec![],
            ));
        }
        if identity.graph_id != graph.id || identity.graph_version != graph.version {
            return Err(fail_execution(
                RuntimeError::new(
                    ErrorKind::Graph,
                    "execution identity does not match compiled graph",
                ),
                vec![],
            ));
        }
        let mut journal = vec![Event::ExecutionStarted {
            identity: identity.clone(),
            graph_id: graph.id.clone(),
            graph_version: graph.version.clone(),
        }];
        let mut arcs = HashMap::<ArcId, ArcValue>::new();
        for contract in &graph.inputs {
            let payload = match inputs.get(&contract.id).cloned() {
                Some(payload) => payload,
                None => {
                    return Err(fail_execution(
                        RuntimeError::new(
                            ErrorKind::Input,
                            format!("missing graph input ARC `{}`", contract.id),
                        ),
                        journal,
                    ));
                }
            };
            if !contract.schema.accepts(&payload) {
                return Err(fail_execution(
                    RuntimeError::new(
                        ErrorKind::Input,
                        format!("graph input ARC `{}` violates schema", contract.id),
                    ),
                    journal,
                ));
            }
            arcs.insert(
                contract.id.clone(),
                ArcValue {
                    id: contract.id.clone(),
                    version: 1,
                    schema: contract.schema.clone(),
                    payload,
                },
            );
        }
        if let Some(extra) = inputs
            .keys()
            .find(|id| !graph.inputs.iter().any(|input| input.id == **id))
        {
            return Err(fail_execution(
                RuntimeError::new(
                    ErrorKind::Input,
                    format!("undeclared graph input ARC `{extra}`"),
                ),
                journal,
            ));
        }
        for wave in &graph.waves {
            if cancellation.is_cancelled() {
                journal.push(Event::Cancelled);
                journal.push(Event::ExecutionFailed {
                    error_kind: ErrorKind::Cancelled,
                });
                return Err(ExecutionFailure::new(
                    RuntimeError::new(ErrorKind::Cancelled, "execution cancelled"),
                    journal,
                ));
            }
            for node_id in wave {
                journal.push(Event::NodeScheduled {
                    node_id: node_id.clone(),
                });
            }

            let tasks: Vec<_> = wave
                .iter()
                .map(|node_id| {
                    let compiled = graph.nodes.get(node_id).expect("compiled node exists");
                    let inputs = compiled
                        .node
                        .inputs
                        .iter()
                        .map(|arc_id| {
                            arcs.get(arc_id)
                                .map(|arc| {
                                    (
                                        arc.payload.clone(),
                                        format!(
                                            "arc://{}/{}@{}",
                                            identity.execution_id, arc.id, arc.version
                                        ),
                                    )
                                })
                                .ok_or_else(|| {
                                    RuntimeError::new(
                                        ErrorKind::Input,
                                        format!(
                                            "ARC `{arc_id}` is not available to node `{node_id}`"
                                        ),
                                    )
                                })
                        })
                        .collect::<Result<Vec<_>, _>>();
                    NodeTask {
                        node_id: node_id.clone(),
                        inputs,
                    }
                })
                .collect();
            let mut outcomes = Vec::with_capacity(tasks.len());
            if self.max_parallelism.get() == 1 {
                for task in tasks {
                    let compiled = graph.nodes.get(&task.node_id).expect("compiled node");
                    outcomes.push(execute_node_guarded(
                        compiled,
                        task,
                        &identity,
                        &self.capabilities,
                        &self.hooks,
                    ));
                }
            } else {
                for batch in tasks.chunks(self.max_parallelism.get()) {
                    let batch_outcomes = thread::scope(|scope| {
                        let handles: Vec<_> = batch
                            .iter()
                            .map(|task| {
                                let compiled =
                                    graph.nodes.get(&task.node_id).expect("compiled node");
                                let task = task.clone();
                                let identity = &identity;
                                let capabilities = &self.capabilities;
                                let hooks = &self.hooks;
                                scope.spawn(move || {
                                    execute_node_guarded(
                                        compiled,
                                        task,
                                        identity,
                                        capabilities,
                                        hooks,
                                    )
                                })
                            })
                            .collect();
                        handles
                            .into_iter()
                            .zip(batch)
                            .map(|(handle, task)| {
                                handle.join().unwrap_or_else(|_| {
                                    NodeOutcome::failed(
                                        task.node_id.clone(),
                                        Vec::new(),
                                        RuntimeError::new(
                                            ErrorKind::Operator,
                                            "node worker panicked",
                                        ),
                                    )
                                })
                            })
                            .collect::<Vec<_>>()
                    });
                    outcomes.extend(batch_outcomes);
                }
            }

            let mut primary_error = None;
            for mut outcome in outcomes {
                journal.append(&mut outcome.events);
                if let Some(output) = outcome.output {
                    arcs.insert(output.id.clone(), output);
                }
                if let Some(error) = outcome.error {
                    journal.push(Event::NodeFailed {
                        node_id: outcome.node_id.clone(),
                        error_kind: error.kind,
                    });
                    let mut reported_error = error.clone();
                    for hook in &self.hooks {
                        if let Err(hook_error) = call_user(ErrorKind::Hook, "on_error hook", || {
                            hook.on_error(&outcome.node_id, &error)
                        }) {
                            reported_error = RuntimeError::new(
                                ErrorKind::Hook,
                                format!("{}; on_error hook failed: {hook_error}", error.message),
                            );
                            break;
                        }
                    }
                    if primary_error.is_none() {
                        primary_error = Some(reported_error);
                    }
                }
            }
            if let Some(error) = primary_error {
                journal.push(Event::ExecutionFailed {
                    error_kind: error.kind,
                });
                return Err(ExecutionFailure::new(error, journal));
            }
        }
        if cancellation.is_cancelled() {
            journal.push(Event::Cancelled);
            journal.push(Event::ExecutionFailed {
                error_kind: ErrorKind::Cancelled,
            });
            return Err(ExecutionFailure::new(
                RuntimeError::new(ErrorKind::Cancelled, "execution cancelled"),
                journal,
            ));
        }
        journal.push(Event::ExecutionCompleted);
        let outputs = graph
            .outputs
            .iter()
            .map(|id| {
                (
                    id.clone(),
                    arcs.get(id).expect("compiled output exists").clone(),
                )
            })
            .collect();
        Ok(ExecutionResult {
            identity,
            outputs,
            journal,
        })
    }
}

fn execute_items(
    compiled: &CompiledNode,
    input: Value,
    context: &OperatorContext,
    selector: &Selector,
    replay: &EffectReplay,
    events: &mut Vec<Event>,
) -> Result<Value, RuntimeError> {
    let input_type = call_user_value(ErrorKind::Operator, "operator input contract", || {
        compiled.operator.input_type()
    })?;
    let output_type = call_user_value(ErrorKind::Operator, "operator output contract", || {
        compiled.operator.output_type()
    })?;
    let items = input.as_array().ok_or_else(|| {
        RuntimeError::new(ErrorKind::Input, "items iterator requires an array input")
    })?;
    let operator_name = compiled.node.operator.as_str();
    let mut output = Vec::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        let Some(selected) = selector.apply(item.clone())? else {
            continue;
        };
        if !input_type.accepts(&selected) {
            return Err(RuntimeError::new(
                ErrorKind::Input,
                format!(
                    "selected item violates operator `{}` input contract",
                    operator_name
                ),
            ));
        }
        events.push(Event::OperatorStarted {
            node_id: compiled.node.id.clone(),
            invocation_index: Some(index),
            operator: operator_name.into(),
            operator_version: compiled.node.operator_version.clone(),
            effects: compiled.effects.clone(),
            replay: replay.clone(),
        });
        let result = call_user(ErrorKind::Operator, "operator execute", || {
            compiled.operator.execute(selected, context)
        })?;
        events.push(Event::OperatorCompleted {
            node_id: compiled.node.id.clone(),
            invocation_index: Some(index),
        });
        if !output_type.accepts(&result) {
            return Err(RuntimeError::new(
                ErrorKind::Output,
                format!(
                    "operator `{}` item output violates its declared output contract",
                    operator_name
                ),
            ));
        }
        output.push(result);
    }
    Ok(Value::Array(output))
}

fn select_output(
    value: Value,
    selector: &Selector,
    iterator: &IteratorKind,
) -> Result<Value, RuntimeError> {
    if *iterator == IteratorKind::Items {
        let values = value.as_array().ok_or_else(|| {
            RuntimeError::new(
                ErrorKind::Output,
                "items iterator returned a non-array output",
            )
        })?;
        values
            .iter()
            .cloned()
            .map(|item| selector.apply(item).map_err(output_selector_error))
            .collect::<Result<Vec<_>, _>>()
            .map(|selected| Value::Array(selected.into_iter().flatten().collect()))
    } else {
        selector
            .apply(value)
            .map_err(output_selector_error)?
            .ok_or_else(|| {
                RuntimeError::new(
                    ErrorKind::Output,
                    "output selector predicate rejected node output",
                )
            })
    }
}

fn fail_execution(error: RuntimeError, mut journal: Vec<Event>) -> ExecutionFailure {
    journal.push(Event::ExecutionFailed {
        error_kind: error.kind,
    });
    ExecutionFailure::new(error, journal)
}

#[derive(Clone, Debug)]
pub struct Transition {
    pub from: String,
    pub event: String,
    pub to: String,
}

#[derive(Clone, Debug)]
pub struct StateMachine {
    states: BTreeSet<String>,
    transitions: BTreeMap<(String, String), String>,
}

impl StateMachine {
    pub fn new(
        states: impl IntoIterator<Item = String>,
        transitions: Vec<Transition>,
    ) -> Result<Self, CompileError> {
        let states: BTreeSet<_> = states.into_iter().collect();
        if states.is_empty() {
            return Err(CompileError::new(
                "state machine must declare states".into(),
            ));
        }
        let mut table = BTreeMap::new();
        for transition in transitions {
            if !states.contains(&transition.from) || !states.contains(&transition.to) {
                return Err(CompileError::new(format!(
                    "transition `{}` -> `{}` references undeclared state",
                    transition.from, transition.to
                )));
            }
            let key = (transition.from, transition.event);
            if table.insert(key.clone(), transition.to).is_some() {
                return Err(CompileError::new(format!(
                    "duplicate transition for `{}` + `{}`",
                    key.0, key.1
                )));
            }
        }
        Ok(Self {
            states,
            transitions: table,
        })
    }

    pub fn apply(
        &self,
        current: &str,
        event: &str,
        journal: &mut Vec<Event>,
    ) -> Result<String, RuntimeError> {
        if !self.states.contains(current) {
            return Err(RuntimeError::new(
                ErrorKind::Transition,
                format!("unknown current state `{current}`"),
            ));
        }
        let next = self
            .transitions
            .get(&(current.to_owned(), event.to_owned()))
            .ok_or_else(|| {
                RuntimeError::new(
                    ErrorKind::Transition,
                    format!("event `{event}` is invalid from state `{current}`"),
                )
            })?;
        journal.push(Event::StateChanged {
            from: current.into(),
            event: event.into(),
            to: next.clone(),
        });
        Ok(next.clone())
    }
}

#[derive(Clone, Debug)]
pub struct ExecutionResult {
    pub identity: Identity,
    pub outputs: HashMap<ArcId, ArcValue>,
    pub journal: Vec<Event>,
}

/// Small convenience helper for graphs assembled in code.
pub fn edge(from: &str, to: &str, arc_id: &str) -> Edge {
    Edge {
        from: from.into(),
        to: to.into(),
        arc_id: arc_id.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::num::NonZeroUsize;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Barrier;
    use std::time::Duration;

    struct CopyOp;
    impl Operator for CopyOp {
        fn name(&self) -> &'static str {
            "copy"
        }
        fn version(&self) -> &'static str {
            "1"
        }
        fn execute(&self, input: Value, _: &OperatorContext) -> Result<Value, String> {
            Ok(input)
        }
    }

    struct CopyV2;
    impl Operator for CopyV2 {
        fn name(&self) -> &'static str {
            "copy"
        }
        fn version(&self) -> &'static str {
            "2"
        }
        fn execute(&self, input: Value, _: &OperatorContext) -> Result<Value, String> {
            Ok(input)
        }
    }

    struct EventOp {
        declaration: &'static [&'static str],
        event_name: &'static str,
    }
    impl Operator for EventOp {
        fn name(&self) -> &'static str {
            "event"
        }
        fn version(&self) -> &'static str {
            "1"
        }
        fn declared_events(&self) -> &'static [&'static str] {
            self.declaration
        }
        fn emitted_events(&self, _: &Value) -> Vec<OperatorEvent> {
            vec![OperatorEvent {
                name: self.event_name.into(),
            }]
        }
        fn execute(&self, input: Value, _: &OperatorContext) -> Result<Value, String> {
            Ok(input)
        }
    }

    fn identity(graph: &Graph) -> Identity {
        Identity {
            project_id: "demo".into(),
            graph_id: graph.id.clone(),
            graph_version: graph.version.clone(),
            execution_id: "e1".into(),
            attempt_id: "a1".into(),
        }
    }
    fn simple_graph() -> Graph {
        Graph {
            id: "g".into(),
            version: "1".into(),
            inputs: vec![ArcContract {
                id: "source".into(),
                schema: ValueType::Object,
            }],
            nodes: vec![Node {
                id: "copy-node".into(),
                operator: "copy".into(),
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
    fn registry() -> Registry {
        let mut r = Registry::default();
        r.register(CopyOp).unwrap();
        r
    }

    #[test]
    fn graph_binds_exact_operator_version() {
        let mut operators = Registry::default();
        operators.register(CopyOp).unwrap();
        operators.register(CopyV2).unwrap();
        let mut graph = simple_graph();
        graph.nodes[0].operator_version = "2".into();
        compile(graph.clone(), &operators, &BTreeSet::new()).unwrap();

        graph.nodes[0].operator_version = "3".into();
        let error = compile(graph, &operators, &BTreeSet::new()).unwrap_err();
        assert!(error.message.contains("missing operator `copy@3`"));

        assert!(operators
            .register(CopyOp)
            .unwrap_err()
            .message
            .contains("copy@1"));
    }

    #[test]
    fn operator_cannot_emit_undeclared_event_or_write_its_arc() {
        let mut operators = Registry::default();
        operators
            .register(EventOp {
                declaration: &["approved"],
                event_name: "secret-payload-value",
            })
            .unwrap();
        let mut graph = simple_graph();
        graph.nodes[0].operator = "event".into();
        let compiled = compile(graph.clone(), &operators, &BTreeSet::new()).unwrap();
        let error = Runtime::new(BTreeSet::new())
            .run(
                &compiled,
                identity(&graph),
                HashMap::from([("source".into(), json!({"value":1}))]),
                &Cancellation::default(),
            )
            .unwrap_err();

        assert_eq!(error.error.kind, ErrorKind::Operator);
        assert!(!error.journal.iter().any(|event| matches!(
            event,
            Event::OperatorEventEmitted { .. } | Event::ArcWritten { .. }
        )));
    }

    #[test]
    fn compiler_rejects_invalid_or_duplicate_operator_event_declarations() {
        for declaration in [&["Bad" as &str][..], &["same", "same"][..]] {
            let mut operators = Registry::default();
            operators
                .register(EventOp {
                    declaration,
                    event_name: "same",
                })
                .unwrap();
            let mut graph = simple_graph();
            graph.nodes[0].operator = "event".into();
            let error = compile(graph, &operators, &BTreeSet::new()).unwrap_err();
            assert!(error
                .message
                .contains("invalid or duplicate operator event"));
        }
    }

    struct BarrierCopyOp {
        barrier: Arc<Barrier>,
        active: Arc<AtomicUsize>,
        max_active: Arc<AtomicUsize>,
    }
    impl Operator for BarrierCopyOp {
        fn name(&self) -> &'static str {
            "barrier_copy"
        }
        fn version(&self) -> &'static str {
            "1"
        }
        fn execute(&self, input: Value, _: &OperatorContext) -> Result<Value, String> {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_active.fetch_max(active, Ordering::SeqCst);
            self.barrier.wait();
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(input)
        }
    }

    struct BarrierFailOp {
        barrier: Arc<Barrier>,
        calls: Arc<AtomicUsize>,
    }

    struct CountedDelayOp {
        active: Arc<AtomicUsize>,
        max_active: Arc<AtomicUsize>,
    }

    struct CancelWaveOp {
        barrier: Arc<Barrier>,
        cancellation: Arc<Cancellation>,
    }
    impl Operator for CancelWaveOp {
        fn name(&self) -> &'static str {
            "cancel_wave"
        }
        fn version(&self) -> &'static str {
            "1"
        }
        fn execute(&self, input: Value, _: &OperatorContext) -> Result<Value, String> {
            self.barrier.wait();
            self.cancellation.cancel();
            Ok(input)
        }
    }

    #[derive(Clone, Copy)]
    enum HookFailurePoint {
        Before,
        After,
        OnError,
    }
    struct FailingHook(HookFailurePoint);
    impl Hook for FailingHook {
        fn before_node(&self, _: &str) -> Result<(), String> {
            if matches!(self.0, HookFailurePoint::Before) {
                Err("before failed".into())
            } else {
                Ok(())
            }
        }
        fn after_node(&self, _: &str) -> Result<(), String> {
            if matches!(self.0, HookFailurePoint::After) {
                Err("after failed".into())
            } else {
                Ok(())
            }
        }
        fn on_error(&self, _: &str, _: &RuntimeError) -> Result<(), String> {
            if matches!(self.0, HookFailurePoint::OnError) {
                Err("on_error failed".into())
            } else {
                Ok(())
            }
        }
    }
    impl Operator for CountedDelayOp {
        fn name(&self) -> &'static str {
            "counted_delay"
        }
        fn version(&self) -> &'static str {
            "1"
        }
        fn execute(&self, input: Value, _: &OperatorContext) -> Result<Value, String> {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_active.fetch_max(active, Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(40));
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(input)
        }
    }
    impl Operator for BarrierFailOp {
        fn name(&self) -> &'static str {
            "barrier_fail"
        }
        fn version(&self) -> &'static str {
            "1"
        }
        fn execute(&self, _: Value, context: &OperatorContext) -> Result<Value, String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.barrier.wait();
            Err(format!("{} failure", context.node_id))
        }
    }

    fn parallel_graph(first_operator: &str, second_operator: &str) -> Graph {
        Graph {
            id: "parallel".into(),
            version: "1".into(),
            inputs: vec![ArcContract {
                id: "source".into(),
                schema: ValueType::Object,
            }],
            nodes: vec![
                Node {
                    id: "z-sibling".into(),
                    operator: second_operator.into(),
                    operator_version: "1".into(),
                    inputs: vec!["source".into()],
                    output: ArcContract {
                        id: "z-output".into(),
                        schema: ValueType::Object,
                    },
                    input_selector: Selector::default(),
                    output_selector: Selector::default(),
                    iterator: IteratorKind::Whole,
                },
                Node {
                    id: "a-sibling".into(),
                    operator: first_operator.into(),
                    operator_version: "1".into(),
                    inputs: vec!["source".into()],
                    output: ArcContract {
                        id: "a-output".into(),
                        schema: ValueType::Object,
                    },
                    input_selector: Selector::default(),
                    output_selector: Selector::default(),
                    iterator: IteratorKind::Whole,
                },
                Node {
                    id: "join".into(),
                    operator: "copy".into(),
                    operator_version: "1".into(),
                    inputs: vec!["a-output".into(), "z-output".into()],
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
                edge("a-sibling", "join", "a-output"),
                edge("z-sibling", "join", "z-output"),
            ],
            outputs: vec!["result".into()],
        }
    }

    #[test]
    fn runtime_respects_max_parallelism_for_larger_ready_waves() {
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let mut operators = Registry::default();
        operators
            .register(CountedDelayOp {
                active,
                max_active: max_active.clone(),
            })
            .unwrap();
        let nodes: Vec<_> = (0..3)
            .map(|index| Node {
                id: format!("node-{index}"),
                operator: "counted_delay".into(),
                operator_version: "1".into(),
                inputs: vec!["source".into()],
                output: ArcContract {
                    id: format!("output-{index}"),
                    schema: ValueType::Object,
                },
                input_selector: Selector::default(),
                output_selector: Selector::default(),
                iterator: IteratorKind::Whole,
            })
            .collect();
        let mut nodes = nodes;
        nodes.push(Node {
            id: "join".into(),
            operator: "copy".into(),
            operator_version: "1".into(),
            inputs: vec!["output-0".into(), "output-1".into(), "output-2".into()],
            output: ArcContract {
                id: "result".into(),
                schema: ValueType::Array,
            },
            input_selector: Selector::default(),
            output_selector: Selector::default(),
            iterator: IteratorKind::Whole,
        });
        let graph = Graph {
            id: "bounded-parallelism".into(),
            version: "1".into(),
            inputs: vec![ArcContract {
                id: "source".into(),
                schema: ValueType::Object,
            }],
            nodes,
            edges: vec![
                edge("node-0", "join", "output-0"),
                edge("node-1", "join", "output-1"),
                edge("node-2", "join", "output-2"),
            ],
            outputs: vec!["result".into()],
        };
        operators.register(CopyOp).unwrap();
        let compiled = compile(graph.clone(), &operators, &BTreeSet::new()).unwrap();
        Runtime::new(BTreeSet::new())
            .with_max_parallelism(NonZeroUsize::new(2).unwrap())
            .run(
                &compiled,
                identity(&graph),
                HashMap::from([("source".into(), json!({"value":1}))]),
                &Cancellation::default(),
            )
            .unwrap();

        assert_eq!(max_active.load(Ordering::SeqCst), 2);
        max_active.store(0, Ordering::SeqCst);
        Runtime::new(BTreeSet::new())
            .run(
                &compiled,
                identity(&graph),
                HashMap::from([("source".into(), json!({"value":1}))]),
                &Cancellation::default(),
            )
            .unwrap();
        assert_eq!(max_active.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn hook_failures_are_classified_and_journaled() {
        let graph = simple_graph();
        let compiled = compile(graph.clone(), &registry(), &BTreeSet::new()).unwrap();
        let mut before_runtime = Runtime::new(BTreeSet::new());
        before_runtime.add_hook(FailingHook(HookFailurePoint::Before));
        let before_failure = before_runtime
            .run(
                &compiled,
                identity(&graph),
                HashMap::from([("source".into(), json!({"value":1}))]),
                &Cancellation::default(),
            )
            .unwrap_err();
        assert_eq!(before_failure.error.kind, ErrorKind::Hook);
        assert!(!before_failure
            .journal
            .iter()
            .any(|event| matches!(event, Event::OperatorStarted { .. })));

        let mut after_runtime = Runtime::new(BTreeSet::new());
        after_runtime.add_hook(FailingHook(HookFailurePoint::After));
        let after_failure = after_runtime
            .run(
                &compiled,
                identity(&graph),
                HashMap::from([("source".into(), json!({"value":1}))]),
                &Cancellation::default(),
            )
            .unwrap_err();
        assert_eq!(after_failure.error.kind, ErrorKind::Hook);
        assert!(after_failure
            .journal
            .iter()
            .any(|event| matches!(event, Event::ArcWritten { .. })));
        assert!(!after_failure
            .journal
            .iter()
            .any(|event| matches!(event, Event::NodeCompleted { .. })));

        let mut fail_registry = Registry::default();
        fail_registry.register(FailOp).unwrap();
        let mut fail_graph = simple_graph();
        fail_graph.nodes[0].operator = "fail".into();
        let fail_compiled = compile(fail_graph.clone(), &fail_registry, &BTreeSet::new()).unwrap();
        let mut error_runtime = Runtime::new(BTreeSet::new());
        error_runtime.add_hook(FailingHook(HookFailurePoint::OnError));
        let error_failure = error_runtime
            .run(
                &fail_compiled,
                identity(&fail_graph),
                HashMap::from([("source".into(), json!({"value":1}))]),
                &Cancellation::default(),
            )
            .unwrap_err();
        assert_eq!(error_failure.error.kind, ErrorKind::Hook);
        assert!(matches!(
            error_failure.journal.last(),
            Some(Event::ExecutionFailed {
                error_kind: ErrorKind::Hook
            })
        ));
    }

    struct PanickingHook;
    impl Hook for PanickingHook {
        fn before_node(&self, _: &str) -> Result<(), String> {
            panic!("controlled hook panic");
        }
    }

    struct PanickingOperator;
    impl Operator for PanickingOperator {
        fn name(&self) -> &'static str {
            "panicking"
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
        fn execute(&self, _: Value, _: &OperatorContext) -> Result<Value, String> {
            panic!("controlled operator panic");
        }
    }

    #[test]
    fn operator_panics_are_classified_and_journaled_in_all_parallel_modes() {
        let mut graph = simple_graph();
        graph.nodes[0].operator = "panicking".into();
        let mut registry = Registry::default();
        registry.register(PanickingOperator).unwrap();
        let compiled = compile(graph.clone(), &registry, &BTreeSet::new()).unwrap();

        for parallelism in [1, 2] {
            let runtime = Runtime::new(BTreeSet::new())
                .with_max_parallelism(NonZeroUsize::new(parallelism).unwrap());
            let failure = runtime
                .run(
                    &compiled,
                    identity(&graph),
                    HashMap::from([("source".into(), json!({"value": 1}))]),
                    &Cancellation::default(),
                )
                .unwrap_err();
            assert_eq!(failure.error.kind, ErrorKind::Operator);
            assert!(failure.journal.iter().any(|event| matches!(
                event,
                Event::OperatorStarted {
                    node_id,
                    ..
                } if node_id == "copy-node"
            )));
            assert!(matches!(
                failure.journal.last(),
                Some(Event::ExecutionFailed {
                    error_kind: ErrorKind::Operator
                })
            ));
        }
    }

    #[test]
    fn panicking_hook_returns_a_classified_execution_failure() {
        let graph = simple_graph();
        let compiled = compile(graph.clone(), &registry(), &BTreeSet::new()).unwrap();
        let mut runtime = Runtime::new(BTreeSet::new());
        runtime.add_hook(PanickingHook);
        let failure = runtime
            .run(
                &compiled,
                identity(&graph),
                HashMap::from([("source".into(), json!({"value": 1}))]),
                &Cancellation::default(),
            )
            .unwrap_err();
        assert_eq!(failure.error.kind, ErrorKind::Hook);
        assert!(matches!(
            failure.journal.last(),
            Some(Event::ExecutionFailed {
                error_kind: ErrorKind::Hook
            })
        ));
    }

    #[test]
    fn cancellation_is_checked_before_execution_and_between_parallel_waves() {
        let graph = simple_graph();
        let compiled = compile(graph.clone(), &registry(), &BTreeSet::new()).unwrap();
        let pre_cancelled = Cancellation::default();
        pre_cancelled.cancel();
        let failure = Runtime::new(BTreeSet::new())
            .run(
                &compiled,
                identity(&graph),
                HashMap::from([("source".into(), json!({"value":1}))]),
                &pre_cancelled,
            )
            .unwrap_err();
        assert_eq!(failure.error.kind, ErrorKind::Cancelled);
        assert!(!failure
            .journal
            .iter()
            .any(|event| matches!(event, Event::NodeScheduled { .. })));

        let barrier = Arc::new(Barrier::new(2));
        let cancellation = Arc::new(Cancellation::default());
        let mut operators = Registry::default();
        operators
            .register(CancelWaveOp {
                barrier,
                cancellation: cancellation.clone(),
            })
            .unwrap();
        operators.register(CopyOp).unwrap();
        let wave_graph = parallel_graph("cancel_wave", "cancel_wave");
        let wave_compiled = compile(wave_graph.clone(), &operators, &BTreeSet::new()).unwrap();
        let wave_failure = Runtime::new(BTreeSet::new())
            .with_max_parallelism(NonZeroUsize::new(2).unwrap())
            .run(
                &wave_compiled,
                identity(&wave_graph),
                HashMap::from([("source".into(), json!({"value":1}))]),
                &cancellation,
            )
            .unwrap_err();
        assert_eq!(wave_failure.error.kind, ErrorKind::Cancelled);
        assert!(wave_failure.journal.iter().any(|event| matches!(
            event,
            Event::NodeCompleted { node_id, .. } if node_id == "a-sibling"
        )));
        assert!(wave_failure.journal.iter().any(|event| matches!(
            event,
            Event::NodeCompleted { node_id, .. } if node_id == "z-sibling"
        )));
        assert!(!wave_failure.journal.iter().any(|event| matches!(
            event,
            Event::NodeStarted { node_id, .. } if node_id == "join"
        )));

        let final_cancellation = Arc::new(Cancellation::default());
        let mut final_operators = Registry::default();
        final_operators
            .register(CancelWaveOp {
                barrier: Arc::new(Barrier::new(1)),
                cancellation: final_cancellation.clone(),
            })
            .unwrap();
        let mut final_graph = simple_graph();
        final_graph.nodes[0].operator = "cancel_wave".into();
        let final_compiled =
            compile(final_graph.clone(), &final_operators, &BTreeSet::new()).unwrap();
        let final_failure = Runtime::new(BTreeSet::new())
            .run(
                &final_compiled,
                identity(&final_graph),
                HashMap::from([("source".into(), json!({"value":1}))]),
                &final_cancellation,
            )
            .unwrap_err();
        assert_eq!(final_failure.error.kind, ErrorKind::Cancelled);
        assert!(matches!(
            final_failure.journal.last(),
            Some(Event::ExecutionFailed {
                error_kind: ErrorKind::Cancelled
            })
        ));
    }

    #[test]
    fn independent_nodes_overlap_and_journal_order_is_stable() {
        let barrier = Arc::new(Barrier::new(2));
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let mut operators = Registry::default();
        operators
            .register(BarrierCopyOp {
                barrier,
                active,
                max_active: max_active.clone(),
            })
            .unwrap();
        operators.register(CopyOp).unwrap();
        let graph = parallel_graph("barrier_copy", "barrier_copy");
        let compiled = compile(graph.clone(), &operators, &BTreeSet::new()).unwrap();
        let runtime =
            Runtime::new(BTreeSet::new()).with_max_parallelism(NonZeroUsize::new(2).unwrap());
        let result = runtime
            .run(
                &compiled,
                identity(&graph),
                HashMap::from([("source".into(), json!({"value":1}))]),
                &Cancellation::default(),
            )
            .unwrap();

        assert_eq!(max_active.load(Ordering::SeqCst), 2);
        assert_eq!(
            result.outputs["result"].payload,
            json!([{"value":1},{"value":1}])
        );
        let scheduled: Vec<_> = result
            .journal
            .iter()
            .filter_map(|event| match event {
                Event::NodeScheduled { node_id } => Some(node_id.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(scheduled, ["a-sibling", "z-sibling", "join"]);
        let started: Vec<_> = result
            .journal
            .iter()
            .filter_map(|event| match event {
                Event::NodeStarted { node_id, .. } => Some(node_id.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(started, ["a-sibling", "z-sibling", "join"]);
    }

    #[test]
    fn failed_wave_drains_siblings_and_does_not_start_dependents() {
        let barrier = Arc::new(Barrier::new(2));
        let calls = Arc::new(AtomicUsize::new(0));
        let mut operators = Registry::default();
        operators
            .register(BarrierFailOp {
                barrier: barrier.clone(),
                calls: calls.clone(),
            })
            .unwrap();
        operators
            .register(BarrierCopyOp {
                barrier,
                active: Arc::new(AtomicUsize::new(0)),
                max_active: Arc::new(AtomicUsize::new(0)),
            })
            .unwrap();
        operators.register(CopyOp).unwrap();
        let graph = parallel_graph("barrier_fail", "barrier_copy");
        let compiled = compile(graph.clone(), &operators, &BTreeSet::new()).unwrap();
        let failure = Runtime::new(BTreeSet::new())
            .with_max_parallelism(NonZeroUsize::new(2).unwrap())
            .run(
                &compiled,
                identity(&graph),
                HashMap::from([("source".into(), json!({"value":1}))]),
                &Cancellation::default(),
            )
            .unwrap_err();

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(failure.error.kind, ErrorKind::Operator);
        assert!(failure.journal.iter().any(|event| matches!(
            event,
            Event::NodeCompleted { node_id, .. } if node_id == "z-sibling"
        )));
        assert!(!failure.journal.iter().any(|event| matches!(
            event,
            Event::NodeStarted { node_id, .. } if node_id == "join"
        )));
        let failed: Vec<_> = failure
            .journal
            .iter()
            .filter_map(|event| match event {
                Event::NodeFailed { node_id, .. } => Some(node_id.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(failed, ["a-sibling"]);
    }

    #[test]
    fn concurrent_failure_selection_and_journal_are_node_id_ordered() {
        let barrier = Arc::new(Barrier::new(2));
        let calls = Arc::new(AtomicUsize::new(0));
        let mut operators = Registry::default();
        operators
            .register(BarrierFailOp {
                barrier,
                calls: calls.clone(),
            })
            .unwrap();
        operators.register(CopyOp).unwrap();
        let graph = parallel_graph("barrier_fail", "barrier_fail");
        let compiled = compile(graph.clone(), &operators, &BTreeSet::new()).unwrap();
        let failure = Runtime::new(BTreeSet::new())
            .with_max_parallelism(NonZeroUsize::new(2).unwrap())
            .run(
                &compiled,
                identity(&graph),
                HashMap::from([("source".into(), json!({"value":1}))]),
                &Cancellation::default(),
            )
            .unwrap_err();

        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(failure.error.message, "a-sibling failure");
        let failed: Vec<_> = failure
            .journal
            .iter()
            .filter_map(|event| match event {
                Event::NodeFailed { node_id, .. } => Some(node_id.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(failed, ["a-sibling", "z-sibling"]);
        assert!(!failure.journal.iter().any(|event| matches!(
            event,
            Event::NodeStarted { node_id, .. } if node_id == "join"
        )));
    }

    #[test]
    fn compile_rejects_missing_operator_and_cycle() {
        let mut graph = simple_graph();
        graph.nodes[0].operator = "missing".into();
        assert!(compile(graph, &registry(), &BTreeSet::new())
            .unwrap_err()
            .message
            .contains("missing operator"));
        let mut graph = simple_graph();
        graph.nodes.push(Node {
            id: "second".into(),
            operator: "copy".into(),
            operator_version: "1".into(),
            inputs: vec!["result".into()],
            output: ArcContract {
                id: "result-2".into(),
                schema: ValueType::Object,
            },
            input_selector: Selector::default(),
            output_selector: Selector::default(),
            iterator: IteratorKind::Whole,
        });
        graph.nodes[0].inputs = vec!["result-2".into()];
        graph.edges = vec![
            edge("copy-node", "second", "result"),
            edge("second", "copy-node", "result-2"),
        ];
        graph.outputs = vec!["result-2".into()];
        assert!(compile(graph, &registry(), &BTreeSet::new())
            .unwrap_err()
            .message
            .contains("cycle"));
    }

    #[test]
    fn graph_topology_returns_waves_and_rejects_cycles_or_dead_nodes() {
        let graph = simple_graph();
        let plan = graph_topology(&graph).unwrap();
        assert_eq!(plan.order, ["copy-node"]);
        assert_eq!(plan.waves, [vec!["copy-node".to_owned()]]);

        let mut cyclic = graph.clone();
        cyclic.nodes.push(Node {
            id: "second".into(),
            operator: "copy".into(),
            operator_version: "1".into(),
            inputs: vec!["result".into()],
            output: ArcContract {
                id: "result-2".into(),
                schema: ValueType::Object,
            },
            input_selector: Selector::default(),
            output_selector: Selector::default(),
            iterator: IteratorKind::Whole,
        });
        cyclic.nodes[0].inputs = vec!["result-2".into()];
        cyclic.edges = vec![
            edge("copy-node", "second", "result"),
            edge("second", "copy-node", "result-2"),
        ];
        cyclic.outputs = vec!["result-2".into()];
        assert!(graph_topology(&cyclic)
            .unwrap_err()
            .message
            .contains("cycle"));

        let mut dead = graph;
        dead.nodes.push(Node {
            id: "dead".into(),
            operator: "not-resolved-by-topology".into(),
            operator_version: "1".into(),
            inputs: vec!["source".into()],
            output: ArcContract {
                id: "dead-output".into(),
                schema: ValueType::Any,
            },
            input_selector: Selector::default(),
            output_selector: Selector::default(),
            iterator: IteratorKind::Whole,
        });
        assert!(graph_topology(&dead)
            .unwrap_err()
            .message
            .contains("cannot reach a declared graph output"));
    }

    #[test]
    fn graph_topology_requires_a_sese_graph_per_object_source() {
        assert!(graph_topology(&simple_graph()).is_ok());

        let mut scalar_payload = simple_graph();
        scalar_payload.inputs[0].schema = ValueType::String;
        assert!(graph_topology(&scalar_payload).is_ok());

        let mut multiple_sources = simple_graph();
        multiple_sources.inputs.push(ArcContract {
            id: "object-b".into(),
            schema: ValueType::Object,
        });
        assert!(graph_topology(&multiple_sources)
            .unwrap_err()
            .message
            .contains("exactly one input ARC; validate each object flow as a separate Graph"));

        let mut multiple_sinks = simple_graph();
        let mut extra_sink = multiple_sinks.nodes[0].clone();
        extra_sink.id = "extra-sink".into();
        extra_sink.inputs = vec!["result".into()];
        extra_sink.output.id = "extra-result".into();
        multiple_sinks.nodes.push(extra_sink);
        multiple_sinks
            .edges
            .push(edge("copy-node", "extra-sink", "result"));
        multiple_sinks.outputs.push("extra-result".into());
        assert!(graph_topology(&multiple_sinks)
            .unwrap_err()
            .message
            .contains("exactly one output ARC; validate each object flow as a separate Graph"));
    }

    #[test]
    fn json_parse_rejects_invalid_graph_shape() {
        assert!(parse_graph_json("{\"nodes\": [}")
            .unwrap_err()
            .message
            .contains("invalid graph JSON"));
        let graph = simple_graph();
        let encoded = serde_json::to_string(&graph).unwrap();
        assert_eq!(parse_graph_json(&encoded).unwrap(), graph);
    }

    struct ObjectOnly;
    impl Operator for ObjectOnly {
        fn name(&self) -> &'static str {
            "object_only"
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
        fn execute(&self, input: Value, _: &OperatorContext) -> Result<Value, String> {
            Ok(input)
        }
    }

    struct WrongStringOutput;
    impl Operator for WrongStringOutput {
        fn name(&self) -> &'static str {
            "wrong_string_output"
        }
        fn version(&self) -> &'static str {
            "1"
        }
        fn input_type(&self) -> ValueType {
            ValueType::Object
        }
        fn output_type(&self) -> ValueType {
            ValueType::String
        }
        fn execute(&self, _: Value, _: &OperatorContext) -> Result<Value, String> {
            Ok(json!({"not": "a string"}))
        }
    }

    #[test]
    fn runtime_enforces_operator_contracts_when_arc_schema_is_any() {
        let mut graph = simple_graph();
        graph.inputs[0].schema = ValueType::Any;
        graph.nodes[0].operator = "object_only".into();
        let mut object_registry = Registry::default();
        object_registry.register(ObjectOnly).unwrap();
        let compiled = compile(graph.clone(), &object_registry, &BTreeSet::new()).unwrap();
        let failure = Runtime::new(BTreeSet::new())
            .run(
                &compiled,
                identity(&graph),
                HashMap::from([("source".into(), json!(42))]),
                &Cancellation::default(),
            )
            .unwrap_err();
        assert_eq!(failure.error.kind, ErrorKind::Input);

        let mut graph = simple_graph();
        graph.nodes[0].operator = "wrong_string_output".into();
        graph.nodes[0].output.schema = ValueType::Any;
        let mut registry = Registry::default();
        registry.register(WrongStringOutput).unwrap();
        let compiled = compile(graph.clone(), &registry, &BTreeSet::new()).unwrap();
        let failure = Runtime::new(BTreeSet::new())
            .run(
                &compiled,
                identity(&graph),
                HashMap::from([("source".into(), json!({"valid": "input"}))]),
                &Cancellation::default(),
            )
            .unwrap_err();
        assert_eq!(failure.error.kind, ErrorKind::Output);
    }

    #[test]
    fn items_iterator_enforces_each_operator_output_contract() {
        let mut graph = simple_graph();
        graph.inputs[0].schema = ValueType::ArrayOf(Box::new(ValueType::Object));
        graph.nodes[0].operator = "wrong_string_output".into();
        graph.nodes[0].iterator = IteratorKind::Items;
        graph.nodes[0].output.schema = ValueType::Array;
        let mut registry = Registry::default();
        registry.register(WrongStringOutput).unwrap();
        let compiled = compile(graph.clone(), &registry, &BTreeSet::new()).unwrap();
        let failure = Runtime::new(BTreeSet::new())
            .run(
                &compiled,
                identity(&graph),
                HashMap::from([("source".into(), json!([{"valid": "input"}]))]),
                &Cancellation::default(),
            )
            .unwrap_err();
        assert_eq!(failure.error.kind, ErrorKind::Output);
    }

    struct NumberOutput;
    impl Operator for NumberOutput {
        fn name(&self) -> &'static str {
            "number_output"
        }
        fn version(&self) -> &'static str {
            "1"
        }
        fn input_type(&self) -> ValueType {
            ValueType::Object
        }
        fn output_type(&self) -> ValueType {
            ValueType::Number
        }
        fn execute(&self, _: Value, _: &OperatorContext) -> Result<Value, String> {
            Ok(json!(5))
        }
    }

    #[test]
    fn output_selector_failures_are_classified_as_output_errors() {
        let mut graph = simple_graph();
        graph.nodes[0].operator = "number_output".into();
        graph.nodes[0].output.schema = ValueType::Number;
        graph.nodes[0]
            .output_selector
            .include
            .insert("value".into());
        let mut registry = Registry::default();
        registry.register(NumberOutput).unwrap();
        let compiled = compile(graph.clone(), &registry, &BTreeSet::new()).unwrap();
        let failure = Runtime::new(BTreeSet::new())
            .run(
                &compiled,
                identity(&graph),
                HashMap::from([("source".into(), json!({"input": true}))]),
                &Cancellation::default(),
            )
            .unwrap_err();
        assert_eq!(failure.error.kind, ErrorKind::Output);
    }

    #[test]
    fn output_selector_result_is_the_value_written_to_the_declared_arc() {
        let mut graph = simple_graph();
        graph.nodes[0]
            .output_selector
            .include
            .insert("value".into());
        let compiled = compile(graph.clone(), &registry(), &BTreeSet::new()).unwrap();
        let result = Runtime::new(BTreeSet::new())
            .run(
                &compiled,
                identity(&graph),
                HashMap::from([("source".into(), json!({"value":1,"ignored":true}))]),
                &Cancellation::default(),
            )
            .unwrap();

        let output = result.outputs.get("result").unwrap();
        assert_eq!(output.payload, json!({"value":1}));
        assert!(output.schema.accepts(&output.payload));
    }

    #[test]
    fn compiler_rejects_invalid_edge_incompatible_schema_and_dead_node() {
        let mut graph = simple_graph();
        graph.outputs.push("result".into());
        assert!(compile(graph, &registry(), &BTreeSet::new())
            .unwrap_err()
            .message
            .contains("duplicate output ARC"));

        let mut graph = simple_graph();
        graph.edges = vec![edge("missing", "copy-node", "source")];
        assert!(compile(graph, &registry(), &BTreeSet::new())
            .unwrap_err()
            .message
            .contains("missing source node"));

        let mut graph = simple_graph();
        graph.inputs[0].schema = ValueType::String;
        graph.nodes[0].operator = "object_only".into();
        let mut object_registry = Registry::default();
        object_registry.register(ObjectOnly).unwrap();
        assert!(compile(graph, &object_registry, &BTreeSet::new())
            .unwrap_err()
            .message
            .contains("incompatible with operator"));

        let mut graph = simple_graph();
        graph.nodes.push(Node {
            id: "dead".into(),
            operator: "copy".into(),
            operator_version: "1".into(),
            inputs: vec!["source".into()],
            output: ArcContract {
                id: "dead-output".into(),
                schema: ValueType::Object,
            },
            input_selector: Selector::default(),
            output_selector: Selector::default(),
            iterator: IteratorKind::Whole,
        });
        assert!(compile(graph, &registry(), &BTreeSet::new())
            .unwrap_err()
            .message
            .contains("cannot reach a declared graph output"));
    }

    struct ArrayOnly;
    impl Operator for ArrayOnly {
        fn name(&self) -> &'static str {
            "array_only"
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

    #[test]
    fn whole_node_multi_arc_contract_matches_runtime_array_bundle() {
        let mut graph = simple_graph();
        let mut left = graph.nodes[0].clone();
        left.id = "left".into();
        left.output.id = "left-output".into();
        let mut right = left.clone();
        right.id = "right".into();
        right.output.id = "right-output".into();
        let mut join = left.clone();
        join.id = "join".into();
        join.operator = "object_only".into();
        join.inputs = vec!["left-output".into(), "right-output".into()];
        join.output.id = "result".into();
        join.output.schema = ValueType::Object;
        graph.nodes = vec![left, right, join];
        graph.edges = vec![
            edge("left", "join", "left-output"),
            edge("right", "join", "right-output"),
        ];
        let mut object_registry = Registry::default();
        object_registry.register(CopyOp).unwrap();
        object_registry.register(ObjectOnly).unwrap();
        let error = compile(graph.clone(), &object_registry, &BTreeSet::new()).unwrap_err();
        assert!(
            error.message.contains("input binding is incompatible"),
            "{error}"
        );

        graph.nodes[2].operator = "array_only".into();
        graph.nodes[2].output.schema = ValueType::Array;
        let mut array_registry = Registry::default();
        array_registry.register(CopyOp).unwrap();
        array_registry.register(ArrayOnly).unwrap();
        let compiled = compile(graph.clone(), &array_registry, &BTreeSet::new()).unwrap();
        let result = Runtime::new(BTreeSet::new())
            .run(
                &compiled,
                identity(&graph),
                HashMap::from([("source".into(), json!({"v":1}))]),
                &Cancellation::default(),
            )
            .unwrap();
        assert_eq!(result.outputs["result"].payload, json!([{"v":1},{"v":1}]));
    }

    #[test]
    fn runtime_executes_compiled_graph_and_records_arc_refs() {
        let graph = simple_graph();
        let compiled = compile(graph.clone(), &registry(), &BTreeSet::new()).unwrap();
        let runtime = Runtime::new(BTreeSet::new());
        let result = runtime
            .run(
                &compiled,
                identity(&graph),
                HashMap::from([("source".into(), json!({"ok":true}))]),
                &Cancellation::default(),
            )
            .unwrap();
        assert_eq!(result.outputs["result"].payload, json!({"ok":true}));
        assert!(result.journal.iter().any(
            |event| matches!(event, Event::ArcRead { arc_ref, .. } if arc_ref.contains("source@1"))
        ));
        assert!(matches!(
            result.journal.last(),
            Some(Event::ExecutionCompleted)
        ));
    }

    #[test]
    fn items_iterator_selects_each_record_and_enforces_nested_schema() {
        let mut graph = simple_graph();
        graph.inputs[0].schema = ValueType::ArrayOf(Box::new(ValueType::Object));
        graph.nodes[0].iterator = IteratorKind::Items;
        graph.nodes[0].input_selector = Selector {
            include: BTreeSet::from(["keep".into()]),
            exclude: BTreeSet::new(),
            predicate: Some(FieldPredicate::Equals {
                field: "keep".into(),
                value: json!(1),
            }),
        };
        graph.nodes[0].output.schema = ValueType::ArrayOf(Box::new(ValueType::Object));
        let compiled = compile(graph.clone(), &registry(), &BTreeSet::new()).unwrap();
        let result = Runtime::new(BTreeSet::new())
            .run(
                &compiled,
                identity(&graph),
                HashMap::from([(
                    "source".into(),
                    json!([{"keep":1,"drop":2},{"keep":3,"drop":4}]),
                )]),
                &Cancellation::default(),
            )
            .unwrap();
        assert_eq!(result.outputs["result"].payload, json!([{"keep":1}]));
    }

    #[test]
    fn items_journal_records_only_actual_operator_calls() {
        let mut graph = simple_graph();
        graph.inputs[0].schema = ValueType::ArrayOf(Box::new(ValueType::Object));
        graph.nodes[0].iterator = IteratorKind::Items;
        graph.nodes[0].input_selector.predicate = Some(FieldPredicate::Equals {
            field: "keep".into(),
            value: json!(true),
        });
        graph.nodes[0].output.schema = ValueType::ArrayOf(Box::new(ValueType::Object));
        let compiled = compile(graph.clone(), &registry(), &BTreeSet::new()).unwrap();
        let runtime = Runtime::new(BTreeSet::new());

        for input in [json!([]), json!([{"keep": false}])] {
            let result = runtime
                .run(
                    &compiled,
                    identity(&graph),
                    HashMap::from([("source".into(), input)]),
                    &Cancellation::default(),
                )
                .unwrap();
            assert_eq!(result.outputs["result"].payload, json!([]));
            assert!(!result.journal.iter().any(|event| matches!(
                event,
                Event::OperatorStarted { .. } | Event::OperatorCompleted { .. }
            )));
        }

        let result = runtime
            .run(
                &compiled,
                identity(&graph),
                HashMap::from([(
                    "source".into(),
                    json!([{"keep": false}, {"keep": true}, {"keep": true}]),
                )]),
                &Cancellation::default(),
            )
            .unwrap();
        let started: Vec<_> = result
            .journal
            .iter()
            .filter_map(|event| match event {
                Event::OperatorStarted {
                    invocation_index, ..
                } => Some(*invocation_index),
                _ => None,
            })
            .collect();
        let completed: Vec<_> = result
            .journal
            .iter()
            .filter_map(|event| match event {
                Event::OperatorCompleted {
                    invocation_index, ..
                } => Some(*invocation_index),
                _ => None,
            })
            .collect();
        assert_eq!(started, [Some(1), Some(2)]);
        assert_eq!(completed, [Some(1), Some(2)]);
    }

    struct NeedsNetwork;
    impl Operator for NeedsNetwork {
        fn name(&self) -> &'static str {
            "needs_network"
        }
        fn version(&self) -> &'static str {
            "1"
        }
        fn effects(&self) -> &'static [&'static str] {
            &["network.http"]
        }
        fn execute(&self, input: Value, _: &OperatorContext) -> Result<Value, String> {
            Ok(input)
        }
    }

    #[test]
    fn effectful_operator_defaults_to_confirmation_for_replay() {
        let mut graph = simple_graph();
        graph.nodes[0].operator = "needs_network".into();
        let mut registry = Registry::default();
        registry.register(NeedsNetwork).unwrap();
        let capabilities = BTreeSet::from(["network.http".to_owned()]);
        let compiled = compile(graph.clone(), &registry, &capabilities).unwrap();
        let result = Runtime::new(capabilities)
            .run(
                &compiled,
                identity(&graph),
                HashMap::from([("source".into(), json!({"request": true}))]),
                &Cancellation::default(),
            )
            .unwrap();
        assert!(result.journal.iter().any(|event| matches!(
            event,
            Event::OperatorStarted {
                replay: EffectReplay::RequiresConfirmation,
                ..
            }
        )));
    }

    #[test]
    fn compiler_rejects_missing_arc_and_ungranted_effect() {
        let mut graph = simple_graph();
        graph.nodes[0].inputs = vec!["unknown".into()];
        assert!(compile(graph, &registry(), &BTreeSet::new())
            .unwrap_err()
            .message
            .contains("undeclared ARC"));

        let mut registry = Registry::default();
        registry.register(NeedsNetwork).unwrap();
        let graph = Graph {
            id: "effect".into(),
            version: "1".into(),
            inputs: vec![ArcContract {
                id: "source".into(),
                schema: ValueType::Object,
            }],
            nodes: vec![Node {
                id: "call".into(),
                operator: "needs_network".into(),
                operator_version: "1".into(),
                inputs: vec!["source".into()],
                output: ArcContract {
                    id: "result".into(),
                    schema: ValueType::Any,
                },
                input_selector: Selector::default(),
                output_selector: Selector::default(),
                iterator: IteratorKind::Whole,
            }],
            edges: vec![],
            outputs: vec!["result".into()],
        };
        assert!(compile(graph, &registry, &BTreeSet::new())
            .unwrap_err()
            .message
            .contains("network.http"));
    }

    struct FailOp;
    impl Operator for FailOp {
        fn name(&self) -> &'static str {
            "fail"
        }
        fn version(&self) -> &'static str {
            "1"
        }
        fn execute(&self, _: Value, _: &OperatorContext) -> Result<Value, String> {
            Err("expected failure".into())
        }
    }

    #[test]
    fn failed_execution_returns_journal_facts() {
        let mut graph = simple_graph();
        graph.nodes[0].operator = "fail".into();
        let mut registry = Registry::default();
        registry.register(FailOp).unwrap();
        let compiled = compile(graph.clone(), &registry, &BTreeSet::new()).unwrap();
        let failure = Runtime::new(BTreeSet::new())
            .run(
                &compiled,
                identity(&graph),
                HashMap::from([("source".into(), json!({"ok":true}))]),
                &Cancellation::default(),
            )
            .unwrap_err();
        assert_eq!(failure.error.kind, ErrorKind::Operator);
        assert!(failure
            .journal
            .iter()
            .any(|event| matches!(event, Event::OperatorStarted { .. })));
        assert!(failure
            .journal
            .iter()
            .any(|event| matches!(event, Event::NodeFailed { .. })));
        assert!(matches!(
            failure.journal.last(),
            Some(Event::ExecutionFailed {
                error_kind: ErrorKind::Operator
            })
        ));
    }

    #[test]
    fn state_machine_rejects_undeclared_transition_and_allows_retry_event() {
        let machine = StateMachine::new(
            ["ready".into(), "running".into(), "failed".into()],
            vec![
                Transition {
                    from: "ready".into(),
                    event: "start".into(),
                    to: "running".into(),
                },
                Transition {
                    from: "failed".into(),
                    event: "retry".into(),
                    to: "ready".into(),
                },
            ],
        )
        .unwrap();
        let mut journal = vec![];
        assert_eq!(
            machine.apply("ready", "start", &mut journal).unwrap(),
            "running"
        );
        assert!(
            machine
                .apply("running", "retry", &mut journal)
                .unwrap_err()
                .kind
                == ErrorKind::Transition
        );
        assert_eq!(
            machine.apply("failed", "retry", &mut journal).unwrap(),
            "ready"
        );
    }
}
