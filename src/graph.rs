use crate::node::Node;
use async_trait::async_trait;
use std::{
    any::{Any, TypeId},
    collections::{HashMap, HashSet},
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
};

/// Boxed node
pub trait NodeWrapper: Send + Sync {
    fn call_boxed<'a>(
        &'a self,
        inputs: Arc<HashMap<String, Option<Arc<dyn Any + Send + Sync>>>>,
    ) -> Pin<Box<dyn Future<Output = Arc<HashMap<String, Arc<dyn Any + Send + Sync>>>> + Send + 'a>>;
}

impl<N> NodeWrapper for N
where
    N: Node + 'static,
{
    fn call_boxed<'a>(
        &'a self,
        inputs: Arc<HashMap<String, Option<Arc<dyn Any + Send + Sync>>>>,
    ) -> Pin<Box<dyn Future<Output = Arc<HashMap<String, Arc<dyn Any + Send + Sync>>>> + Send + 'a>>
    {
        Box::pin(async move {
            let map = Arc::try_unwrap(inputs).unwrap_or_else(|arc| (*arc).clone());
            Arc::new(self.run(map).await)
        })
    }
}

/// Graph-unique node identifier
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct NodeId(pub usize);

/// Typed and named input port
#[derive(Clone, Debug)]
pub struct InputPort<T: 'static + Send + Sync> {
    node: NodeId,
    name: &'static str,
    _ty: PhantomData<T>,
}

/// Typed and named output port
#[derive(Clone, Debug)]
pub struct OutputPort<T: 'static + Send + Sync> {
    node: NodeId,
    name: &'static str,
    _ty: PhantomData<T>,
}

/// Builder for assembling a graph with static port check
pub struct GraphBuilder {
    next_id: usize,
    nodes: HashMap<NodeId, Arc<dyn NodeWrapper>>,
    inputs: Vec<(NodeId, &'static str, TypeId)>,
    outputs: Vec<(NodeId, &'static str, TypeId)>,
    edges: Vec<(TypeId, (NodeId, &'static str), (NodeId, &'static str))>,
    external_inputs: Vec<(&'static str, NodeId, &'static str, TypeId)>,
    external_outputs: Vec<(&'static str, NodeId, &'static str, TypeId)>,
}

impl GraphBuilder {
    pub fn new() -> Self {
        Self {
            next_id: 0,
            nodes: Default::default(),
            inputs: Vec::new(),
            outputs: Vec::new(),
            edges: Vec::new(),
            external_inputs: Vec::new(),
            external_outputs: Vec::new(),
        }
    }

    /// Add node to graph and return node builder
    pub fn add_node<N: Node + 'static>(&mut self, node: N) -> NodeBuilder<'_> {
        let id = NodeId(self.next_id);
        self.next_id += 1;
        self.nodes.insert(id, Arc::new(node));
        NodeBuilder { gb: self, node: id }
    }

    /// Connect ports of the same type
    pub fn connect<T: 'static + Send + Sync>(
        &mut self,
        out: OutputPort<T>,
        inp: InputPort<T>,
    ) -> &mut Self {
        assert!(
            self.outputs
                .contains(&(out.node, out.name, TypeId::of::<T>())),
            "output port not registered"
        );
        assert!(
            self.inputs
                .contains(&(inp.node, inp.name, TypeId::of::<T>())),
            "input port not registered"
        );
        self.edges.push((
            TypeId::of::<T>(),
            (out.node, out.name),
            (inp.node, inp.name),
        ));
        self
    }

    /// Expose a node input as a graph input
    pub fn map_input<T: 'static + Send + Sync>(
        &mut self,
        graph_port: &'static str,
        node_in: InputPort<T>,
    ) -> &mut Self {
        self.external_inputs
            .push((graph_port, node_in.node, node_in.name, TypeId::of::<T>()));
        self
    }

    /// Expose a node output as a graph output
    pub fn map_output<T: 'static + Send + Sync>(
        &mut self,
        graph_port: &'static str,
        node_out: OutputPort<T>,
    ) -> &mut Self {
        self.external_outputs
            .push((graph_port, node_out.node, node_out.name, TypeId::of::<T>()));
        self
    }

    /// Finalize runnable graph
    pub fn finish(self) -> Graph {
        let nodes = self
            .nodes
            .into_iter()
            .map(|(NodeId(id), node)| (id.to_string(), node))
            .collect();

        let edges = self
            .edges
            .into_iter()
            .map(|(_ty, (NodeId(f), p), (NodeId(t), q))| {
                (
                    (f.to_string(), p.to_string()),
                    (t.to_string(), q.to_string()),
                )
            })
            .collect();

        let external_inputs = self
            .external_inputs
            .into_iter()
            .map(|(g, NodeId(n), p, _)| (g.to_string(), (n.to_string(), p.to_string())))
            .collect();

        let external_outputs = self
            .external_outputs
            .into_iter()
            .map(|(g, NodeId(n), p, _)| (g.to_string(), (n.to_string(), p.to_string())))
            .collect();

        Graph {
            nodes,
            edges,
            external_inputs,
            external_outputs,
        }
    }
}

/// Helper for declaring ports on a new node
pub struct NodeBuilder<'a> {
    gb: &'a mut GraphBuilder,
    node: NodeId,
}

impl<'a> NodeBuilder<'a> {
    /// Declare an input port and keep the builder
    pub fn input<T: 'static + Send + Sync>(&mut self, name: &'static str) -> InputPort<T> {
        self.gb.inputs.push((self.node, name, TypeId::of::<T>()));
        InputPort {
            node: self.node,
            name,
            _ty: PhantomData,
        }
    }

    /// Declare an output port and keep the builder
    pub fn output<T: 'static + Send + Sync>(&mut self, name: &'static str) -> OutputPort<T> {
        self.gb.outputs.push((self.node, name, TypeId::of::<T>()));
        OutputPort {
            node: self.node,
            name,
            _ty: PhantomData,
        }
    }
}

/// The runtime graph executor
pub struct Graph {
    nodes: HashMap<String, Arc<dyn NodeWrapper>>,
    edges: Vec<((String, String), (String, String))>,
    external_inputs: HashMap<String, (String, String)>,
    external_outputs: HashMap<String, (String, String)>,
}

impl Graph {
    /// Runs the graph, using Option<Any> for missing ports
    pub async fn run(
        &self,
        mut graph_inputs: HashMap<String, Arc<dyn Any + Send + Sync>>,
    ) -> HashMap<String, Arc<dyn Any + Send + Sync>> {
        let mut node_inputs: HashMap<
            (String, String),
            Arc<HashMap<String, Option<Arc<dyn Any + Send + Sync>>>>,
        > = HashMap::new();

        for (g_port, (node_id, node_port)) in &self.external_inputs {
            if let Some(val) = graph_inputs.remove(g_port) {
                let mut m = HashMap::new();
                m.insert(node_port.clone(), Some(val));
                node_inputs.insert((node_id.clone(), node_port.clone()), Arc::new(m));
            }
        }

        let mut executed = HashSet::new();
        let mut out_map = HashMap::new();

        while executed.len() < self.nodes.len() {
            let mut progress = false;

            for (node_id, node) in &self.nodes {
                if executed.contains(node_id) {
                    continue;
                }

                let mut flat: HashMap<String, Option<Arc<dyn Any + Send + Sync>>> = HashMap::new();
                for ((n, _), arc_map) in &node_inputs {
                    if n == node_id {
                        for (k, v) in arc_map.iter() {
                            flat.insert(k.clone(), v.clone());
                        }
                    }
                }

                let is_mapped = self.external_inputs.values().any(|(n, _)| n == node_id);
                if !flat.is_empty() || is_mapped {
                    let arc_in = Arc::new(flat);
                    let arc_out = node.call_boxed(arc_in).await;

                    for (out_port, val) in arc_out.iter() {
                        let mut linked = false;
                        for ((from_n, from_p), (to_n, to_p)) in &self.edges {
                            if from_n == node_id && from_p == out_port {
                                let mut new_map = HashMap::new();
                                new_map.insert(to_p.clone(), Some(Arc::clone(val)));
                                node_inputs.insert((to_n.clone(), to_p.clone()), Arc::new(new_map));
                                linked = true;
                            }
                        }
                        if !linked {
                            for (g_out, (n, p)) in &self.external_outputs {
                                if n == node_id && p == out_port {
                                    out_map.insert(g_out.clone(), Arc::clone(val));
                                }
                            }
                        }
                    }
                    executed.insert(node_id.clone());
                    progress = true;
                }
            }

            if !progress {
                panic!("graph deadlock or missing required inputs");
            }
        }

        out_map
    }
}

#[async_trait]
impl Node for Graph {
    async fn run(
        &self,
        inputs: HashMap<String, Option<Arc<dyn Any + Send + Sync>>>,
    ) -> HashMap<String, Arc<dyn Any + Send + Sync>> {
        let mut initial = HashMap::new();
        for (k, v_opt) in inputs {
            if let Some(v) = v_opt {
                initial.insert(k, v);
            }
        }
        self.run(initial).await
    }
}
