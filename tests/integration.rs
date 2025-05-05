use async_trait::async_trait;
use boxwaya::{graph::GraphBuilder, *};
use futures::executor::block_on;
use std::{any::Any, collections::HashMap, sync::Arc};

struct SumNode;
#[async_trait]
impl Node for SumNode {
    async fn run(
        &self,
        inputs: HashMap<String, Option<Arc<dyn Any + Send + Sync>>>,
    ) -> HashMap<String, Arc<dyn Any + Send + Sync>> {
        let a = inputs
            .get("a")
            .and_then(|o| o.as_ref())
            .and_then(|v| v.downcast_ref::<i32>())
            .copied()
            .unwrap_or(0);
        let b = inputs
            .get("b")
            .and_then(|o| o.as_ref())
            .and_then(|v| v.downcast_ref::<i32>())
            .copied()
            .unwrap_or(0);
        let mut out = HashMap::new();
        out.insert("sum".to_string(), Arc::new(a + b) as _);
        out
    }
}

struct DoubleNode;
#[async_trait]
impl Node for DoubleNode {
    async fn run(
        &self,
        inputs: HashMap<String, Option<Arc<dyn Any + Send + Sync>>>,
    ) -> HashMap<String, Arc<dyn Any + Send + Sync>> {
        let v = inputs
            .get("value")
            .and_then(|o| o.as_ref())
            .and_then(|v| v.downcast_ref::<i32>())
            .copied()
            .unwrap_or(0);
        let mut out = HashMap::new();
        out.insert("result".to_string(), Arc::new(v * 2) as _);
        out
    }
}

#[test]
fn node() {
    let mut inputs: HashMap<_, Option<Arc<dyn Any + Send + Sync>>> = HashMap::new();
    inputs.insert("a".into(), Some(Arc::new(2i32)));
    inputs.insert("b".into(), Some(Arc::new(3i32)));

    let out = block_on(SumNode.run(inputs));
    let sum = out["sum"].downcast_ref::<i32>().unwrap();
    assert_eq!(*sum, 5);
}

#[test]
fn graph() {
    let mut builder = GraphBuilder::new();

    let mut sum_builder = builder.add_node(SumNode);
    let in_a = sum_builder.input::<i32>("a");
    let in_b = sum_builder.input::<i32>("b");
    let out_sum = sum_builder.output::<i32>("sum");

    let mut dbl_builder = builder.add_node(DoubleNode);
    let in_val = dbl_builder.input::<i32>("value");
    let out_res = dbl_builder.output::<i32>("result");

    builder
        .map_input("a", in_a)
        .map_input("b", in_b)
        .connect(out_sum, in_val)
        .map_output("out", out_res);

    let graph = builder.finish();

    let mut inputs: HashMap<_, Arc<dyn Any + Send + Sync>> = HashMap::new();
    inputs.insert("a".into(), Arc::new(4i32));
    inputs.insert("b".into(), Arc::new(6i32));

    let out_map = block_on(graph.run(inputs));
    let result = out_map["out"].downcast_ref::<i32>().unwrap();
    assert_eq!(*result, 20);
}

#[test]
fn subgraph() {
    let mut sub_b = GraphBuilder::new();
    let mut sum_b = sub_b.add_node(SumNode);
    let sx = sum_b.input::<i32>("a");
    let sy = sum_b.input::<i32>("b");
    let ssum = sum_b.output::<i32>("sum");
    let mut dbl_b = sub_b.add_node(DoubleNode);
    let sv = dbl_b.input::<i32>("value");
    let sr = dbl_b.output::<i32>("result");

    sub_b
        .map_input("x", sx)
        .map_input("y", sy)
        .connect(ssum, sv)
        .map_output("z", sr);
    let sub_graph = sub_b.finish();

    let mut parent = GraphBuilder::new();
    let mut sub_node = parent.add_node(sub_graph);
    let px = sub_node.input::<i32>("x");
    let py = sub_node.input::<i32>("y");
    let pz = sub_node.output::<i32>("z");
    let mut dbl2 = parent.add_node(DoubleNode);
    let dv2 = dbl2.input::<i32>("value");
    let dr2 = dbl2.output::<i32>("result");

    parent
        .map_input("a", px)
        .map_input("b", py)
        .connect(pz, dv2)
        .map_output("result", dr2);
    let graph = parent.finish();

    let mut inputs = HashMap::new();
    inputs.insert("a".into(), Arc::new(2i32) as _);
    inputs.insert("b".into(), Arc::new(3i32) as _);

    let out_map = block_on(graph.run(inputs));
    let final_res = out_map["result"].downcast_ref::<i32>().unwrap();

    assert_eq!(*final_res, 20);
}
