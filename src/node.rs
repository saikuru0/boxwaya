use async_trait::async_trait;
use std::{any::Any, collections::HashMap, sync::Arc};

#[async_trait]
pub trait Node: Send + Sync {
    async fn run(
        &self,
        inputs: HashMap<String, Option<Arc<dyn Any + Send + Sync>>>,
    ) -> HashMap<String, Arc<dyn Any + Send + Sync>>;
}
