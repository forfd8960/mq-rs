use std::{collections::HashMap, sync::Arc};

use tokio::sync::Mutex;

use crate::topic::Topic;

#[derive(Debug)]
pub struct MQ {
    // clientIDSequence int64
    pub client_id_seq: i64,
    pub topic_map: HashMap<String, Arc<Mutex<Topic>>>,
}
