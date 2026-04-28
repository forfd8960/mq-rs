use std::{collections::HashMap, rc::Rc, sync::Arc, time::Duration};

use tokio::{net::TcpListener, sync::Mutex};

use crate::{errors::MQError, topic::Topic};

#[derive(Debug, Clone)]
pub struct Options {
    pub tcp_addr: String,
    pub mem_queue_size: i64,
    pub max_msg_size_in_bytes: i64,
    pub max_body_size_in_bytes: i64,
    pub msg_timeout_in_sec: Duration,
    pub max_msg_timeout_in_min: Duration,
    pub max_channel_consumers: i64,
}

impl Options {
    pub fn new() -> Self {
        Self {
            tcp_addr: "0.0.0.0:5050".to_string(),
            mem_queue_size: 10000,
            max_msg_size_in_bytes: 1024 * 1024,
            max_body_size_in_bytes: 5 * 1024 * 1024,
            msg_timeout_in_sec: Duration::from_secs(60),
            max_msg_timeout_in_min: Duration::from_mins(15),
            max_channel_consumers: 10,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MQ {
    // clientIDSequence int64
    pub client_id_seq: u64,
    pub opts: Options,
    pub topic_map: HashMap<String, Topic>,
}

impl MQ {
    pub async fn new(options: Options) -> Result<Self, MQError> {
        let opts = options.clone();
        Ok(Self {
            client_id_seq: 0,
            opts,
            topic_map: HashMap::new(),
        })
    }

    pub fn get_topic(&self, name: &str) -> Option<&Topic> {
        self.topic_map.get(name)
    }

    pub async fn start_queue(&self) {

    }
}
