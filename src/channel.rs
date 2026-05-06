use std::{collections::HashMap, sync::Arc};

use tokio::sync::{broadcast, Mutex};

use crate::{client::ClientID, errors::MQError, message::Message, mq::MQ};

// channel is the intermedia layer which hold the clients and messages
#[derive(Debug)]
pub struct Channel {
    pub name: String,
    pub topic: String,
    pub mq: Arc<Mutex<MQ>>,
    // topic broadcasr message to channels
    pub memory_msg_chan: broadcast::Receiver<Message>,
    pub clients: HashMap<ClientID, ()>,
}

impl Channel {
    pub fn new(
        name: &str,
        topic: &str,
        mq: Arc<Mutex<MQ>>,
        rx: broadcast::Receiver<Message>,
    ) -> Self {
        Self {
            name: name.to_string(),
            topic: topic.to_string(),
            mq,
            memory_msg_chan: rx,
            clients: HashMap::new(),
        }
    }

    pub fn add_client(&mut self, c_id: ClientID) -> Result<(), MQError> {
        self.clients.insert(c_id, ());
        Ok(())
    }
}
