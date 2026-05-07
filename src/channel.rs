use std::collections::HashMap;

use tokio::sync::{broadcast, mpsc};

use crate::{
    client::ClientID,
    errors::MQError,
    message::Message,
};

#[derive(Debug)]
pub struct SlimChannel {
    pub name: String,
    pub topic: String,
    pub clients: Vec<ClientID>,
}

// channel is the intermedia layer which hold the clients and messages
#[derive(Debug)]
pub struct Channel {
    pub name: String,
    pub topic: String,
    // topic broadcasr message to channels
    pub memory_msg_chan: broadcast::Receiver<Message>,
    pub clients: HashMap<ClientID, mpsc::Sender<Message>>,
}

impl Channel {
    pub fn new(name: &str, topic: &str, rx: broadcast::Receiver<Message>) -> Self {
        Self {
            name: name.to_string(),
            topic: topic.to_string(),
            memory_msg_chan: rx,
            clients: HashMap::new(),
        }
    }

    pub fn add_client(&mut self, c_id: ClientID, tx: mpsc::Sender<Message>) -> Result<(), MQError> {
        self.clients.insert(c_id, tx);
        Ok(())
    }
}
