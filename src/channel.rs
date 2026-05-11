use std::collections::HashMap;

use tokio::sync::{broadcast, mpsc};

use crate::{client::ClientID, errors::MQError, message::Message};

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
    pub memory_msg_chan: broadcast::Sender<Message>,
    pub clients: HashMap<ClientID, ()>,
}

impl Channel {
    pub fn new(name: &str, topic: &str) -> Self {
        let (tx, _) = broadcast::channel(1000);
        Self {
            name: name.to_string(),
            topic: topic.to_string(),
            memory_msg_chan: tx,
            clients: HashMap::new(),
        }
    }

    pub fn add_client(&mut self, c_id: ClientID) -> Result<(), MQError> {
        self.clients.insert(c_id, ());
        Ok(())
    }

    pub fn sub_channel(&self) -> broadcast::Receiver<Message> {
        self.memory_msg_chan.subscribe()
    }

    pub fn send_msg(&self, msg: Message) {
        match self.memory_msg_chan.send(msg) {
            Ok(_) => {}
            Err(e) => eprintln!("send msg error: {}", e),
        }
    }
}
