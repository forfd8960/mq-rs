use std::collections::HashMap;

use tokio::sync::broadcast;

use crate::{
    client::{Client, ClientID},
    message::Message,
    mq::MQ,
};

// channel is the intermedia layer which hold the clients and messages
#[derive(Debug)]
pub struct Channel {
    pub name: String,
    pub topic: String,
    pub mq: MQ,
    // topic broadcasr message to channels
    pub memory_msg_chan: broadcast::Receiver<Message>,
    pub clients: HashMap<ClientID, Client>,
}
