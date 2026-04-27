use std::collections::HashMap;

use tokio::sync::{broadcast, mpsc};

use crate::{channel::Channel, message::Message, mq::MQ};

pub type MsgSender = mpsc::Sender<Message>;

#[derive(Debug)]
pub struct Topic {
    pub name: String,
    pub mq: MQ,
    pub msg_count: u64,
    pub msg_size: u64,

    // topic receive message and send
    // protocol call topic to put message into sender
    pub memory_msg_chan_sender: mpsc::Sender<Message>,

    // topic receive message from client conn
    // recv msg in message pump and send it through channel broadcast sender
    pub memory_msg_chan_recv: mpsc::Receiver<Message>,

    // and then broadcast the message to all channels
    pub channel_msg_sender: broadcast::Sender<Message>,
    pub chan_map: HashMap<String, Channel>,
}
