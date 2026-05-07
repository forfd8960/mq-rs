use std::{collections::HashMap, sync::Arc};

use tokio::sync::{broadcast, mpsc, RwLock};

use crate::{
    channel::{Channel, SlimChannel},
    client::ClientID,
    errors::MQError,
    message::Message,
    mq::MQ,
};

pub type MsgSender = mpsc::Sender<Message>;

#[derive(Debug)]
pub struct Topic {
    pub name: String,
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
    pub chan_map: Arc<RwLock<HashMap<String, Channel>>>,
}

impl Topic {
    pub fn new(name: &str, msg_cap: usize) -> Self {
        let (msg_chan_sender, msg_chan_recv) = mpsc::channel::<Message>(msg_cap);
        let (chan_msg_chan_sender, _) = broadcast::channel::<Message>(msg_cap);
        Self {
            name: name.to_string(),
            msg_count: 0,
            msg_size: 0,
            memory_msg_chan_sender: msg_chan_sender,
            memory_msg_chan_recv: msg_chan_recv,
            channel_msg_sender: chan_msg_chan_sender,
            chan_map: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn add_client(
        &mut self,
        c_id: ClientID,
        chan_name: &str,
        tx: mpsc::Sender<Message>,
    ) -> Result<(), MQError> {
        let topic_name = self.name.clone();
        let mut chan_map = self.chan_map.write().await;

        let channel = chan_map.get_mut(chan_name);
        match channel {
            Some(chan) => {
                let _ = chan.add_client(c_id, tx);
            }
            None => {
                let rx = self.sub_msg_chan();
                let mut chan = Channel::new(chan_name, &topic_name, rx);
                let _ = chan.add_client(c_id, tx);
                chan_map.insert(chan_name.to_string(), chan);
            }
        }

        Ok(())
    }

    pub fn sub_msg_chan(&self) -> broadcast::Receiver<Message> {
        self.channel_msg_sender.subscribe()
    }

    pub async fn list_chans(&self) -> Vec<SlimChannel> {
        let mut slim_chans = vec![];

        let chan_map = self.chan_map.read().await;
        for (_, chan) in chan_map.iter() {
            slim_chans.push(SlimChannel {
                name: chan.name.clone(),
                topic: chan.topic.clone(),
                clients: chan.clients.iter().map(|(c_id, _)| *c_id).collect(),
            });
        }
        slim_chans
    }

    pub async fn add_chan(&mut self, chan_name: &str, chan: Channel) {
        let mut chan_map = self.chan_map.write().await;
        chan_map.insert(chan_name.to_string(), chan);
    }

    pub fn put_message(&self, msg: Message) {
        let _ = self.channel_msg_sender.send(msg);
    }
}
