use std::{collections::HashMap, sync::Arc, time::Duration};

use tokio::sync::{broadcast, mpsc, RwLock};

use crate::{
    channel::{Channel, SlimChannel},
    client::ClientID,
    errors::MQError,
    message::Message,
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
    pub channel_msg_receiver: broadcast::Receiver<Message>,
    pub chan_map: Arc<RwLock<HashMap<String, Channel>>>,
}

impl Topic {
    pub async fn new(name: &str, msg_cap: usize) -> Self {
        let (msg_chan_sender, msg_chan_recv) = mpsc::channel::<Message>(msg_cap);
        let (chan_msg_sender, chan_msg_receiver) = broadcast::channel::<Message>(msg_cap);
        let topic = Topic {
            name: name.to_string(),
            msg_count: 0,
            msg_size: 0,
            memory_msg_chan_sender: msg_chan_sender,
            memory_msg_chan_recv: msg_chan_recv,
            channel_msg_sender: chan_msg_sender,
            channel_msg_receiver: chan_msg_receiver,
            chan_map: Arc::new(RwLock::new(HashMap::new())),
        };

        topic.message_pump().await;
        topic
    }

    pub async fn add_client(&mut self, c_id: ClientID, chan_name: &str) -> Result<(), MQError> {
        let topic_name = self.name.clone();
        let mut chan_map = self.chan_map.write().await;

        let channel = chan_map.get_mut(chan_name);
        match channel {
            Some(chan) => {
                let _ = chan.add_client(c_id);
            }
            None => {
                let mut chan = Channel::new(chan_name, &topic_name);
                let _ = chan.add_client(c_id);
                chan_map.insert(chan_name.to_string(), chan);
            }
        }

        Ok(())
    }

    pub fn sub_msg_chan(&self) -> broadcast::Receiver<Message> {
        self.channel_msg_sender.subscribe()
    }

    pub async fn get_channel_receiver(
        &self,
        channel: &str,
    ) -> Option<broadcast::Receiver<Message>> {
        let chan_map = self.chan_map.read().await;
        match chan_map.get(channel) {
            Some(chan) => Some(chan.sub_channel()),
            None => None,
        }
    }

    pub async fn start_in_flight(
        &self,
        chan_name: &str,
        msg: &mut Message,
        c_id: u64,
        timeout: Duration,
    ) -> Result<(), MQError> {
        let mut chan_map = self.chan_map.write().await;
        match chan_map.get_mut(chan_name) {
            Some(chan) => {
                chan.start_in_flight_timeout(msg, c_id, timeout).await;
                Ok(())
            }
            None => Err(MQError::ChannelNotFound(chan_name.to_string())),
        }
    }

    pub async fn finish_message(
        &self,
        chan_name: &str,
        c_id: ClientID,
        msg_id: &str,
    ) -> Result<(), MQError> {
        let mut chan_map = self.chan_map.write().await;
        match chan_map.get_mut(chan_name) {
            Some(chan) => {
                chan.finish_message(c_id, msg_id).await?;
                Ok(())
            }
            None => Err(MQError::ChannelNotFound(chan_name.to_string())),
        }
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

    pub async fn get_chan(&mut self, chan_name: &str) -> Option<SlimChannel> {
        let chan_map = self.chan_map.read().await;
        match chan_map.get(chan_name) {
            Some(chan) => Some(SlimChannel {
                name: chan.name.clone(),
                topic: chan.topic.clone(),
                clients: chan.clients.iter().map(|(c_id, _)| *c_id).collect(),
            }),
            None => None,
        }
    }

    pub async fn add_chan(&mut self, chan_name: &str, chan: Channel) {
        let mut chan_map = self.chan_map.write().await;
        chan_map.insert(chan_name.to_string(), chan);
    }

    pub fn put_message(&self, msg: Message) {
        let _ = self.channel_msg_sender.send(msg);
    }

    pub async fn message_pump(&self) {
        let chan_map = self.chan_map.clone();
        let mut chan_msg_receiver = self.channel_msg_sender.subscribe();

        tokio::spawn(async move {
            // let chans = chan_map.clone();
            while let Ok(msg) = chan_msg_receiver.recv().await {
                // fanout to channels
                for (_, chan) in chan_map.read().await.iter() {
                    chan.send_msg(msg.clone());
                }
            }
        });
    }
}
