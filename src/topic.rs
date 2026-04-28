use std::{collections::HashMap, sync::Arc};

use tokio::sync::{broadcast, mpsc, Mutex};

use crate::{channel::Channel, client::ClientID, errors::MQError, message::Message, mq::MQ};

pub type MsgSender = mpsc::Sender<Message>;

#[derive(Debug)]
pub struct Topic {
    pub name: String,
    pub mq: Arc<Mutex<MQ>>,
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
    pub chan_map: HashMap<String, ()>,
}

impl Topic {
    pub fn new(name: &str, mq: Arc<Mutex<MQ>>, msg_cap: usize) -> Self {
        let (msg_chan_sender, msg_chan_recv) = mpsc::channel::<Message>(msg_cap);
        let (chan_msg_chan_sender, _) = broadcast::channel::<Message>(msg_cap);
        Self {
            name: name.to_string(),
            mq,
            msg_count: 0,
            msg_size: 0,
            memory_msg_chan_sender: msg_chan_sender,
            memory_msg_chan_recv: msg_chan_recv,
            channel_msg_sender: chan_msg_chan_sender,
            chan_map: HashMap::new(),
        }
    }

    pub async fn add_client(&mut self, c_id: ClientID, channel: &str) -> Result<(), MQError> {
        let topic_name = self.name.clone();

        let chan = self.chan_map.get(channel);
        match chan {
            Some(_) => {
                let mut mq = self.mq.lock().await;
                let _ = mq.add_client_to_chan(channel, c_id);
            }
            None => {
                let mq_c = self.mq.clone();
                let rx = self.channel_msg_sender.subscribe();

                let mut chan = Channel::new(channel, &topic_name, mq_c, rx);
                let _ = chan.add_client(c_id);

                let mut mq = self.mq.lock().await;
                let _ = mq.create_channel(chan);
                self.chan_map.insert(channel.to_string(), ());
            }
        }

        Ok(())
    }
}
