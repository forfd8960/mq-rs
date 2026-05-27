use std::{
    collections::{BinaryHeap, HashMap},
    ops::Add,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use tokio::sync::{broadcast, mpsc, RwLock};

use crate::{client::ClientID, errors::MQError, message::Message};

#[derive(Debug)]
pub struct SlimChannel {
    pub name: String,
    pub topic: String,
    pub clients: Vec<ClientID>,
}

#[derive(Debug)]
pub struct InFlightMessages {
    pub in_flight_queue: BinaryHeap<Message>,
    pub in_flight_messages: HashMap<String, Message>,
}

impl InFlightMessages {
    pub fn new() -> Self {
        Self {
            in_flight_queue: BinaryHeap::new(),
            in_flight_messages: HashMap::new(),
        }
    }
}

// channel is the intermedia layer which hold the clients and messages
#[derive(Debug)]
pub struct Channel {
    pub name: String,
    pub topic: String,
    // topic broadcasr message to channels
    pub memory_msg_chan: broadcast::Sender<Message>,
    pub clients: HashMap<ClientID, ()>,
    pub in_flight_messages: RwLock<InFlightMessages>,
}

impl Channel {
    pub fn new(name: &str, topic: &str) -> Self {
        let (tx, _) = broadcast::channel(1000);
        Self {
            name: name.to_string(),
            topic: topic.to_string(),
            memory_msg_chan: tx,
            clients: HashMap::new(),
            in_flight_messages: RwLock::new(InFlightMessages::new()),
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

    pub async fn start_in_flight_timeout(
        &mut self,
        msg: &mut Message,
        client_id: u64,
        timeout: Duration,
    ) {
        let now = Instant::now();
        msg.set_client_id(client_id);
        msg.set_delivery_ts(now);

        let pri = SystemTime::now()
            .add(timeout)
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            .try_into()
            .unwrap();
        msg.set_pri(pri);

        self.push_msg_to_in_flight(msg).await
    }

    async fn push_msg_to_in_flight(&mut self, msg: &Message) {
        let mut inf_msg = self.in_flight_messages.write().await;

        let m_c = msg.clone();

        let msg_id = msg.id.clone();
        inf_msg
            .in_flight_messages
            .insert(msg_id.to_string(), msg.clone());

        inf_msg.in_flight_queue.push(m_c);
    }

    pub async fn finish_message(&mut self, c_id: ClientID, msg_id: &str) -> Result<(), MQError> {
        let _ = self.pop_in_flight_msg(c_id, msg_id).await?;
        //todo: remove from in_flight_queue
        Ok(())
    }
    async fn pop_in_flight_msg(
        &mut self,
        c_id: ClientID,
        msg_id: &str,
    ) -> Result<Option<Message>, MQError> {
        let mut inf_msg = self.in_flight_messages.write().await;

        let msg = inf_msg.in_flight_messages.remove(msg_id);
        match msg {
            Some(m) => {
                if m.client_id() != Some(c_id) {
                    return Err(MQError::MessageNotOwned(msg_id.to_string()));
                }
                Ok(Some(m))
            }
            None => Ok(None),
        }
    }
}
