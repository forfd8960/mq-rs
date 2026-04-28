use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use tokio::{
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

use crate::{channel::Channel, client::{Client, ClientID}, errors::MQError, topic::Topic};

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

#[derive(Debug)]
pub struct MQ {
    // clientIDSequence int64
    pub client_id_seq: u64,
    pub opts: Options,
    pub topic_map: HashMap<String, Topic>,
    pub clients: HashMap<ClientID, Client>,
    pub channels: HashMap<String, Channel>
}

impl MQ {
    pub fn new(options: Options) -> Result<Self, MQError> {
        let opts = options.clone();
        Ok(Self {
            client_id_seq: 0,
            opts,
            topic_map: HashMap::new(),
            clients: HashMap::new(),
            channels: HashMap::new(),
        })
    }

    pub fn incr_client_id_seq(&mut self) -> u64 {
        let counter = AtomicU64::new(self.client_id_seq);
        // atomic increment, returns previous value
        counter.fetch_add(1, Ordering::SeqCst);
        self.client_id_seq = counter.load(Ordering::SeqCst);
        self.client_id_seq
    }

    pub fn get_topic(&self, name: &str) -> Option<&Topic> {
        self.topic_map.get(name)
    }

    pub fn create_topic(&mut self, t: Topic) -> Result<(), MQError> {
        let name = t.name.clone();
        if let Some(_) = self.get_topic(&name) {
            return Err(MQError::TopicAlreadyExists(name));
        }

        self.topic_map.insert(name ,t);
        Ok(())
    }

    pub fn create_client(&mut self, c_id: ClientID, client: Client) -> Result<(), MQError> {
        if let Some(_) = self.clients.get(&c_id) {
            return Ok(());
        }

        self.clients.insert(c_id ,client);
        Ok(())
    }

    pub fn create_channel(&mut self, chan: Channel) -> Result<(), MQError> {
        let name = chan.name.clone();
        if let Some(_) = self.channels.get(&name) {
            return Ok(());
        }

        self.channels.insert(name,chan);
        Ok(())
    }

    pub fn get_client(&self, c_id: ClientID) -> Option<&Client> {
        self.clients.get(&c_id)
    }

    pub async fn topic_add_client(&mut self, c_id: ClientID, t: &str, chan: &str) -> Result<(), MQError> {
        let topic = self.topic_map.get_mut(t);
        match topic {
            Some(t) => {
                t.add_client(c_id, chan).await
            }
            None => return Err(MQError::TopicNotFound(t.to_string()))
        }
    }

    pub fn add_client_to_chan(&mut self, chan_name: &str, c_id: ClientID) -> Result<(), MQError> {
        let chan = self.channels.get_mut(chan_name);
        match chan {
            Some(ch) => ch.add_client(c_id),
            None => Ok(())
        }
    }
}

pub async fn start_queue(mq: Arc<Mutex<MQ>>, tcp_addr: &str) -> Result<(), MQError> {
    println!("starting queue at: {}", tcp_addr);
    let listener = TcpListener::bind(tcp_addr).await?;

    println!("queue initilized, waiting connection");
    loop {
        // accept a new connection
        let (stream, addr) = listener.accept().await?;
        println!("accepted connection from {}", addr);

        let mq_clone = mq.clone();
        // spawn a task to handle this client loop
        tokio::spawn(async move {
            if let Err(e) = tcp_handler(stream, mq_clone).await {
                eprintln!("client {} error: {}", addr, e);
            }
        });
    }
}

async fn tcp_handler(stream: TcpStream, mq: Arc<Mutex<MQ>>) -> Result<(), MQError> {
    let c_id = {
        let mut q = mq.lock().await;
        q.incr_client_id_seq()
    };

    let mut client = new_client(stream, mq, c_id).await;
    client.handle_conn().await
}

pub async fn new_client(tcp_stream: TcpStream, mq: Arc<Mutex<MQ>>, client_id: u64) -> Client {
    Client {
        id: client_id,
        stream: Arc::new(Mutex::new(tcp_stream)),
        mq,
        sub_event_chan: None,
    }
}
