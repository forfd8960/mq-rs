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
    sync::{mpsc, RwLock},
};

use crate::{
    channel::SlimChannel,
    client::{Client, ClientID},
    errors::MQError,
    message::Message,
    topic::Topic,
};

pub type ArcMQ = Arc<RwLock<MQ>>;

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
    pub client_id_seq: u64,
    pub opts: Options,
    pub topic_map: HashMap<String, Topic>,
    pub clients: HashMap<ClientID, Client>,
}

impl MQ {
    pub fn new(options: Options) -> Result<Self, MQError> {
        let opts = options.clone();
        Ok(Self {
            client_id_seq: 0,
            opts,
            topic_map: HashMap::new(),
            clients: HashMap::new(),
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

    pub fn get_topics(&self) -> Vec<&Topic> {
        self.topic_map.iter().map(|(_, t)| t).collect()
    }

    pub fn create_topic(&mut self, t: Topic) -> Result<(), MQError> {
        let name = t.name.clone();
        if let Some(_) = self.get_topic(&name) {
            return Err(MQError::TopicAlreadyExists(name));
        }

        self.topic_map.insert(name, t);
        Ok(())
    }

    pub fn create_client(&mut self, c_id: ClientID, client: Client) {
        if let Some(_) = self.clients.get(&c_id) {
            return;
        }

        self.clients.insert(c_id, client);
    }

    pub fn get_client(&self, c_id: ClientID) -> Option<&Client> {
        self.clients.get(&c_id)
    }

    pub async fn get_channels(&self, topic_name: &str) -> Vec<SlimChannel> {
        let topic = self.get_topic(topic_name);
        match topic {
            Some(t) => t.list_chans().await,
            None => vec![],
        }
    }

    pub fn get_clients(&self, chan: &SlimChannel) -> Vec<&Client> {
        let mut clients = vec![];

        for c_id in &chan.clients {
            if let Some(client) = self.clients.get(c_id) {
                clients.push(client);
            }
        }

        clients
    }

    pub async fn sub_channel(
        &mut self,
        c_id: ClientID,
        t: &str,
        chan: &str,
        tx: mpsc::Sender<Message>,
    ) -> Result<(), MQError> {
        let topic = self.topic_map.get_mut(t);
        match topic {
            Some(t) => t.add_client(c_id, chan, tx).await,
            None => return Err(MQError::TopicNotFound(t.to_string())),
        }
    }
}

pub async fn start_queue(mq: ArcMQ, tcp_addr: &str) -> Result<(), MQError> {
    println!("starting queue at: {}", tcp_addr);
    let listener = TcpListener::bind(tcp_addr).await?;

    println!("queue initilized, waiting connection");
    loop {
        // accept a new connection
        let (mut stream, addr) = listener.accept().await?;
        println!("accepted connection from {}", addr);

        let mq_clone = mq.clone();
        // spawn a task to handle this client loop
        tokio::spawn(async move {
            if let Err(e) = tcp_handler(&mut stream, mq_clone).await {
                eprintln!("client {} error: {}", addr, e);
            }
        });
    }
}

async fn tcp_handler(stream: &mut TcpStream, mq: ArcMQ) -> Result<(), MQError> {
    let mq_clone = mq.clone();

    let c_id = {
        let mut q = mq.write().await;
        q.incr_client_id_seq()
    };

    let mut client = Client::new(c_id, mq);
    {
        let mut mq = mq_clone.write().await;
        mq.create_client(c_id, client.clone());
    }

    client.handle_conn(stream).await
}
