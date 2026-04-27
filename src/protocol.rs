use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex};

use crate::channel::Channel;
use crate::client::Client;
use crate::{errors::MQError, mq::MQ};

/*
const (
    frameTypeResponse int32 = 0
    frameTypeError    int32 = 1
    frameTypeMessage  int32 = 2
)
*/
#[derive(Debug, Clone, PartialEq)]
pub struct FrameType(pub u8);

#[derive(Debug)]
pub struct Protocol {
    pub mq: Arc<Mutex<MQ>>,
    pub channel_sender: mpsc::Sender<Channel>,
}

impl Protocol {
    pub fn new(mq: Arc<Mutex<MQ>>) -> Self {
        let (tx, _) = mpsc::channel(1000);
        Self {
            mq,
            channel_sender: tx,
        }
    }

    pub async fn new_client(&self, tcp_stream: TcpStream) -> Client {
        let mq = self.mq.lock().await;

        let counter = AtomicU64::new(mq.client_id_seq);
        // atomic increment, returns previous value
        counter.fetch_add(1, Ordering::SeqCst);
        Client {
            id: counter.load(Ordering::SeqCst),
            stream: Arc::new(Mutex::new(tcp_stream)),
            sub_event_chan: None,
        }
    }
}

pub async fn tcp_handle(listener: TcpListener, mq: Arc<Mutex<MQ>>) -> Result<(), MQError> {
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
    let prot = Protocol::new(mq);
    let client = prot.new_client(stream).await;
    client.handle_conn().await
}
