use std::sync::Arc;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex};

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

pub enum Event {
    PUB {
        topic: String,
        channel: String,
        msg: String,
    },
    SUB {
        topic: String,
        channel: String,
    },
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
    let c_id = {
        let mut q = mq.lock().await;
        q.incr_client_id_seq()
    };

    let client = new_client(stream, mq, c_id).await;
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

pub fn decode_line_to_event(line: String) -> Result<Event, MQError> {
    let parts: Vec<&str> = line.split(" ").into_iter().collect();
    match parts[0] {
        "PUB" => {
            let topic = parts[1];
            let channel = parts[2];
            let msg = parts[3];
            Ok(Event::PUB {
                topic: topic.to_string(),
                channel: channel.to_string(),
                msg: msg.to_string(),
            })
        }
        "SUB" => {
            let topic = parts[1];
            let channel = parts[2];
            Ok(Event::SUB {
                topic: topic.to_string(),
                channel: channel.to_string(),
            })
        }
        _ => Err(MQError::UnknowEvent(parts[0].to_string())),
    }
}
