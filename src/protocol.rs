use std::sync::Arc;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex};
use tokio_util::bytes::{Bytes, BytesMut};

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

#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    PUB {
        topic: String,
        channel: String,
        msg: Vec<u8>,
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

pub fn decode_line_to_event(payload: BytesMut) -> Result<Event, MQError> {
    let payload_vec = payload.to_vec();
    let payload_parts: Vec<&[u8]> = payload_vec.split(|e| *e == b'\n').collect();

    let line = payload_parts[0];
    let event_head = line.to_vec();
    let parts: Vec<&[u8]> = event_head.split(|d| *d == b' ').collect();

    match parts[0] {
        b"PUB" => {
            if payload_parts.len() < 2 {
                return Err(MQError::BadEventPayload(
                    "payload should contain \\n".to_string(),
                ));
            }

            let msg_bs = payload_parts[1];
            Ok(Event::PUB {
                topic: bytes_to_string(parts[1])?,
                channel: bytes_to_string(parts[2])?,
                msg: msg_bs.to_vec(),
            })
        }
        b"SUB" => {
            let topic = bytes_to_string(parts[1])?;
            let channel = bytes_to_string(parts[2])?;
            Ok(Event::SUB { topic, channel })
        }
        _ => Err(MQError::UnknowEvent(bytes_to_string(parts[0])?)),
    }
}

fn bytes_to_string(bs: &[u8]) -> Result<String, MQError> {
    String::from_utf8(bs.to_vec())
        .map_err(|e| MQError::BadEventPayload("not valid topic name".to_string()))
}

#[cfg(test)]
mod tests {
    use tokio_util::bytes::BufMut;

    use super::*;

    #[test]
    fn test_decode_event() {
        let mut buf = BytesMut::new();
        let event = b"PUB orders analysis\n{'order_id': 123, 'price': 1200.99}";
        buf.extend_from_slice(event);

        let res = decode_line_to_event(buf);
        println!("decode event: {:?}", res);
        assert!(res.is_ok());

        let event = res.unwrap();
        assert_eq!(
            event,
            Event::PUB {
                topic: "orders".to_string(),
                channel: "analysis".to_string(),
                msg: b"{'order_id': 123, 'price': 1200.99}".to_vec(),
            }
        )
    }

    #[test]
    fn test_decode_event1() {
        let mut buf = BytesMut::new();
        let event = b"SUB orders analysis\n";
        buf.extend_from_slice(event);

        let res = decode_line_to_event(buf);
        println!("decode event: {:?}", res);
        assert!(res.is_ok());

        let event = res.unwrap();
        assert_eq!(
            event,
            Event::SUB {
                topic: "orders".to_string(),
                channel: "analysis".to_string(),
            }
        )
    }
}
