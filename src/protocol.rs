use std::sync::Arc;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio_util::bytes::BytesMut;

use crate::{errors::MQError, mq::MQ};

/*
const (
    frameTypeResponse int32 = 0
    frameTypeError    int32 = 1
    frameTypeMessage  int32 = 2
)
*/
#[derive(Debug, Clone, PartialEq)]
pub enum FrameType {
    Response,
    Error,
    Message,
    Unknown
}

impl From<u32> for FrameType {
    fn from(value: u32) -> Self {
        match value {
            0 => FrameType::Response,
            1 => FrameType::Error,
            2 => FrameType::Message,
            _ => FrameType::Unknown,
        }
    } 
}

impl From<FrameType> for u32 {
    fn from(value: FrameType) -> Self {
        match value {
            FrameType::Response => 0,
            FrameType::Error => 1,
            FrameType::Message => 2,
            FrameType::Unknown => u32::MAX,
        }
    } 
}


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
