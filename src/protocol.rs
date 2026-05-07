use std::sync::Arc;

use bytes::Buf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio_util::bytes::BytesMut;
use tokio_util::codec::LengthDelimitedCodec;

use crate::message::decode_message;
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
}

impl TryFrom<u32> for FrameType {
    type Error = MQError;
    fn try_from(value: u32) -> Result<Self, MQError> {
        match value {
            0 => Ok(FrameType::Response),
            1 => Ok(FrameType::Error),
            2 => Ok(FrameType::Message),
            _ => Err(MQError::BadProtocol),
        }
    }
}

impl From<FrameType> for u32 {
    fn from(value: FrameType) -> Self {
        match value {
            FrameType::Response => 0,
            FrameType::Error => 1,
            FrameType::Message => 2,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    PUB { topic: String, msg: Vec<u8> },
    SUB { topic: String, channel: String },
}

/// Common command encoder for SUB / PUB line commands.
pub fn encode_sub(topic: &str, channel: &str) -> BytesMut {
    let line = format!("SUB {topic} {channel}\n");
    BytesMut::from(line.as_bytes())
}

pub fn encode_pub(topic: &str, body: &[u8]) -> BytesMut {
    // Example command format; adjust if your server expects a different PUB wire format.
    // Here: "PUB <topic>\n" + raw body
    let mut out = BytesMut::new();
    out.extend_from_slice(format!("PUB {topic}\n").as_bytes());
    out.extend_from_slice(body);
    out
}

/// Decodes one length-delimited payload into `<frame_type><data>`.
/// (`<size>` is already stripped by LengthDelimitedCodec.)
pub fn decode_server_frame(mut payload: BytesMut) -> Result<(FrameType, BytesMut), MQError> {
    if payload.is_empty() {
        return Err(MQError::BadEventPayload("empty frame payload".into()));
    }
    let ft = FrameType::try_from(payload.get_u32())?;
    Ok((ft, payload))
}

pub fn decode_line_to_event(payload: BytesMut) -> Result<Event, MQError> {
    let payload_vec = payload.to_vec();
    let payload_parts: Vec<&[u8]> = payload_vec.split(|e| *e == b'\n').collect();

    let line = payload_parts[0];
    let event_head = line.to_vec();
    let parts: Vec<&[u8]> = event_head.split(|d| *d == b' ').collect();

    match parts[0] {
        b"PUB" => {
            if payload_parts.len() < 1 {
                return Err(MQError::BadEventPayload(
                    "payload should contain \\n".to_string(),
                ));
            }

            let msg_bs = payload_parts[1];
            Ok(Event::PUB {
                topic: bytes_to_string(parts[1])?,
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

pub fn build_r_w_codec() -> (LengthDelimitedCodec, LengthDelimitedCodec) {
    let mut r_builder = LengthDelimitedCodec::builder();
    let length_decoder = r_builder
        .big_endian()
        .length_field_offset(0)
        .length_field_type::<u32>()
        .length_field_length(4)
        .new_codec();

    let mut w_builder = LengthDelimitedCodec::builder();
    let length_encoder = w_builder
        .big_endian()
        .length_field_offset(0)
        .length_field_type::<u32>()
        .length_field_length(4)
        .new_codec();

    (length_decoder, length_encoder)
}

pub fn display_data(frame_type: FrameType, data: BytesMut) {
    match frame_type {
        FrameType::Message => {
            // optional: decode into Message (depends on server message format)
            match decode_message(&data) {
                Ok(msg) => {
                    println!(
                        "MSG id={} ts={} attempts={} body_len={}",
                        msg.id,
                        msg.ts,
                        msg.attempts,
                        msg.body.len()
                    );
                }
                Err(_) => {
                    // If your data is not Message format, keep raw handling:
                    println!("MSG raw len={}", data.len());
                }
            }
        }
        FrameType::Response => {
            println!("OK: {}", String::from_utf8_lossy(&data));
        }
        FrameType::Error => {
            eprintln!("server error: {}", String::from_utf8_lossy(&data))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_event() {
        let mut buf = BytesMut::new();
        let event = b"PUB orders\n{'order_id': 123, 'price': 1200.99}";
        buf.extend_from_slice(event);

        let res = decode_line_to_event(buf);
        println!("decode event: {:?}", res);
        assert!(res.is_ok());

        let event = res.unwrap();
        assert_eq!(
            event,
            Event::PUB {
                topic: "orders".to_string(),
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
