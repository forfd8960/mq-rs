use std::time::{SystemTime, UNIX_EPOCH};

use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct Message {
    pub id: Uuid,
    pub body: Vec<u8>,
    pub ts: u128, // unix nano
    pub attempts: u16,
}

impl Message {
    pub fn new(body: Vec<u8>) -> Self {
        let now_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time went backwards")
            .as_nanos();
        Self {
            id: Uuid::new_v4(),
            body,
            ts: now_ts,
            attempts: 0,
        }
    }
}

pub fn encode(msg: Message) -> Vec<u8> {
    let mut buf: Vec<u8> = Vec::new();
    buf.extend_from_slice(msg.ts.to_be_bytes().as_ref());
    buf.extend_from_slice(msg.attempts.to_be_bytes().as_ref());
    buf.extend_from_slice(msg.id.as_bytes());
    buf.extend_from_slice(&msg.body);
    buf
}
