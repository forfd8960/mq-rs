use std::{
    cmp::Ordering,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use uuid::Uuid;

use crate::errors::MQError;

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct Message {
    pub id: Uuid,
    pub body: Vec<u8>,
    pub ts: u128, // unix nano
    pub attempts: u16,

    delivery_ts: Option<Instant>,
    client_id: Option<u64>,
    pri: Option<i64>, // timeout
    index: i64,
    deferred: Option<Duration>,
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
            delivery_ts: None,
            client_id: None,
            pri: None,
            index: -1,
            deferred: None,
        }
    }

    pub fn set_client_id(&mut self, client_id: u64) {
        self.client_id = Some(client_id)
    }

    pub fn client_id(&self) -> Option<u64> {
        self.client_id
    }

    pub fn set_delivery_ts(&mut self, ts: Instant) {
        self.delivery_ts = Some(ts)
    }

    pub fn set_pri(&mut self, timeout: i64) {
        self.pri = Some(timeout)
    }
}

impl Ord for Message {
    fn cmp(&self, other: &Self) -> Ordering {
        other.pri.cmp(&self.pri)
    }
}

impl PartialOrd for Message {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub fn encode_msg(msg: Message) -> Vec<u8> {
    let mut buf: Vec<u8> = Vec::new();
    buf.extend_from_slice(msg.ts.to_be_bytes().as_ref());
    buf.extend_from_slice(msg.attempts.to_be_bytes().as_ref());
    buf.extend_from_slice(msg.id.as_bytes());
    buf.extend_from_slice(&msg.body);
    buf
}

// Example message body decoder (if frame_type == Msg).
// Body format assumed:
// [16 bytes ts(u128)][2 bytes attempts][16 bytes uuid][remaining bytes body]
pub fn decode_message(data: &[u8]) -> Result<Message, MQError> {
    const META: usize = 16 + 16 + 2;
    if data.len() < META {
        return Err(MQError::BadResponse(format!(
            "message too short: {} < {}",
            data.len(),
            META
        )));
    }

    let ts = u128::from_be_bytes(
        data[0..16]
            .try_into()
            .map_err(|_| MQError::BadResponse("invalid ts bytes".into()))?,
    );
    let attempts = u16::from_be_bytes(
        data[16..18]
            .try_into()
            .map_err(|_| MQError::BadResponse("invalid attempts bytes".into()))?,
    );

    let id = Uuid::from_slice(&data[18..34])
        .map_err(|e| MQError::BadResponse(format!("invalid uuid in message: {e}")))?;

    let body = data[34..].to_vec();

    let mut msg = Message::new(body);
    msg.id = id;
    msg.ts = ts;
    msg.attempts = attempts;

    Ok(msg)
}
