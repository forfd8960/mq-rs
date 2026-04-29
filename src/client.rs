use std::sync::Arc;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{tcp::OwnedWriteHalf, TcpStream},
    sync::{mpsc, Mutex},
};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

use crate::{
    channel::Channel,
    errors::MQError,
    mq::MQ,
    protocol::{decode_line_to_event, Event, FrameType},
};

pub type ClientID = u64;
const MAGIC: &'static str = "RQV0";

pub enum EventResp {
    Err(MQError),
    Msg(Vec<u8>),
}

#[derive(Debug)]
pub struct Client {
    pub id: ClientID,
    pub stream: Arc<Mutex<TcpStream>>,
    pub mq: Arc<Mutex<MQ>>,
    // SubEventChan      chan *Channel
    pub sub_event_chan: Option<mpsc::Receiver<Channel>>,
}

impl Client {
    pub async fn handle_conn(&mut self) -> Result<(), MQError> {
        let mut buf = vec![0u8; 4];
        let n = {
            let mut stream = self.stream.lock().await;
            stream.read(&mut buf).await?
        };

        if n == 0 {
            println!("client disconnected");
            return Ok(());
        }

        if buf != MAGIC.as_bytes() {
            {
                let mut stream = self.stream.lock().await;
                stream
                    .write(encode_error(MQError::BadProtocol).as_ref())
                    .await?;
            }
            return Ok(());
        }

        self.handle_stream().await?;

        Ok(())
    }

    async fn handle_stream(&mut self) -> Result<(), MQError> {
        let stream = Arc::try_unwrap(self.stream.clone())
            .map_err(|_| MQError::Custom("Failed to unwrap Arc".to_string()))?
            .into_inner();

        let (read_half, write_half) = stream.into_split();
        let (decoder, encoder) = build_r_w_codec();

        let mut framed_read = FramedRead::new(read_half, decoder);
        let mut framed_write: FramedWrite<OwnedWriteHalf, LengthDelimitedCodec> =
            FramedWrite::new(write_half, encoder);

        loop {
            match framed_read.next().await {
                Some(Ok(data)) => {
                    let event = decode_line_to_event(data)?;
                    let resp = self.handle_event(event).await;
                    match resp {
                        Ok(r) => {
                            self.send(&mut framed_write, r);
                        }
                        Err(e) => {
                            self.send(&mut framed_write, EventResp::Err(e));
                        }
                    }
                }
                Some(Err(e)) => {
                    self.send(&mut framed_write, EventResp::Err(MQError::IOError(e)));
                }
                None => break,
            }
        }

        Ok(())
    }

    async fn handle_event(&mut self, ev: Event) -> Result<EventResp, MQError> {
        match ev {
            Event::PUB {
                topic,
                channel,
                msg,
            } => self.handle_pub(&topic, &channel, msg).await,
            Event::SUB { topic, channel } => self.handle_sub(&topic, &channel).await,
        }
    }

    fn send(
        &self,
        writer: &mut FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,
        event_resp: EventResp,
    ) {
        match event_resp {
            EventResp::Err(e) => {
                send_framed_response(writer, FrameType::Error, e.to_string().as_bytes().to_vec())
            }
            EventResp::Msg(msg) => send_framed_response(writer, FrameType::Message, msg),
        }
    }

    async fn handle_sub(
        &mut self,
        topic_name: &str,
        channel_name: &str,
    ) -> Result<EventResp, MQError> {
        let mut mq = self.mq.lock().await;
        let _ = mq
            .topic_add_client(self.id, topic_name, channel_name)
            .await?;
        Ok(EventResp::Msg(vec![]))
    }

    async fn handle_pub(
        &mut self,
        topic: &str,
        channel: &str,
        msg: Vec<u8>,
    ) -> Result<EventResp, MQError> {
        Ok(EventResp::Msg(vec![]))
    }
}

fn build_r_w_codec() -> (LengthDelimitedCodec, LengthDelimitedCodec) {
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

pub fn send_framed_response(
    writer: &mut FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,
    frame_type: FrameType,
    data: Vec<u8>,
) {
    let mut buf = vec![];
    let f_t: u32 = frame_type.into();
    buf.extend_from_slice(f_t.to_be_bytes().as_ref());
    buf.extend_from_slice(&data);

    let _ = writer.send(Bytes::from(buf));
    let _ = writer.flush();
}

fn encode_error(err: MQError) -> Vec<u8> {
    let mut buf = u32::from(FrameType::Error).to_be_bytes().to_vec();
    buf.extend_from_slice(err.to_string().as_bytes());

    let size = buf.len() + 4;
    let mut new_buf: Vec<u8> = Vec::with_capacity(size);
    new_buf.extend_from_slice(size.to_be_bytes().as_ref());
    new_buf.extend_from_slice(&buf);

    new_buf
}
