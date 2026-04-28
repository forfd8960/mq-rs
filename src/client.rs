use std::sync::Arc;

use futures::StreamExt;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{mpsc, Mutex},
};
use tokio_util::codec::{FramedRead, LengthDelimitedCodec, LinesCodec};

use crate::{
    channel::Channel,
    errors::MQError,
    mq::MQ,
    protocol::{Event, FrameType, decode_line_to_event},
};

pub type ClientID = u64;
const MAGIC: &'static str = "RQV0";

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
            let mut s = self.stream.lock().await;
            send_framed_response(&mut s, FrameType(1), b"bad protocol".to_vec()).await?;
            return Ok(());
        }

        let stream = Arc::try_unwrap(self.stream.clone())
            .map_err(|_| MQError::Custom("Failed to unwrap Arc".to_string()))?
            .into_inner();

        let (read_half, write_half) = stream.into_split();

        let mut r_builder = LengthDelimitedCodec::builder();
        let length_decoder_builder = r_builder
            .big_endian()
            .length_field_offset(0)
            .length_field_type::<u32>()
            .length_field_length(4);

        let mut w_builder = LengthDelimitedCodec::builder();
        let length_encoder_builder = w_builder
            .big_endian()
            .length_field_offset(0)
            .length_field_type::<u32>()
            .length_field_length(4);

        let mut framed_read = FramedRead::new(read_half, length_decoder_builder.new_codec());
        let mut framed_write = FramedRead::new(write_half, length_encoder_builder.new_codec());

        loop {
            match framed_read.next().await {
                Some(Ok(data)) => {
                    let event = decode_line_to_event(data)?;
                    self.handle_event(event).await;
                }
                Some(Err(e)) => {}
                None => break,
            }
        }

        Ok(())
    }

    async fn handle_event(&mut self, ev: Event) -> Result<(), MQError> {
        match ev {
            Event::PUB {
                topic,
                channel,
                msg,
            } => self.handle_pub(&topic, &channel, msg).await,
            Event::SUB { topic, channel } => self.handle_sub(&topic, &channel).await,
        }
    }

    async fn handle_sub(&mut self, topic_name: &str, channel_name: &str) -> Result<(), MQError> {
        let mut mq = self.mq.lock().await;
        mq.topic_add_client(self.id, topic_name, channel_name).await
    }

    async fn handle_pub(
        &mut self,
        topic: &str,
        channel: &str,
        msg: Vec<u8>,
    ) -> Result<(), MQError> {
        Ok(())
    }
}

pub async fn send_framed_response(
    w: &mut TcpStream,
    frame_type: FrameType,
    data: Vec<u8>,
) -> Result<usize, MQError> {
    let mut buf: Vec<u8> = vec![];
    let size = (data.len() + 4) as u32;
    buf.extend_from_slice(size.to_be_bytes().as_ref());

    let f_t = frame_type.0 as u32;
    buf.extend_from_slice(f_t.to_be_bytes().as_ref());

    buf.extend_from_slice(&data);

    w.write_all(&buf).await?;
    Ok(0)
}
