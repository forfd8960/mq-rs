use std::sync::Arc;

use futures::StreamExt;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{mpsc, Mutex},
};
use tokio_util::codec::{FramedRead, LinesCodec};

use crate::{channel::Channel, errors::MQError, mq::MQ, protocol::{Event, FrameType}};

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
    pub async fn handle_conn(&self) -> Result<(), MQError> {
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

        let mut framed_read = FramedRead::new(read_half, LinesCodec::new());
        let mut framed_write = FramedRead::new(write_half, LinesCodec::new());

        loop {
            match framed_read.next().await {
                Some(Ok(data)) => {
                    
                },
                Some(Err(e)) => {},
                None => break
            }
        }


        Ok(())
    }

    async fn handle_cmd(&mut self, ev: Event) -> Result<(), MQError> {
        match ev {
            Event::PUB { topic, channel, msg } => todo!(),
            Event::SUB { topic, channel } => todo!(),
        }
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
