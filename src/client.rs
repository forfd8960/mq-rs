use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{tcp::OwnedWriteHalf, TcpStream},
    sync::{mpsc, Mutex, RwLock},
    time,
};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

use crate::{
    channel::Channel,
    errors::MQError,
    message::{encode_msg, Message},
    mq::MQ,
    protocol::{build_r_w_codec, decode_line_to_event, Event, FrameType},
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
    pub sub_chan: Arc<RwLock<Option<Channel>>>,
    event_sender: mpsc::Sender<EventResp>,
    event_receiver: Arc<RwLock<mpsc::Receiver<EventResp>>>,
}

impl Client {
    pub fn new(client_id: u64, tcp_stream: TcpStream, mq: Arc<Mutex<MQ>>) -> Self {
        let (tx, rx) = mpsc::channel::<EventResp>(1000);

        Self {
            id: client_id,
            stream: Arc::new(Mutex::new(tcp_stream)),
            mq,
            sub_chan: Arc::new(RwLock::new(None)),
            event_sender: tx,
            event_receiver: Arc::new(RwLock::new(rx)),
        }
    }

    pub async fn sub_to_chan(&mut self, chan: Channel) {
        let c_chan = self.sub_chan.read().await;
        if c_chan.is_some() {
            return;
        }

        let mut c_chan = self.sub_chan.write().await;
        *c_chan = Some(chan)
    }

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

        self.message_pump(self.event_sender.clone()).await;
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

        let recv_clone = self.event_receiver.clone();
        let mut event_recv = recv_clone.write().await;

        loop {
            tokio::select! {
                    event = event_recv.recv() => {
                        match event {
                            Some(ev) => {
                                self.send(&mut framed_write, ev);
                            }
                            None => {}
                        }
                    },

                    frame_data = framed_read.next() => {
                    match frame_data {
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

                    None => return Ok(()),
                    }
                }
            }
        }
    }

    async fn handle_event(&mut self, ev: Event) -> Result<EventResp, MQError> {
        match ev {
            Event::PUB { topic, msg } => self.handle_pub(&topic, msg).await,
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

    async fn handle_pub(&mut self, topic: &str, msg: Vec<u8>) -> Result<EventResp, MQError> {
        // 1. find the topic
        // 2. send the message to the topic message chan

        let mq = self.mq.lock().await;
        match mq.get_topic(topic) {
            Some(t) => {
                t.put_message(Message::new(msg));
                Ok(EventResp::Msg(vec![]))
            }
            None => return Err(MQError::TopicNotFound("topic: {} not found".to_string())),
        }
    }

    async fn message_pump(&mut self, event_sender: mpsc::Sender<EventResp>) {
        let chan_clone = self.sub_chan.clone();

        tokio::spawn(async move {
            let mut tick = time::interval(Duration::from_millis(500));
            tick.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

            loop {
                tick.tick().await; // waits for next 1s tick
                let mut sub_chan = chan_clone.write().await;
                if sub_chan.is_none() {
                    continue;
                }

                let chan = sub_chan.as_mut();
                match chan {
                    Some(ch) => {
                        tokio::select! {
                            chan_msg_res = ch.memory_msg_chan.recv() => {
                                match chan_msg_res {
                                    Ok(chan_msg) => {
                                        println!("received msg: {:?}", chan_msg);
                                        event_sender.send(EventResp::Msg(encode_msg(chan_msg))).await;
                                    },
                                    Err(e) => {
                                        eprintln!("recv failed: {}", e);
                                    }
                                }
                            }
                        }
                    }
                    None => {}
                }
            }
        });
    }
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
