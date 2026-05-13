use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use std::time::Duration;
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt, WriteHalf},
    net::TcpStream,
    sync::mpsc,
};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

use crate::{
    errors::MQError,
    message::{encode_msg, Message},
    mq::ArcMQ,
    protocol::{build_r_w_codec, decode_line_to_event, Event, FrameType},
};

pub type ClientID = u64;
const MAGIC: &'static str = "RQV0";

#[derive(Debug)]
pub enum EventResp {
    Err(MQError),
    Msg(Message),
    Resp(Vec<u8>),
}

#[derive(Debug, Clone)]
pub struct Client {
    pub id: ClientID,
    pub mq: ArcMQ,
    pub topic: Option<String>,
    pub chan: Option<String>,
}

impl Client {
    pub fn new(client_id: u64, mq: ArcMQ) -> Self {
        Self {
            id: client_id,
            mq,
            topic: None,
            chan: None,
        }
    }

    pub async fn handle_conn(&mut self, tcp_stream: &mut TcpStream) -> Result<(), MQError> {
        let mut buf = vec![0u8; 4];
        let n = { tcp_stream.read(&mut buf).await? };

        if n == 0 {
            println!("client disconnected");
            return Ok(());
        }

        if buf != MAGIC.as_bytes() {
            {
                tcp_stream
                    .write(encode_error(MQError::BadProtocol).as_ref())
                    .await?;
            }
            return Ok(());
        }

        self.handle_stream(tcp_stream).await?;
        Ok(())
    }

    async fn handle_stream(&mut self, tcp_stream: &mut TcpStream) -> Result<(), MQError> {
        let (read_half, write_half) = io::split(tcp_stream);

        let (decoder, encoder) = build_r_w_codec();

        let mut framed_read = FramedRead::new(read_half, decoder);
        let mut framed_write = FramedWrite::new(write_half, encoder);

        let (msg_sender, mut msg_recv) = mpsc::channel::<EventResp>(1000);

        loop {
            tokio::select! {
                    event = msg_recv.recv() => {
                        match event {
                            Some(ev) => {
                                println!("received event: {:?}", ev);
                                self.send(&mut framed_write, &ev).await;
                                self.start_in_flight(ev).await
                            }
                            None => {}
                        }
                    },

                    frame_data = framed_read.next() => {
                    match frame_data {
                        Some(Ok(data)) => {
                                let event = decode_line_to_event(data)?;
                                let resp = self.handle_event(event, msg_sender.clone()).await;
                                match resp {
                                    Ok(r) => {
                                        self.send(&mut framed_write, &r).await;
                                    }
                                    Err(e) => {
                                        self.send(&mut framed_write, &EventResp::Err(e)).await;
                                    }
                                }
                        }

                        Some(Err(e)) => {
                            self.send(&mut framed_write, &EventResp::Err(MQError::IOError(e))).await;
                        }

                        None => return Ok(()),
                    }
                }
            }
        }
    }

    async fn handle_event(
        &mut self,
        ev: Event,
        msg_sender: mpsc::Sender<EventResp>,
    ) -> Result<EventResp, MQError> {
        match ev {
            Event::PUB { topic, msg } => self.handle_pub(&topic, msg).await,
            Event::SUB { topic, channel } => self.handle_sub(&topic, &channel, msg_sender).await,
        }
    }

    async fn send(
        &self,
        writer: &mut FramedWrite<WriteHalf<&mut TcpStream>, LengthDelimitedCodec>,
        event_resp: &EventResp,
    ) {
        match event_resp {
            EventResp::Err(e) => {
                send_framed_response(writer, FrameType::Error, e.to_string().as_bytes().to_vec())
                    .await
            }
            EventResp::Msg(msg) => {
                send_framed_response(writer, FrameType::Message, encode_msg(msg.clone())).await;
            }

             EventResp::Resp(resp) => {
                send_framed_response(writer, FrameType::Response, resp.to_vec()).await;
            }
        }
    }

    async fn handle_sub(
        &mut self,
        topic_name: &str,
        channel_name: &str,
        msg_sender: mpsc::Sender<EventResp>,
    ) -> Result<EventResp, MQError> {
        println!("handle sub: {}->{}", topic_name, channel_name);

        let mut mq = self.mq.write().await;
        let _ = mq.sub_channel(self.id, topic_name, channel_name).await?;
        drop(mq);

        self.topic = Some(topic_name.to_string());
        self.chan = Some(channel_name.to_string());
        self.message_pump1(msg_sender).await;

        println!("add client: {} to {}", self.id, topic_name);
        Ok(EventResp::Msg(Message::new(b"OK".to_vec())))
    }

    async fn handle_pub(&mut self, topic: &str, msg: Vec<u8>) -> Result<EventResp, MQError> {
        // 1. find the topic
        // 2. send the message to the topic message chan

        println!(
            "handle pub, topic: {}, message: {}",
            topic,
            String::from_utf8_lossy(&msg)
        );

        let mq = self.mq.read().await;
        match mq.get_topic(topic) {
            Some(t) => {
                println!("put message: {:?} to topic msg channel", msg);
                t.put_message(Message::new(msg));
                Ok(EventResp::Msg(Message::new(b"OK".to_vec())))
            }
            None => return Err(MQError::TopicNotFound("topic: {} not found".to_string())),
        }
    }

    async fn message_pump1(&self, tx: mpsc::Sender<EventResp>) {
        let topic_name = match self.topic.clone() {
            Some(topic) => topic,
            None => return,
        };

        let channel = match self.chan.clone() {
            Some(chan) => chan,
            None => return,
        };

        let mq_clone = self.mq.clone();
        let mq_read = mq_clone.read().await;
        let channel_receiver = match mq_read.get_topic(&topic_name) {
            Some(topic) => topic.get_channel_receiver(&channel),
            None => return,
        };
        let chan_receiver = channel_receiver.await;
        drop(mq_read);
        if chan_receiver.is_none() {
            return;
        }

        let mut channel_msg_receiver = chan_receiver.unwrap();

        tokio::spawn(async move {
            loop {
                match channel_msg_receiver.recv().await {
                    Ok(chan_msg) => {
                        println!("received msg: {:?}", chan_msg);
                        let _ = tx.send(EventResp::Msg(chan_msg)).await;
                    }
                    Err(e) => {
                        eprintln!("recv failed: {}", e);
                        break;
                    }
                }
            }
        });
    }

    async fn start_in_flight(&self, ev: EventResp) {
        if self.chan.is_none() {
            return;
        }

        let mut msg = match ev {
            EventResp::Msg(msg) => msg,
            _ => return,
        };

        let t = self.topic.clone();
        let ch = self.chan.clone();
        let ch_name = &ch.unwrap();

        let mq = self.mq.write().await;
        match mq.get_topic(&t.unwrap()) {
            Some(topic) => {
                let _ = topic
                    .start_in_flight(ch_name, &mut msg, self.id, Duration::from_secs(3600))
                    .await;
            }
            None => return,
        };
    }
}

pub async fn send_framed_response(
    writer: &mut FramedWrite<WriteHalf<&mut TcpStream>, LengthDelimitedCodec>,
    frame_type: FrameType,
    data: Vec<u8>,
) {
    let mut buf = vec![];
    let f_t: u32 = frame_type.into();
    buf.extend_from_slice(f_t.to_be_bytes().as_ref());
    buf.extend_from_slice(&data);

    let _ = writer.send(Bytes::from(buf)).await;
    let _ = writer.flush().await;
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
