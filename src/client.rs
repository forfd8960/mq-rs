use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt, WriteHalf},
    net::{TcpStream, tcp::{self, OwnedWriteHalf}},
    sync::{Mutex, RwLock, mpsc},
    time,
};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

use crate::{
    channel::Channel,
    errors::MQError,
    message::{encode_msg, Message},
    mq::ArcMQ,
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
    pub mq: ArcMQ,
    pub sub_chan: Arc<RwLock<Option<Channel>>>,
    event_sender: mpsc::Sender<EventResp>,
    event_receiver: Arc<RwLock<mpsc::Receiver<EventResp>>>,
}

impl Client {
    pub fn new(client_id: u64, mq: ArcMQ) -> Self {
        let (tx, rx) = mpsc::channel::<EventResp>(1000);

        Self {
            id: client_id,
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

    pub async fn handle_conn(&mut self, tcp_stream: &mut TcpStream) -> Result<(), MQError> {
        let mut buf = vec![0u8; 4];
        let n = {
            tcp_stream.read(&mut buf).await?
        };

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

        self.message_pump().await;
        self.handle_stream(tcp_stream).await?;

        Ok(())
    }

    async fn handle_stream(&mut self, tcp_stream: &mut TcpStream) -> Result<(), MQError> {
        let (read_half, write_half) = io::split(tcp_stream);

        let (decoder, encoder) = build_r_w_codec();

        let mut framed_read = FramedRead::new(read_half, decoder);
        let mut framed_write = FramedWrite::new(write_half, encoder);

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
        writer: &mut FramedWrite<WriteHalf<&mut TcpStream>, LengthDelimitedCodec>,
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
        println!("handle sub: {}->{}", topic_name, channel_name);

        let mut mq = self.mq.write().await;
        let _ = mq
            .sub_channel(self.id, topic_name, channel_name)
            .await?;

        println!("add client: {} to {}", self.id, topic_name);
        Ok(EventResp::Msg(b"OK".to_vec()))
    }

    async fn handle_pub(&mut self, topic: &str, msg: Vec<u8>) -> Result<EventResp, MQError> {
        // 1. find the topic
        // 2. send the message to the topic message chan

        println!("handle pub: {}->{}", topic, String::from_utf8_lossy(&msg));

        let mq = self.mq.read().await;
        match mq.get_topic(topic) {
            Some(t) => {
                t.put_message(Message::new(msg));
                Ok(EventResp::Msg(b"OK".to_vec()))
            }
            None => return Err(MQError::TopicNotFound("topic: {} not found".to_string())),
        }
    }

    async fn message_pump(&self) {
        let chan_clone = self.sub_chan.clone();
        let ev_sender = self.event_sender.clone();

        tokio::spawn(async move {
            let mut tick = time::interval(Duration::from_millis(2000));
            tick.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

            loop {
                tick.tick().await; // waits for next 1s tick
                println!("message pump after 2s");

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
                                        let _ = ev_sender.send(EventResp::Msg(encode_msg(chan_msg))).await;
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
    writer: &mut FramedWrite<WriteHalf<&mut TcpStream>, LengthDelimitedCodec>,
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
