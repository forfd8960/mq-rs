use std::sync::Arc;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{mpsc, Mutex},
};

use crate::{channel::Channel, errors::MQError, protocol::FrameType};

pub type ClientID = u64;

#[derive(Debug)]
pub struct Client {
    pub id: ClientID,
    pub stream: Arc<Mutex<TcpStream>>,
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

        if buf != b"MQV0" {
            let mut s = self.stream.lock().await;
            send_framed_response(&mut s, FrameType(1), b"bad protocol".to_vec()).await?;
            return Ok(());
        }

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
