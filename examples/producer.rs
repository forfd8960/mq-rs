use std::time::Duration;

use mq_rs::{producer::Producer, protocol::build_r_w_codec};
use tokio::{io, net::TcpStream, time::sleep};
use tokio_util::codec::{FramedRead, FramedWrite};
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, WriteHalf},
    net::{tcp::{self, OwnedWriteHalf}},
    sync::{Mutex, RwLock, mpsc},
    time,
};
use tokio_util::codec::{LengthDelimitedCodec};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut stream = TcpStream::connect("0.0.0.0:5050").await?;

    let mut producer = Producer::new("0.0.0.0:5050");
    producer.init_client(&mut stream).await?;

    // 2) split stream into read/write halves
    let (read_half, write_half) = io::split(stream);

    // 3) setup length-delimited decoder/encoder
    let (read_codec, write_codec) = build_r_w_codec();

    let mut reader = FramedRead::new(read_half, read_codec);
    let mut writer = FramedWrite::new(write_half, write_codec);

    producer
        .pub_msg(&mut writer, "orders", b"Hello".to_vec())
        .await?;

    println!("success pub msg to: {}", "orders");
    
    while let Some(recv_data) = reader.next().await {
        match recv_data {
            Ok(dd) => println!("received data: {}", String::from_utf8_lossy(dd.as_ref())),
            Err(e) => eprintln!("recv data error: {}", e)
        }
    }

    sleep(Duration::from_secs(1)).await;


    Ok(())
}
