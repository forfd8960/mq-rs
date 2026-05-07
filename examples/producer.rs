use std::env;
use std::time::Duration;

use futures::StreamExt;
use mq_rs::protocol::{decode_server_frame, display_data};
use mq_rs::{producer::Producer, protocol::build_r_w_codec};
use tokio::{io, net::TcpStream, time::sleep};
use tokio_util::codec::{FramedRead, FramedWrite};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!("usage: cargo run --example producer -- <topic> <message>");
        return Err(anyhow::anyhow!("missing args: expected <topic> <message>"));
    }

    let topic = &args[1];
    let message = args[2..].join(" ");

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
        .pub_msg(&mut writer, topic, message.as_bytes().to_vec())
        .await?;

    println!("success pub msg to: {}", topic);

    if let Some(recv_data) = reader.next().await {
        match recv_data {
            Ok(dd) => {
                let frame = decode_server_frame(dd);

                match frame {
                    Ok((f_type, msg_data)) => display_data(f_type, msg_data),
                    Err(e) => eprintln!("decode frame err: {}", e),
                }
            }
            Err(e) => eprintln!("recv data error: {}", e),
        }
    }

    sleep(Duration::from_millis(200)).await;

    Ok(())
}
