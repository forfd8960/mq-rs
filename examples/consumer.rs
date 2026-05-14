use std::env;

use mq_rs::{
    consumer::commnunicate,
    protocol::{encode_fin, encode_sub},
};
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!("usage: cargo run --example consumer -- <topic> <channel>");
        return Err(anyhow::anyhow!("missing args: expected <topic> <channel>"));
    }

    let topic = &args[1];
    let channel = &args[2];

    let stream = TcpStream::connect("0.0.0.0:5050").await?;
    let (mut rx, cmd_sender) = commnunicate(stream).await?;

    let _ = cmd_sender.send(encode_sub(topic, channel)).await;
    let _ = cmd_sender.send(encode_fin("msg-not-exists")).await;
    while let Some(msg) = rx.recv().await {
        println!("received msg: {:?}", msg);
        let _ = cmd_sender.send(encode_fin(&msg.id.to_string())).await;
    }

    Ok(())
}
