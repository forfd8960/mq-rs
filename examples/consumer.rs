use std::env;

use mq_rs::consumer::consume;
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
    let mut rx = consume(stream, topic, channel).await?;

    while let Some(msg) = rx.recv().await {
        println!("received msg: {:?}", msg);
    }

    Ok(())
}
