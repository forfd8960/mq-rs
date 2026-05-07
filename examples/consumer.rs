use mq_rs::consumer::consume;
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let stream = TcpStream::connect("0.0.0.0:5050").await?;

    let mut rx = consume(stream, "orders", "analysis").await?;

    while let Some(msg) = rx.recv().await {
        println!("received msg: {:?}", msg);
    }

    Ok(())
}
