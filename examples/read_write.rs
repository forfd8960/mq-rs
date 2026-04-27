use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

#[derive(Clone)]
struct Client {
    // Shared reference to TcpStream so it can be used across async calls safely.
    stream: Arc<Mutex<TcpStream>>,
}

impl Client {
    fn new(stream: TcpStream) -> Self {
        Self {
            stream: Arc::new(Mutex::new(stream)),
        }
    }

    async fn handle_loop(&self) -> tokio::io::Result<()> {
        let mut buf = vec![0u8; 1024];

        loop {
            // 1) Read a message from tcp stream
            let n = {
                let mut stream = self.stream.lock().await;
                stream.read(&mut buf).await?
            };

            if n == 0 {
                println!("client disconnected");
                return Ok(());
            }

            let msg = String::from_utf8_lossy(&buf[..n]);
            println!("received: {}", msg.trim_end());

            // 2) Send a heartbeat message to tcp stream
            {
                let mut stream = self.stream.lock().await;
                stream
                    .write_all(format!("echo: {:?}\n", msg).as_bytes())
                    .await?;
            }
        }
    }
}

#[tokio::main]
async fn main() -> tokio::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    println!("server listening on 127.0.0.1:8080");

    loop {
        // accept a new connection
        let (stream, addr) = listener.accept().await?;
        println!("accepted connection from {}", addr);

        // init Client struct with tcp stream
        let client = Client::new(stream);

        // spawn a task to handle this client loop
        tokio::spawn(async move {
            if let Err(e) = client.handle_loop().await {
                eprintln!("client {} error: {}", addr, e);
            }
        });
    }
}
