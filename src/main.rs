use std::sync::Arc;

use mq_rs::mq::{start_queue, Options, MQ};
use tokio::sync::Mutex;

#[tokio::main]
async fn main() {
    let mq = MQ::new(Options::new());
    match mq {
        Ok(queue) => {
            let addr = queue.opts.tcp_addr.clone();
            let ato_mq = Arc::new(Mutex::new(queue));
            match start_queue(ato_mq, &addr).await {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("start queue failed: {}", e.to_string())
                }
            }
        }
        Err(e) => eprintln!("init mq failed: {}", e.to_string()),
    }
}
