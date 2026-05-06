use std::sync::Arc;

use mq_rs::{
    http_server::start_http,
    mq::{start_queue, Options, MQ},
};
use tokio::sync::{RwLock};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mq_res = MQ::new(Options::new());

    let (arc_mq, addr) = match mq_res {
        Ok(queue) => {
            let addr = queue.opts.tcp_addr.clone();
            (Arc::new(RwLock::new(queue)), addr)
        }
        Err(e) => {
            eprintln!("init mq failed: {}", e.to_string());
            return Ok(());
        }
    };

    let mq1 = arc_mq.clone();
    let mq2 = arc_mq.clone();

    let tcp_task = tokio::spawn(async move {
        match start_queue(mq1, &addr).await {
            Ok(_) => {}
            Err(e) => {
                eprintln!("start queue failed: {}", e.to_string())
            }
        }
    });

    let http_task = tokio::spawn(async { start_http(mq2).await });

    // Wait for both (if one exits with error, return it)
    let _ = tokio::try_join!(tcp_task, http_task)?;
    Ok(())
}
