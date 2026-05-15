use std::path::PathBuf;

use mq_rs::{diskqueue::DiskQueue, errors::MQError};

const MAX_SIZE_BYTES: i64 = 1000 * 1000; // 1 Mega bytes

fn main() -> Result<(), MQError> {
    let data = PathBuf::from("./data");

    let mut queue = DiskQueue::new("topic-test2".to_string(), data, MAX_SIZE_BYTES);

    println!("enqueue: Hello Queue");
    queue.enqueue(b"Hello Queue".to_vec())?;

    println!("enqueue: Nice to see you");
    queue.enqueue(b"Nice to see you".to_vec())?;

    let item = queue.dequeue()?;
    println!("item content1: {}", String::from_utf8_lossy(&item));

    let item = queue.dequeue()?;
    println!("item content2: {}", String::from_utf8_lossy(&item));

    Ok(())
}
