use futures::{SinkExt, StreamExt};
use tokio::io::{self, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

use crate::errors::MQError;
use crate::message::{decode_message, Message};
use crate::protocol::{build_r_w_codec, decode_server_frame, encode_sub, FrameType};

const MAGIC: &[u8; 4] = b"RQV0";

pub async fn consume(
    stream: TcpStream,
    topic: &str,
    channel: &str,
) -> Result<mpsc::Receiver<Message>, MQError> {
    // 1) write MAGIC first
    let mut stream = stream;
    stream.write_all(MAGIC).await?;
    stream.flush().await?;

    // 2) split stream into read/write halves
    let (read_half, write_half) = io::split(stream);

    // 3) setup length-delimited decoder/encoder
    let (read_codec, write_codec) = build_r_w_codec();

    let mut reader = FramedRead::new(read_half, read_codec);
    let mut writer = FramedWrite::new(write_half, write_codec);

    // 4) send SUB command via encoder
    let sub_cmd = encode_sub(topic, channel); // e.g. SUB orders analysis\n
    writer.send(sub_cmd.freeze()).await?;

    let (tx, rx) = mpsc::channel::<Message>(1000);

    tokio::spawn(async move {
        // 5) receive loop and decode `<size><frame_type><data>`
        match read_loop(tx, &mut reader).await {
            Ok(_) => {}
            Err(e) => {
                eprintln!("read message err: {}", e);
            }
        }
    });

    Ok(rx)
}

async fn read_loop(
    tx: mpsc::Sender<Message>,
    reader: &mut FramedRead<ReadHalf<TcpStream>, LengthDelimitedCodec>,
) -> Result<(), MQError> {
    while let Some(frame) = reader.next().await {
        let payload = frame.map_err(MQError::IOError)?; // BytesMut
        let (frame_type, data) = decode_server_frame(payload)?;

        match frame_type {
            FrameType::Message => {
                // optional: decode into Message (depends on server message format)
                match decode_message(&data) {
                    Ok(msg) => {
                        println!(
                            "MSG id={} ts={} attempts={} body_len={}",
                            msg.id,
                            msg.ts,
                            msg.attempts,
                            msg.body.len()
                        );
                        let _ = tx.send(msg).await;
                    }
                    Err(_) => {
                        // If your data is not Message format, keep raw handling:
                        println!("MSG raw len={}", data.len());
                    }
                }
            }
            FrameType::Response => {
                println!("OK: {}", String::from_utf8_lossy(&data));
            }
            FrameType::Error => {
                return Err(MQError::BadResponse(format!(
                    "server error: {}",
                    String::from_utf8_lossy(&data)
                )));
            }
        }
    }

    Ok(())
}
