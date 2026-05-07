use futures::SinkExt;
use tokio::io::{AsyncWriteExt, WriteHalf};
use tokio::net::TcpStream;
use tokio_util::codec::{FramedWrite, LengthDelimitedCodec};

use crate::errors::MQError;
use crate::protocol::encode_pub;

const MAGIC: &[u8; 4] = b"RQV0";

pub struct Producer<'a> {
    pub addr: &'a str,
}

impl<'a> Producer<'a> {
    pub fn new(addr: &'a str) -> Self {
        Self { addr }
    }

    pub async fn init_client(&mut self, stream: &mut TcpStream) -> Result<(), MQError> {
        stream.write_all(MAGIC).await?;
        stream.flush().await?;
        Ok(())
    }

    pub async fn pub_msg(
        &mut self,
        writer: &mut FramedWrite<WriteHalf<TcpStream>, LengthDelimitedCodec>,
        topic: &str,
        msg: Vec<u8>,
    ) -> Result<(), MQError> {
        let pub_cmd = encode_pub(topic, &msg);
        writer.send(pub_cmd.freeze()).await?;
        Ok(())
    }
}
