use thiserror::Error;
use tokio::io;

#[derive(Debug, Error)]
pub enum MQError {
    #[error("io error")]
    IOError(#[from] io::Error),
}
