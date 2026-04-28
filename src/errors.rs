use thiserror::Error;
use tokio::io;

#[derive(Debug, Error)]
pub enum MQError {
    #[error("io error")]
    IOError(#[from] io::Error),

    #[error("internal err: {0}")]
    Custom(String),

    #[error("unknown event: {0}")]
    UnknowEvent(String)
}
