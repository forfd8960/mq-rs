use thiserror::Error;
use tokio::io;

#[derive(Debug, Error)]
pub enum MQError {
    #[error("io error")]
    IOError(#[from] io::Error),

    #[error("internal err: {0}")]
    Custom(String),

    #[error("unknown event: {0}")]
    UnknowEvent(String),

    #[error("invalid event: {0}")]
    BadEventPayload(String),

    #[error("topic: {0} not found")]
    TopicNotFound(String),

    #[error("topic: {0} exists")]
    TopicAlreadyExists(String),

    #[error("bad protocol")]
    BadProtocol,
}
