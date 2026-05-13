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

    #[error("channel: {0} not found")]
    ChannelNotFound(String),

    #[error("topic: {0} exists")]
    TopicAlreadyExists(String),

    #[error("bad protocol")]
    BadProtocol,

    #[error("bad response: {0}")]
    BadResponse(String),

    #[error("disk queue send err: {0}")]
    DiskQueueError(String),
}
