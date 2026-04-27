use std::sync::Arc;

use tokio::{
    net::TcpStream,
    sync::{mpsc, Mutex},
};

use crate::channel::Channel;

pub type ClientID = i64;

#[derive(Debug)]
pub struct Client {
    pub id: ClientID,
    pub name: String,
    pub address: String,
    pub stream: Arc<Mutex<TcpStream>>,
    // SubEventChan      chan *Channel
    pub sub_event_chan: mpsc::Receiver<Channel>,
}
