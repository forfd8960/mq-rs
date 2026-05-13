/*
// diskQueue implements a filesystem backed FIFO queue
type diskQueue struct {
    // 64bit atomic vars need to be first for proper alignment on 32bit platforms

    // run-time state (also persisted to disk)
    readPos      int64
    writePos     int64
    readFileNum  int64
    writeFileNum int64
    depth        int64
*/

use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::{Arc};

use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::oneshot;
use tokio::{
    fs::File,
    sync::{mpsc, RwLock, Mutex},
};

use crate::errors::MQError;

pub type Item = Vec<u8>;

#[derive(Debug, Default)]
struct WriteState {
    write_pos: i64,
    write_file_num: i64,
    depth: i64,
}

#[derive(Debug)]
pub struct DiskQueue {
    name: String,
    read_pos: RwLock<i64>,
    read_file_num: i64,
    max_bytes_per_file: i64,

    data_path: PathBuf,
    write_chan: mpsc::UnboundedSender<Item>,
    write_state: Arc<Mutex<WriteState>>,
    exit_chan_tx: Option<oneshot::Sender<()>>,
}

impl DiskQueue {
    pub fn new(name: String, data_path: PathBuf, max_bytes_per_file: i64) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let write_state = Arc::new(Mutex::new(WriteState::default()));
        
        let (exit_tx, exit_rx) = oneshot::channel::<()>();
        
        let dk = Self {
            name: name.clone(),
            read_pos: RwLock::new(0),
            read_file_num: 0,
            max_bytes_per_file,
            data_path: data_path.clone(),
            write_chan: tx,
            write_state: Arc::clone(&write_state),
            exit_chan_tx: Some(exit_tx),
        };

        tokio::spawn(ioloop(name, data_path, max_bytes_per_file, write_state, rx, exit_rx));
        dk
    }

    pub fn put(&self, item: Item) -> Result<(), MQError> {
        self.write_chan
            .send(item)
            .map_err(|e| MQError::DiskQueueError(e.to_string()))
    }

    pub fn file_name(&self) -> PathBuf {
        self.data_path.join(format!(
            "{}.diskqueue.{:06}.dat",
            self.name, self.read_file_num
        ))
    }

    pub async fn write_file_name(&self) -> PathBuf {
        let write_file_num = self.write_state.lock().await.write_file_num;
        self.data_path
            .join(format!("{}.diskqueue.{:06}.dat", self.name, write_file_num))
    }

    pub async fn peek(&mut self) -> Result<Item, MQError> {
        let mut read_pos = self.read_pos.write().await;

        let mut f = File::open(self.file_name()).await?;
        let item = read_frame_at(&mut f, *read_pos as u64).await?;

        *read_pos += (4 + item.len()) as i64;
        Ok(item)
    }

    pub fn close(&mut self) {
        if let Some(tx) = self.exit_chan_tx.take() {
            let _ = tx.send(());
        }
    }
}

async fn open_write_file(name: &str, data_path: &PathBuf, file_num: i64) -> Result<File, MQError> {
    let path = data_path.join(format!("{}.diskqueue.{:06}.dat", name, file_num));
    let f = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await?;
    Ok(f)
}

async fn ioloop(
    name: String,
    data_path: PathBuf,
    max_bytes_per_file: i64,
    write_state: Arc<Mutex<WriteState>>,
    mut rx: mpsc::UnboundedReceiver<Item>,
    mut exit_rx: oneshot::Receiver<()>,
) -> Result<(), MQError> {
    // Open (or create) the initial write file.
    let initial_file_num = write_state.lock().await.write_file_num;
    let mut f = open_write_file(&name, &data_path, initial_file_num).await?;

    loop {
        tokio::select! {
            _ = &mut exit_rx => {
                break;
            }

            msg = rx.recv() => {
                match msg {
                    None => continue,
                    Some(v) => {
                        let frame_len = (4 + v.len()) as i64;

                        f.write_all(&(v.len() as u32).to_be_bytes()).await?;
                        f.write_all(&v).await?;

                        // Update shared state; decide if we need to roll the file.
                        // The guard must be dropped before any `.await`.
                        let next_file_num = {
                            let mut state = write_state.lock().await;
                            state.write_pos += frame_len;
                            state.depth += 1;

                            if state.write_pos >= max_bytes_per_file {
                                state.write_file_num += 1;
                                state.write_pos = 0;
                                Some(state.write_file_num)
                            } else {
                                None
                            }
                        }; // MutexGuard released here

                        if let Some(num) = next_file_num {
                            f = open_write_file(&name, &data_path, num).await?;
                        }
                    }
                }
            },
        }
    }

    Ok(())
}

async fn read_frame_at(file: &mut File, pos: u64) -> Result<Vec<u8>, MQError> {
    // Go to the requested position
    file.seek(SeekFrom::Start(pos)).await?;

    // Read 4-byte big-endian length
    let mut len_buf = [0u8; 4];
    file.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;

    // Read body
    let mut body = vec![0u8; len];
    file.read_exact(&mut body).await?;

    Ok(body)
}
