use std::fs::{File, OpenOptions};
use std::future::Future;
use std::path::PathBuf;

use memmap2::{MmapMut, MmapOptions};
use tokio::sync::RwLock;

use crate::errors::MQError;

pub type Item = Vec<u8>;

pub trait Queue {
    fn enque(&mut self, item: Item) -> impl Future<Output = Result<(), MQError>> + Send;
    fn deque(&mut self) -> impl Future<Output = Result<Item, MQError>> + Send;
}

#[derive(Debug)]
pub struct DiskQueue {
    name: String,
    read_pos: RwLock<i64>,
    write_pos: RwLock<i64>,
    read_file_num: i64,
    write_file_num: i64,
    max_bytes_per_file: i64,

    data_path: PathBuf,
    read_file: Option<File>,
    write_file: Option<File>,
    read_map: Option<MmapMut>,
    write_map: Option<MmapMut>,
}

impl DiskQueue {
    pub fn new(name: String, data_path: PathBuf, max_bytes_per_file: i64) -> Self {
        let dk = Self {
            name: name.clone(),
            read_pos: RwLock::new(0),
            write_pos: RwLock::new(0),
            read_file_num: 0,
            write_file_num: 0,
            max_bytes_per_file,
            data_path: data_path.clone(),
            read_file: None,
            write_file: None,
            read_map: None,
            write_map: None,
        };

        dk
    }

    fn file_name(&self, file_num: i64) -> PathBuf {
        self.data_path
            .join(format!("{}.diskqueue.{:06}.dat", self.name, file_num))
    }

    fn remap_write(&mut self) -> Result<(), MQError> {
        if self.write_map.is_some() {
            return Ok(());
        }

        println!("init write file");

        self.write_file = Some(self.open_file_rw(self.write_file_num)?);

        let f = self.write_file.as_ref().unwrap();

        println!("init mmap file");
        let mmap = unsafe {
            MmapOptions::new()
                .len(self.max_bytes_per_file as usize)
                .map_mut(f)?
        };
        self.write_map = Some(mmap);
        Ok(())
    }

    fn remap_read(&mut self) -> Result<(), MQError> {
        if self.read_map.is_some() {
            return Ok(());
        }

        self.read_file = Some(self.open_file_rw(self.read_file_num)?);
        let f = self.read_file.as_ref().unwrap();
        let map = unsafe {
            MmapOptions::new()
                .len(self.max_bytes_per_file as usize)
                .map_mut(f)?
        };
        self.read_map = Some(map);
        Ok(())
    }

    async fn rotate_write_file(&mut self) -> Result<(), MQError> {
        if let Some(map) = self.write_map.as_mut() {
            map.flush()?;
        }
        self.write_map = None;
        self.write_file = None;

        self.write_file_num += 1;
        {
             let mut write_pos = self.write_pos.write().await;
             *write_pos = 0;
        }
        self.remap_write()
    }

    async fn rotate_read_file(&mut self) -> Result<(), MQError> {
        
        if let Some(map) = self.read_map.as_mut() {
            map.flush()?;
        }
        self.read_map = None;
        self.read_file = None;
        self.read_file_num += 1;
        
        {
            let mut read_pos = self.read_pos.write().await;
             *read_pos = 0;
        }
        self.remap_read()
    }

    fn open_file_rw(&self, file_num: i64) -> Result<File, MQError> {
        let path = self.file_name(file_num);
        println!("file path: {:?}", path);

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .write(true)
            .open(path)?;

        println!("created file: {:?}", file);

        // Make file fixed-size for mmap
        file.set_len(self.max_bytes_per_file as u64)?;
        Ok(file)
    }

    pub async fn enque(&mut self, item: Item) -> Result<(), MQError> {
        self.remap_write()?;

        let item_len = item.len();

        let write_pos = {
             let pos = self.write_pos.read().await;
             *pos
        };

        if 4usize + write_pos as usize + item_len > self.max_bytes_per_file as usize {
            self.rotate_write_file().await?;
        }

        let mut write_pos1 = self.write_pos.write().await;
        let write_start = *write_pos1 as usize;

        let end_len = 4 + write_start;
        let end_payload = end_len + item_len;
        
        let w_map = self.write_map.as_mut().unwrap();
        let len_bs = (item_len as u32).to_be_bytes();
        w_map[write_start..end_len].copy_from_slice(&len_bs);
        w_map[end_len..end_payload].copy_from_slice(&item);
        
        let record_size = 4 + item_len;
        w_map.flush_range(write_start, record_size)?;
        
        *write_pos1 += record_size as i64;
        Ok(())
    }

    pub async fn deque(&mut self) -> Result<Item, MQError> {
        self.remap_read()?;

        let start = {
            let pos = self.read_pos.read().await;
            *pos
        } as usize;

        let write_pos = {
            let pos = self.write_pos.write().await;
            *pos
        } as usize;

        if self.read_file_num > self.write_file_num || start >= write_pos {
            return Err(MQError::DiskQueueError(
                "all items has been read".to_string(),
            ));
        }

        if self.read_file_num < self.write_file_num && start >= self.max_bytes_per_file as usize {
            self.rotate_read_file().await?;
        }

        let r_map = self.read_map.as_ref().unwrap();

        let mut len_bs = [0u8; 4];
        len_bs.copy_from_slice(&r_map[start..start + 4]);
        let item_len = u32::from_be_bytes(len_bs);
        let record_size = 4 + item_len;
        let end = start as u32 + record_size;
        
        let data = &r_map[start+4..end as usize].to_vec();
        {
            
            let mut read_pos = self.read_pos.write().await;
            *read_pos += record_size as i64;
        }

        Ok(data.clone())
    }

    pub fn close(&mut self) -> Result<(), MQError> {
        if let Some(map) = self.write_map.as_mut() {
            map.flush()?;
        }

        self.write_map = None;
        self.read_map = None;
        self.write_file = None;
        self.read_file = None;

        Ok(())
    }
}
