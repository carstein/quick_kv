use std::fs::{OpenOptions, File};
use std::path::Path;
use std::io::{Read, Seek, Write, SeekFrom};

use crate::page::{Page, PAGE_SIZE};
use crate::page;
use crate::metadata::{Metadata, Location};
use crate::error::Error;

const NUM_PAGES: u64 = 32;

#[derive(Debug)]
pub struct Storage {
  data_file: File,
  metadata: Metadata,
  cache: Vec<Page>,
}

impl Storage {
  pub fn new(namespace: &String) -> Storage {
    let data_file =  OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(format!("{namespace}.data"))
        .expect("Failed to open provided namespace");
    
    // check if metadata file exist
    let mut metadata = Metadata::new();
    let meta_name = format!("{namespace}.meta");
    let meta_path = Path::new(&meta_name);

    if meta_path.exists() {
      metadata = Metadata::load_meta(&meta_path);  
    }

    Storage {
      data_file,
      metadata: metadata,
      cache: vec!(),
    }
  }
  
  pub fn load_page(&mut self, page_offset: u64) -> Result<Page, Error> {
    let mut page = Page::new();
    let metadata = self.data_file.metadata().unwrap();

    // check offset
    if page_offset >= (metadata.len() - page::PAGE_SIZE) {
      return Err(Error::FileTooSmall);
    }

    // Read page (TODO: handle unwraps)
    self.data_file.seek(std::io::SeekFrom::Start(page_offset)).unwrap();
    self.data_file.read(&mut page.data).unwrap();

    Ok(page)
  }

  pub fn read(&mut self, key: &String) -> Result<Vec<u8>, Error> {
    // find key in metadata and retrieve offset + length
    let item_location = match self.metadata.get_item_location(key) {
      Some(offset) => offset,
      None => return Err(Error::KeyNotFound) 
    };

    match self.data_file.seek(SeekFrom::Start(item_location.get_offset())) {
      Ok(_) => {}
      Err(_) => return Err(Error::DataFileSeek)
    };

    let mut buf = vec![0u8; item_location.len()as usize];
    match self.data_file.read_exact(&mut buf) {
      Ok(_) => {}
      Err(_) => return Err(Error::DataFileWrite)
    };

    return Ok(buf)
  }

  pub fn write(&mut self, key: &String, value: &Vec<u8>) -> Result<(), Error> {
    // Check the data correctness
    if value.len() > PAGE_SIZE as usize {
      return Err(Error::ValueToLarge);
    }

    let mut current_cursor = self.metadata.get_write_cursor();
    let next_page_addr = (current_cursor + PAGE_SIZE) & !(PAGE_SIZE - 1);

    // Check if the current page has enough space for value
    if (next_page_addr - current_cursor) < value.len() as u64  {
      // not enough space in this page - move cursor to another page
      current_cursor = next_page_addr;
    }
    
    // create metadata entry first
    let location: Location = Location::new(current_cursor, value.len() as u64);
    self.metadata.set_item_location(key, location);

    // Actually write data
    // TODO: add error handling
    match self.data_file.seek(SeekFrom::Start(current_cursor)) {
      Ok(_) => {}
      Err(_) => return Err(Error::DataFileSeek)
    };

    match self.data_file.write_all(value) {
      Ok(_) => {}
      Err(_) => return Err(Error::DataFileWrite)
    };

    // Update cursor
    self.metadata.update_write_cursor(current_cursor + value.len() as u64);

    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn next_page_address() {
    let next_page_addr = |x: u64|  (x + PAGE_SIZE) & !(PAGE_SIZE - 1);

    let vals = [
      (0,1024),
      (1521, 2048),
      (1024, 2048),
    ];

    for(offset, address) in vals {
      assert_eq!(next_page_addr(offset), address);
    }
  }
}