use crate::page;
use crate::metadata;
use crate::error::Error;

use std::collections::hash_map::Keys;
use std::fs::{OpenOptions, File};
use std::path::Path;
use std::io::{Read, Seek, Write, SeekFrom};


#[derive(Debug)]
pub struct Storage {
  name: String,
  data_file: File,
  metadata: metadata::Metadata,
  cache: Vec<page::Page>,
  free: Vec<metadata::Location>
}

impl Drop for Storage {
  fn drop(&mut self) {
      if self.save().is_err() {
        println!("[!] Failed saving storage.")
      }
  }
}

impl Storage {
  pub fn new(namespace: &String) -> Result<Storage, Error> {
    let data_file =  OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(format!("data/{namespace}.data"))
        .map_err(|_| Error::NamespaceNotFound)?;
    
    // check if metadata file exist
    let mut metadata = metadata::Metadata::new();
    let meta_name = format!("data/{namespace}.meta");
    let meta_path = Path::new(&meta_name);

    if meta_path.exists() {
      metadata = metadata::Metadata::load_meta(meta_path)?;
    }

    Ok(Storage {
      name: namespace.clone(),
      data_file,
      metadata,
      cache: vec!(),
      free: vec!(),
    })
  }

  pub fn save(&mut self) -> Result<(), Error> {
    let meta_name = format!("data/{}.meta", &self.name);
    let meta_path = Path::new(&meta_name);
    self.metadata.save_meta(meta_path)?;

    Ok(())
  }
  
  pub fn cache_page(&mut self, page_offset: u64) -> Result<&page::Page, Error> {
    let mut page = page::Page::new();
    let file_metadata = self.data_file.metadata().map_err(|_| Error::CacheReadFailed)?;

    // check offset
    if page_offset >= file_metadata.len() {
      return Err(Error::FileTooSmall);
    }

    // Read page
    if self.data_file.seek(std::io::SeekFrom::Start(page_offset)).is_err() {
      return Err(Error::LoadPageFail);
    }

    if self.data_file.read(&mut page.data).is_err() {
      return Err(Error::LoadPageFail);
    }

    // Set proper offset
    page.offset = page_offset;

    self.cache.push(page);
    self.cache.last().ok_or(Error::CacheEmpty)
  }

  pub fn read(&mut self, key: &String) -> Result<Vec<u8>, Error> {
    // find key in metadata and retrieve offset + length
    let item_location = match self.metadata.get_item_location(key) {
      Some(location) => location,
      None => return Err(Error::KeyNotFound) 
    };

    // Check if page is already in cache
    // if not found, store value in cache 
    let page = match self.cache.iter().find(|page| page.offset == item_location.get_page_offset()) {
      Some(v) => v,
      None => {
        match self.cache_page(item_location.get_page_offset()) {
          Ok(p) => p,
          Err(_) => {
            return Err(Error::LoadPageFail)
          }
        }
      }
    };

    // Read from cache
    page.read(&item_location)
  }

  pub fn write(&mut self, key: &String, value: &Vec<u8>) -> Result<(), Error> {
    // Adjust the size of the value to the next power of 2
    let block_size = value.len().next_power_of_two() as u64;

    // Check the data correctness
    if block_size > page::PAGE_SIZE {
      return Err(Error::ValueToLarge);
    }

    // Check if key already exist - probably not needed
    // if self.metadata.has_key(key) {
    //   self.delete(&key);
    // }

    let mut current_cursor = self.metadata.get_write_cursor();
    let next_page_addr = (current_cursor + page::PAGE_SIZE) & !(page::PAGE_SIZE - 1);

    // Check if the current page has enough space for value
    if (next_page_addr - current_cursor) < block_size  {
      // not enough space in this page - record it as free block
      let block = metadata::Location::new(
        current_cursor,
        next_page_addr - current_cursor
      );
      self.free.push(block);
      
      // move cursor to another page
      current_cursor = next_page_addr;

    }
    
    // create metadata entry first
    let location = metadata::Location::new(current_cursor, value.len() as u64);
    self.metadata.set_item_location(key, location);

    // Actually write data
    self.data_file.seek(SeekFrom::Start(current_cursor))
        .map_err(|_| Error::DataFileSeek)?;


    if self.data_file.write_all(value).is_err() {
      return Err(Error::DataFileWrite);
    }

    // Update cursor
    self.metadata.update_write_cursor(current_cursor + block_size);

    // selective cache invalidation
    self.cache.retain(|p| p.offset != location.get_page_offset());

    Ok(())
  }

  pub fn delete(&mut self, key: &String) -> Result<(), Error> {
    // Record data about the free block
  
    match self.metadata.remove(key) {
      Some(block) => {
        self.free.push(block);
        Ok(())
      },
      None => Err(Error::KeyNotFound)
    }
  }

  pub fn list_keys(&self) -> Keys<String, metadata::Location> {
    self.metadata.get_keys()
  }

  /// Status functions
  pub fn get_item_location(&self, key: &String) -> Option<metadata::Location> {
    self.metadata.get_item_location(key)
  }

  pub fn get_cache(&self) -> &Vec<page::Page> {
    &self.cache
  }
  pub fn get_free(&self) -> &Vec<metadata::Location> {
    &self.free
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn next_page_address() {
    let next_page_addr = |x: u64|  (x + page::PAGE_SIZE) & !(page::PAGE_SIZE - 1);

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