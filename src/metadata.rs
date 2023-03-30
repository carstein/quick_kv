use bincode;

use std::collections::{HashMap, hash_map::Keys};
use std::{path::Path, fs::File, io::Write};
use serde::{Serialize, Deserialize};

use crate::error;

const PAGE_SIZE: u64 = 1024;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Location {
  offset: u64,
  pub length: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Metadata {
  index: HashMap<String, Location>,
  pub write_cursor: u64,
}

impl Location {
  pub fn new(offset:u64, length:u64) -> Location {
    Location {
      offset,
      length
    }
  }

  pub fn get_offset(&self) -> u64 {
    self.offset
  }

  pub fn get_page_offset(self) -> u64 {
    // zero out last 10 bits
    self.offset & !(PAGE_SIZE - 1)
  }

  pub fn get_relative_offset(self) -> u64 {
    // keep last 10 bits
    self.offset & !(PAGE_SIZE - 1)
  }
}

impl Default for Metadata {
  fn default() -> Self {
    Self::new()
  }
}

impl Metadata {
  pub fn new() -> Metadata {
    Metadata {
      index: HashMap::new(),
      write_cursor: 0,
    }
  }

  pub fn load_meta(meta_path: &Path) -> Result<Metadata, error::Error> {
    let meta_file = File::open(meta_path).unwrap();
    let metadata = bincode::deserialize_from(meta_file)
      .map_err(|_| error::Error::MetadataSerialization)?;

    Ok(metadata)
  }

  pub fn get_write_cursor(&self) -> u64 {
    self.write_cursor
  }

  pub fn update_write_cursor(&mut self, new_cursor: u64) {
    self.write_cursor = new_cursor;
  }

  pub fn get_item_location(&self, key: &String) -> Option<Location> {
    self.index.get(key).copied()
  }

  pub fn set_item_location(&mut self, name: &String, location: Location) {
    self.index.insert(name.to_owned(), location);
  }

  pub fn save_meta(&mut self, meta_path: &Path) -> Result<(), error::Error> {
    let m = bincode::serialize(self).unwrap();
    let mut meta_file = File::create(meta_path).unwrap();
    meta_file.write_all(&m).unwrap();

    Ok(())
  }

  pub fn remove(&mut self, key: &String) {
    self.index.remove(key);
  }

  pub fn get_keys(&self) -> Keys<String, Location> {
    self.index.keys()
  }

  pub fn has_key(&self, key: &String) -> bool {
    self.index.contains_key(key)
  }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_offset() {
      let location_1 = Location {
        offset: 536,
        length: 10
      };
      let location_2 = Location {
        offset: 1536,
        length: 10
      };

      assert_eq!(location_1.get_page_offset(), 0);
      assert_eq!(location_2.get_page_offset(), 1024);
    }
}