use bincode;

use std::collections::{HashMap, hash_map::Keys};
use std::{path::Path, fs::File, io::Write};
use serde::{Serialize, Deserialize};

use crate::error::Error;
use crate::page::PAGE_SIZE;

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
    self.offset & (PAGE_SIZE - 1)
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

  pub fn load_meta(meta_path: &Path) -> Result<Metadata, Error> {
    let meta_file = File::open(meta_path).map_err(|_| Error::MetadataDoesntExist)?;
    let metadata = bincode::deserialize_from(meta_file)
      .map_err(|_| Error::MetadataSerialization)?;

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

  pub fn save_meta(&mut self, meta_path: &Path) -> Result<(), Error> {
    let m = bincode::serialize(self).map_err(|_| Error::MetadataSerialization)?;
    let mut meta_file = File::create(meta_path).map_err(|_| Error::MetadataCreateFailed)?;
    meta_file.write_all(&m).map_err(|_| Error::MetadataSaveFailed)?;

    Ok(())
  }

  pub fn remove(&mut self, key: &String) -> Option<Location> {
    if let Some(mut block) = self.index.remove(key) {
      block.length = block.length.next_power_of_two();
      return Some(block);
    }

    None
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
        offset: 100,
        length: 10
      };
      let location_2 = Location {
        offset: PAGE_SIZE + 100,
        length: 10
      };

      assert_eq!(location_1.get_page_offset(), 0);
      assert_eq!(location_2.get_page_offset(), PAGE_SIZE);
    }

    #[test]
    fn relative_offset() {
      let location_1 = Location {
        offset: 12,
        length: 10
      };
      let location_2 = Location {
        offset: PAGE_SIZE + 10,
        length: 10
      };
      let location_3 = Location {
        offset: 0,
        length: 10
      };

      assert_eq!(location_1.get_relative_offset(), 12);
      assert_eq!(location_2.get_relative_offset(), 10);
      assert_eq!(location_3.get_relative_offset(), 0);
    }
}