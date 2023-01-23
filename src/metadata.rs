use std::collections::HashMap;

const PAGE_SIZE: u64 = 1024;

#[derive(Debug, Clone, Copy)]
pub struct Location {
  offset: u64,
  length: u64,
}

#[derive(Debug)]
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

  pub fn get_offset(self) -> u64 {
    self.offset
  }

  pub fn len(self) -> u64 {
    self.length
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

impl Metadata {
  pub fn new(namespace: String) -> Metadata {
    // if file exist, deserialize metadata
    // otherwise, create empty one

    Metadata {
      index: HashMap::new(),
      write_cursor: 0,
    }
  }

  pub fn get_write_cursor(&self) -> u64 {
    self.write_cursor
  }

  pub fn update_write_cursor(&mut self, new_cursor: u64) {
    self.write_cursor = new_cursor;
  }

  pub fn get_item_location(&self, key: &String) -> Option<&Location> {
    self.index.get(key)
  }

  pub fn set_item_location(&mut self, name: &String, location: Location) {
    self.index.insert(name.to_owned(), location);
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