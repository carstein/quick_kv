use std::ops::Range;
use std::fmt;

use crate::metadata;
use crate::error;

pub const PAGE_SIZE: u64 = 128;

#[derive(Debug)]
 pub struct Page {
  pub offset: u64,
 // access_count: usize,
  pub data: Vec<u8>,
}

impl Default for Page {
  fn default() -> Self {
    Page::new()
  }
}

impl fmt::Display for Page {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {

        write!(f, "Page< 0x{} >", self.offset)
  }
}

impl Page {
  pub fn new() -> Page {
    Page {
      offset: 0,
      // access_count: 0,
      data: vec![0; PAGE_SIZE as usize],
    }
  }

  pub fn read(&self, location: &metadata::Location) -> Result<Vec<u8>, error::Error> {
    let s = location.get_relative_offset() as usize;
    let r = Range {
      start: s,
      end: s + location.length as usize
    };
    
    let v = &self.data[r];
    Ok(v.to_vec())
  }
}

