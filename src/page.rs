
pub const PAGE_SIZE: u64 = 1024;

#[derive(Debug)]
 pub struct Page {
  offset: u64,
  access_count: usize,
  pub data: Vec<u8>,
}

impl Page {
  pub fn new() -> Page {
    Page {
      offset: 0,
      access_count: 0,
      data: Vec::with_capacity(PAGE_SIZE as usize),
    }
  }
}

impl Default for Page {
  fn default() -> Self {
    Page::new()
  }
}