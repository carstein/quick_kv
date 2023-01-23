#!/usr/bin/env python3

import sys

page_size = 1024
filename = "page.db"

def generate_page(value):
  b = []
  pattern = 0x41 + value

  for i in range(page_size):
    b.append(chr(pattern))

  return ''.join(b)

def main():
  with open(filename, "w") as fh:

    for x in range(0, 20):
      fh.write(generate_page(x))


if __name__ == "__main__":
  sys.exit(main())