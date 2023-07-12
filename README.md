# Quick-KV - Quick and Dirty KV

This is a toy project - a KV library.

**Features**
- support for namespaces - you can create multiple different namespaces for your keys
- storage backed by file
- reading the key put them into memory cache
- writing a new key only invalidates part of the cache where the new key was written
- delete creates a free block that will be used in the next write if the size matches