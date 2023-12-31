#[derive(Debug)]
pub enum Error {
    NamespaceNotFound,
    FileTooSmall,
    InvalidValueLen,
    PageReadOverflow,
    KeyNotFound,
    ValueToLarge,
    DataFileSeek,
    DataFileWrite,
    MetadataSerialization,
    LoadPageFail,
    CacheEmpty,
    MetadataDoesntExist, 
    MetadataSaveFailed,
    MetadataCreateFailed,
    CacheReadFailed
}