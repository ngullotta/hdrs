use bincode::{Decode, Encode, config, encode_to_vec};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::Path;
use std::error::Error;


#[derive(Debug, Encode)]
pub struct Blob {
    pub timestamp: u64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: u64,
}

impl<Context> bincode::Decode<Context> for Blob {
    fn decode<D: bincode::de::Decoder<Context = Context>>(
        decoder: &mut D,
    ) -> core::result::Result<Self, bincode::error::DecodeError> {
        Ok(Self {
            timestamp: bincode::Decode::decode(decoder)?,
            open: bincode::Decode::decode(decoder)?,
            high: bincode::Decode::decode(decoder)?,
            low: bincode::Decode::decode(decoder)?,
            close: bincode::Decode::decode(decoder)?,
            volume: bincode::Decode::decode(decoder)?,
        })
    }
}

const OBJECTS_DIR: &str = ".cndl/objects";

fn write_and_hash_object<T: Encode>(data: &T) -> Result<String, Box<dyn Error>> {
    let sdata = encode_to_vec(data, config::standard())?;
    let mut hasher = Sha256::new();
    hasher.update(&sdata);
    let hash = format!("{:x}", hasher.finalize());

    let (prefix, fname) = hash.split_at(2);
    let dir = Path::new(OBJECTS_DIR).join(prefix);
    let path = dir.join(fname);

    fs::create_dir_all(&dir)?;
    fs::write(&path, &sdata)?;

    Ok(hash)
}

pub fn write_blob_object(data: &Blob) -> Result<String, Box<dyn Error>> {
    write_and_hash_object(data)
}

#[derive(Debug, Encode, Decode, PartialEq, Eq, PartialOrd, Ord)]
pub struct Entry {
    pub ticker: String,
    pub blob_hash: String,
}

#[derive(Debug, Encode, Decode)]
pub struct Snapshot {
    pub entries: Vec<Entry>,
}

pub fn write_snapshot_object(data: &Snapshot) -> Result<String, Box<dyn Error>> {
    write_and_hash_object(data)
}

#[derive(Debug, Encode, Decode)]
pub struct Commit {
    pub tree_hash: String,
    pub parent_hash: Option<String>,
    pub timestamp: u64,
    pub author: String,
    pub message: String,
}

pub fn write_commit_object(data: &Commit) -> Result<String, Box<dyn std::error::Error>> {
    write_and_hash_object(data)
}
