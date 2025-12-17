mod crc32;
mod delta_encoding;
mod types;
mod compression;

pub use types::{Price, Tick, CompressionMetadata};
pub use compression::CompressedTimeSeries;
