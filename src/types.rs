use std::collections::HashMap;

#[derive(Debug, Clone, Copy)]
pub struct Price {
    pub value: f64,
    pub timestamp: u64,
}

#[derive(Debug, Clone)]
pub struct Tick {
    pub timestamp: u64,
    pub prices: HashMap<String, f64>,
}

#[derive(Debug, Clone)]
pub struct CompressionMetadata {
    pub version: u8,
    pub num_symbols: usize,
    pub num_ticks: usize,
    pub base_timestamp: u64,
    pub symbols: Vec<String>,
    pub compressed_size: usize,
    pub reference_checksum: u32,
    pub data_checksum: u32,
    pub overall_checksum: u32,
}
