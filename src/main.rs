// ==================== src/crc32.rs ====================
/// CRC32 implementation for checksums


// ==================== src/delta_encoding.rs ====================


// ==================== src/types.rs ====================


// ==================== src/compression.rs ====================


// ==================== src/lib.rs ====================


// ==================== src/main.rs ====================
use std::collections::HashMap;
use hdrs_compression::{Tick, CompressedTimeSeries};

fn main() {
    println!("HDRS Financial Data Compression Library");
    println!("========================================\n");

    let mut ticks = Vec::new();
    
    for i in 0..1000 {
        let mut prices = HashMap::new();
        let volatility = (i as f64 / 100.0).sin() * 0.5;
        
        prices.insert("AAPL".to_string(), 150.0 + volatility + (i as f64 * 0.01));
        prices.insert("GOOGL".to_string(), 2800.0 + volatility * 10.0 + (i as f64 * 0.05));
        prices.insert("MSFT".to_string(), 300.0 + volatility * 2.0 + (i as f64 * 0.02));

        ticks.push(Tick {
            timestamp: 1700000000 + i,
            prices,
        });
    }

    println!("Original data:");
    println!("  Symbols: 3");
    println!("  Ticks: {}", ticks.len());
    let original_size = ticks.len() * 3 * (8 + 8);
    println!("  Estimated size: {} bytes\n", original_size);

    let start = std::time::Instant::now();
    let compressed = CompressedTimeSeries::compress(&ticks).unwrap();
    let compress_time = start.elapsed();

    let serialized = compressed.serialize().unwrap();
    println!("Compressed data:");
    println!("  Size: {} bytes", serialized.len());
    println!("  Compression ratio: {:.2}%", compressed.compression_ratio(original_size) * 100.0);
    println!("  Compression time: {:?}", compress_time);
    
    let meta = compressed.metadata();
    println!("  Reference checksum: 0x{:08X}", meta.reference_checksum);
    println!("  Data checksum: 0x{:08X}", meta.data_checksum);
    println!("  Overall checksum: 0x{:08X}\n", meta.overall_checksum);

    let start = std::time::Instant::now();
    let deserialized = CompressedTimeSeries::deserialize(&serialized).unwrap();
    println!("Deserialization: Checksums verified ✓");
    
    let decompressed = deserialized.decompress().unwrap();
    let decompress_time = start.elapsed();

    println!("Decompressed data:");
    println!("  Ticks restored: {}", decompressed.len());
    println!("  Decompression time: {:?}\n", decompress_time);

    let mut max_error = 0.0f64;
    for (orig, restored) in ticks.iter().zip(decompressed.iter()) {
        for (symbol, &orig_price) in &orig.prices {
            if let Some(&restored_price) = restored.prices.get(symbol) {
                let error = (orig_price - restored_price).abs() / orig_price;
                max_error = max_error.max(error);
            }
        }
    }

    println!("Accuracy:");
    println!("  Maximum relative error: {:.6}%\n", max_error * 100.0);

    println!("--- File I/O Test ---");
    let filename = "/tmp/financial_data.hdrs";
    compressed.write_to_file(filename).unwrap();
    println!("✓ Written to: {}", filename);
    
    let _ = CompressedTimeSeries::read_from_file(filename).unwrap();
    println!("✓ Loaded from file");
    
    let blob = compressed.to_blob().unwrap();
    println!("✓ Blob size: {} bytes", blob.len());
    
    std::fs::remove_file(filename).ok();
    println!("✓ Cleanup complete");
}

// ==================== Cargo.toml ====================


// ==================== tests/integration_test.rs ====================
// use hdrs_compression::{Tick, CompressedTimeSeries};
// use std::collections::HashMap;

// #[test]
// fn test_compression_decompression() {
//     let mut ticks = Vec::new();
//     for i in 0..100 {
//         let mut prices = HashMap::new();
//         prices.insert("AAPL".to_string(), 150.0 + (i as f64 * 0.1));
//         prices.insert("GOOGL".to_string(), 2800.0 + (i as f64 * 0.5));
//         ticks.push(Tick { timestamp: 1000000 + i, prices });
//     }

//     let compressed = CompressedTimeSeries::compress(&ticks).unwrap();
//     let decompressed = compressed.decompress().unwrap();
//     assert_eq!(ticks.len(), decompressed.len());
// }

// #[test]
// fn test_file_io() {
//     let mut prices = HashMap::new();
//     prices.insert("AAPL".to_string(), 150.0);
//     let ticks = vec![Tick { timestamp: 1000000, prices }];

//     let compressed = CompressedTimeSeries::compress(&ticks).unwrap();
//     compressed.write_to_file("test.hdrs").unwrap();
//     let loaded = CompressedTimeSeries::read_from_file("test.hdrs").unwrap();
    
//     assert_eq!(compressed.metadata().num_ticks, loaded.metadata().num_ticks);
//     std::fs::remove_file("test.hdrs").ok();
// }

// #[test]
// fn test_corruption_detection() {
//     let mut prices = HashMap::new();
//     prices.insert("AAPL".to_string(), 150.0);
//     let ticks = vec![Tick { timestamp: 1000000, prices }];

//     let compressed = CompressedTimeSeries::compress(&ticks).unwrap();
//     let mut serialized = compressed.serialize().unwrap();
//     serialized[serialized.len() / 2] ^= 0xFF;
    
//     assert!(CompressedTimeSeries::deserialize(&serialized).is_err());
// }