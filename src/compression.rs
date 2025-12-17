use std::collections::HashMap;
use std::io::{self, Write};
use std::fs::File;
use std::path::Path;

use crate::crc32::Crc32;
use crate::delta_encoding::DeltaEncoding;
use crate::types::{Tick, CompressionMetadata};

/// Compressed financial time series
pub struct CompressedTimeSeries {
    version: u8,
    symbols: Vec<String>,
    base_timestamp: u64,
    reference_frame: Vec<f64>,
    compressed_data: Vec<u8>,
    num_ticks: u32,
    // Checksums for integrity verification
    reference_checksum: u32,
    data_checksum: u32,
    overall_checksum: u32,
}

impl CompressedTimeSeries {
    /// Compress a time series of multi-symbol ticks
    pub fn compress(ticks: &[Tick]) -> io::Result<Self> {
        if ticks.is_empty() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Empty tick data"));
        }

        let crc = Crc32::new();

        // Build symbol dictionary from first tick
        let symbols: Vec<String> = ticks[0].prices.keys().cloned().collect();
        let symbol_to_idx: HashMap<String, usize> = symbols
            .iter()
            .enumerate()
            .map(|(i, s)| (s.clone(), i))
            .collect();

        let num_symbols = symbols.len();
        let base_timestamp = ticks[0].timestamp;

        // Reference frame from first tick
        let mut reference_frame = vec![0.0; num_symbols];
        for (symbol, &price) in &ticks[0].prices {
            if let Some(&idx) = symbol_to_idx.get(symbol) {
                reference_frame[idx] = price;
            }
        }

        // Compute reference frame checksum
        let mut ref_bytes = Vec::new();
        for &price in &reference_frame {
            ref_bytes.extend_from_slice(&price.to_le_bytes());
        }
        let reference_checksum = crc.checksum(&ref_bytes);

        // Compress subsequent ticks
        let mut compressed_data = Vec::new();
        let mut prev_prices = reference_frame.clone();

        for tick in ticks.iter().skip(1) {
            // Timestamp delta
            let ts_delta = (tick.timestamp - base_timestamp) as u32;
            compressed_data.extend_from_slice(&ts_delta.to_le_bytes());

            // Build change bitmap
            let mut changed_symbols = vec![false; num_symbols];
            let mut deltas = Vec::new();

            for (symbol, &price) in &tick.prices {
                if let Some(&idx) = symbol_to_idx.get(symbol) {
                    let prev_price = prev_prices[idx];
                    let delta_bp = ((price - prev_price) / prev_price * 10000.0).round() as i32;

                    if delta_bp != 0 {
                        changed_symbols[idx] = true;
                        deltas.push((idx, price, delta_bp));
                        prev_prices[idx] = price;
                    }
                }
            }

            // Write bitmap
            let bitmap_bytes = (num_symbols + 7) / 8;
            let mut bitmap = vec![0u8; bitmap_bytes];
            for (idx, &changed) in changed_symbols.iter().enumerate() {
                if changed {
                    bitmap[idx / 8] |= 1 << (idx % 8);
                }
            }
            compressed_data.extend_from_slice(&bitmap);

            // Write deltas
            for (_idx, _price, delta_bp) in deltas {
                let encoding = DeltaEncoding::from_basis(delta_bp);
                encoding.encode(&mut compressed_data);
            }
        }

        let data_checksum = crc.checksum(&compressed_data);

        Ok(CompressedTimeSeries {
            version: 1,
            symbols,
            base_timestamp,
            reference_frame,
            compressed_data,
            num_ticks: ticks.len() as u32,
            reference_checksum,
            data_checksum,
            overall_checksum: 0,
        })
    }

    /// Decompress the entire time series
    pub fn decompress(&self) -> io::Result<Vec<Tick>> {
        let crc = Crc32::new();

        // Verify reference frame integrity
        let mut ref_bytes = Vec::new();
        for &price in &self.reference_frame {
            ref_bytes.extend_from_slice(&price.to_le_bytes());
        }
        let computed_ref_checksum = crc.checksum(&ref_bytes);
        if computed_ref_checksum != self.reference_checksum {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Reference frame checksum mismatch: expected {}, got {}", 
                    self.reference_checksum, computed_ref_checksum)
            ));
        }

        // Verify compressed data integrity
        let computed_data_checksum = crc.checksum(&self.compressed_data);
        if computed_data_checksum != self.data_checksum {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Compressed data checksum mismatch: expected {}, got {}", 
                    self.data_checksum, computed_data_checksum)
            ));
        }

        let mut ticks = Vec::with_capacity(self.num_ticks as usize);
        let num_symbols = self.symbols.len();

        // First tick is the reference frame
        let mut first_tick_prices = HashMap::new();
        for (i, symbol) in self.symbols.iter().enumerate() {
            first_tick_prices.insert(symbol.clone(), self.reference_frame[i]);
        }
        ticks.push(Tick {
            timestamp: self.base_timestamp,
            prices: first_tick_prices,
        });

        let mut current_prices = self.reference_frame.clone();
        let mut pos = 0;
        let bitmap_bytes = (num_symbols + 7) / 8;

        while pos < self.compressed_data.len() {
            if pos + 4 > self.compressed_data.len() {
                break;
            }
            let mut ts_bytes = [0u8; 4];
            ts_bytes.copy_from_slice(&self.compressed_data[pos..pos + 4]);
            let ts_delta = u32::from_le_bytes(ts_bytes);
            pos += 4;

            if pos + bitmap_bytes > self.compressed_data.len() {
                break;
            }
            let bitmap = &self.compressed_data[pos..pos + bitmap_bytes];
            pos += bitmap_bytes;

            for idx in 0..num_symbols {
                let byte_idx = idx / 8;
                let bit_idx = idx % 8;
                if bitmap[byte_idx] & (1 << bit_idx) != 0 {
                    let delta_encoding = DeltaEncoding::decode(&self.compressed_data, &mut pos)?;
                    let delta_bp = delta_encoding.to_basis();
                    let prev_price = current_prices[idx];
                    current_prices[idx] = prev_price * (1.0 + delta_bp as f64 / 10000.0);
                }
            }

            let mut tick_prices = HashMap::new();
            for (i, symbol) in self.symbols.iter().enumerate() {
                tick_prices.insert(symbol.clone(), current_prices[i]);
            }
            ticks.push(Tick {
                timestamp: self.base_timestamp + ts_delta as u64,
                prices: tick_prices,
            });
        }

        Ok(ticks)
    }

    /// Serialize to bytes
    pub fn serialize(&self) -> io::Result<Vec<u8>> {
        let crc = Crc32::new();
        let mut buffer = Vec::new();

        buffer.write_all(&[self.version])?;
        buffer.write_all(&(self.symbols.len() as u16).to_le_bytes())?;
        buffer.write_all(&self.num_ticks.to_le_bytes())?;
        buffer.write_all(&self.base_timestamp.to_le_bytes())?;

        for symbol in &self.symbols {
            buffer.write_all(&(symbol.len() as u8).to_le_bytes())?;
            buffer.write_all(symbol.as_bytes())?;
        }

        for &price in &self.reference_frame {
            buffer.write_all(&price.to_le_bytes())?;
        }

        buffer.write_all(&self.reference_checksum.to_le_bytes())?;
        buffer.write_all(&self.data_checksum.to_le_bytes())?;

        buffer.write_all(&(self.compressed_data.len() as u32).to_le_bytes())?;
        buffer.write_all(&self.compressed_data)?;

        let overall_checksum = crc.checksum(&buffer);
        buffer.write_all(&overall_checksum.to_le_bytes())?;

        Ok(buffer)
    }

    /// Deserialize from bytes
    pub fn deserialize(data: &[u8]) -> io::Result<Self> {
        let crc = Crc32::new();
        
        if data.len() < 4 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Data too short"));
        }
        
        let overall_checksum_pos = data.len() - 4;
        let mut expected_checksum_bytes = [0u8; 4];
        expected_checksum_bytes.copy_from_slice(&data[overall_checksum_pos..]);
        let expected_checksum = u32::from_le_bytes(expected_checksum_bytes);
        
        let computed_checksum = crc.checksum(&data[..overall_checksum_pos]);
        if computed_checksum != expected_checksum {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Overall checksum mismatch: expected {}, got {}", 
                    expected_checksum, computed_checksum)
            ));
        }

        let mut pos = 0;

        let version = data[pos];
        pos += 1;

        let mut num_symbols_bytes = [0u8; 2];
        num_symbols_bytes.copy_from_slice(&data[pos..pos + 2]);
        let num_symbols = u16::from_le_bytes(num_symbols_bytes) as usize;
        pos += 2;

        let mut num_ticks_bytes = [0u8; 4];
        num_ticks_bytes.copy_from_slice(&data[pos..pos + 4]);
        let num_ticks = u32::from_le_bytes(num_ticks_bytes);
        pos += 4;

        let mut base_ts_bytes = [0u8; 8];
        base_ts_bytes.copy_from_slice(&data[pos..pos + 8]);
        let base_timestamp = u64::from_le_bytes(base_ts_bytes);
        pos += 8;

        let mut symbols = Vec::with_capacity(num_symbols);
        for _ in 0..num_symbols {
            let len = data[pos] as usize;
            pos += 1;
            let symbol = String::from_utf8(data[pos..pos + len].to_vec())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            symbols.push(symbol);
            pos += len;
        }

        let mut reference_frame = Vec::with_capacity(num_symbols);
        for _ in 0..num_symbols {
            let mut price_bytes = [0u8; 8];
            price_bytes.copy_from_slice(&data[pos..pos + 8]);
            reference_frame.push(f64::from_le_bytes(price_bytes));
            pos += 8;
        }

        let mut ref_checksum_bytes = [0u8; 4];
        ref_checksum_bytes.copy_from_slice(&data[pos..pos + 4]);
        let reference_checksum = u32::from_le_bytes(ref_checksum_bytes);
        pos += 4;

        let mut data_checksum_bytes = [0u8; 4];
        data_checksum_bytes.copy_from_slice(&data[pos..pos + 4]);
        let data_checksum = u32::from_le_bytes(data_checksum_bytes);
        pos += 4;

        let mut compressed_len_bytes = [0u8; 4];
        compressed_len_bytes.copy_from_slice(&data[pos..pos + 4]);
        let compressed_len = u32::from_le_bytes(compressed_len_bytes) as usize;
        pos += 4;

        let compressed_data = data[pos..pos + compressed_len].to_vec();

        Ok(CompressedTimeSeries {
            version,
            symbols,
            base_timestamp,
            reference_frame,
            compressed_data,
            num_ticks,
            reference_checksum,
            data_checksum,
            overall_checksum: expected_checksum,
        })
    }

    pub fn compression_ratio(&self, original_size: usize) -> f64 {
        let compressed_size = self.serialize().unwrap().len();
        1.0 - (compressed_size as f64 / original_size as f64)
    }

    /// Write compressed data to a file
    pub fn write_to_file<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        let serialized = self.serialize()?;
        let mut file = File::create(path)?;
        file.write_all(&serialized)?;
        file.sync_all()?;
        Ok(())
    }

    /// Read compressed data from a file
    pub fn read_from_file<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let mut file = File::open(path)?;
        let mut buffer = Vec::new();
        use std::io::Read;
        file.read_to_end(&mut buffer)?;
        Self::deserialize(&buffer)
    }

    /// Write compressed data to a writer
    pub fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<usize> {
        let serialized = self.serialize()?;
        writer.write_all(&serialized)?;
        Ok(serialized.len())
    }

    /// Read compressed data from a reader
    pub fn read_from<R: std::io::Read>(reader: &mut R) -> io::Result<Self> {
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer)?;
        Self::deserialize(&buffer)
    }

    /// Get the raw serialized bytes
    pub fn to_blob(&self) -> io::Result<Vec<u8>> {
        self.serialize()
    }

    /// Create from raw blob bytes
    pub fn from_blob(blob: &[u8]) -> io::Result<Self> {
        Self::deserialize(blob)
    }

    /// Get metadata without decompressing
    pub fn metadata(&self) -> CompressionMetadata {
        CompressionMetadata {
            version: self.version,
            num_symbols: self.symbols.len(),
            num_ticks: self.num_ticks as usize,
            base_timestamp: self.base_timestamp,
            symbols: self.symbols.clone(),
            compressed_size: self.compressed_data.len(),
            reference_checksum: self.reference_checksum,
            data_checksum: self.data_checksum,
            overall_checksum: self.overall_checksum,
        }
    }
}