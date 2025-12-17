use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::Path;

use crate::crc32::Crc32;
use crate::delta_encoding::DeltaEncoding;
use crate::types::{CompressionMetadata, Tick};

pub struct CompressedTimeSeries {
    version: u8,
    symbols: Vec<String>,
    base_ts: u64,
    ref_frame: Vec<f64>,
    data: Vec<u8>,
    num_ticks: u32,
    ref_crc: u32,
    data_crc: u32,
    overall_crc: u32,
}

impl CompressedTimeSeries {
    pub fn compress(ticks: &[Tick]) -> io::Result<Self> {
        if ticks.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Empty tick data",
            ));
        }

        let crc = Crc32::new();
        let symbols: Vec<String> = ticks[0].prices.keys().cloned().collect();
        let sym_idx: HashMap<String, usize> = symbols
            .iter()
            .enumerate()
            .map(|(i, s)| (s.clone(), i))
            .collect();

        let n = symbols.len();
        let base_ts = ticks[0].timestamp;

        let mut ref_frame = vec![0.0; n];
        for (sym, &price) in &ticks[0].prices {
            if let Some(&idx) = sym_idx.get(sym) {
                ref_frame[idx] = price;
            }
        }

        let mut ref_bytes = Vec::new();
        for &p in &ref_frame {
            ref_bytes.extend_from_slice(&p.to_le_bytes());
        }
        let ref_crc = crc.checksum(&ref_bytes);

        let mut data = Vec::new();
        let mut prev = ref_frame.clone();

        for tick in ticks.iter().skip(1) {
            let ts_delta = (tick.timestamp - base_ts) as u32;
            data.extend_from_slice(&ts_delta.to_le_bytes());

            let mut changed = vec![false; n];
            let mut deltas = Vec::new();

            for (sym, &price) in &tick.prices {
                if let Some(&idx) = sym_idx.get(sym) {
                    let delta_bp = ((price - prev[idx]) / prev[idx] * 10000.0).round() as i32;
                    if delta_bp != 0 {
                        changed[idx] = true;
                        deltas.push((idx, price, delta_bp));
                        prev[idx] = price;
                    }
                }
            }

            let bm_bytes = (n + 7) / 8;
            let mut bm = vec![0u8; bm_bytes];
            for (idx, &ch) in changed.iter().enumerate() {
                if ch {
                    bm[idx / 8] |= 1 << (idx % 8);
                }
            }
            data.extend_from_slice(&bm);

            for (_, _, delta_bp) in deltas {
                DeltaEncoding::from_basis(delta_bp).encode(&mut data);
            }
        }

        let data_crc = crc.checksum(&data);

        Ok(CompressedTimeSeries {
            version: 1,
            symbols,
            base_ts,
            ref_frame,
            data,
            num_ticks: ticks.len() as u32,
            ref_crc,
            data_crc,
            overall_crc: 0,
        })
    }

    pub fn decompress(&self) -> io::Result<Vec<Tick>> {
        let crc = Crc32::new();

        let mut ref_bytes = Vec::new();
        for &p in &self.ref_frame {
            ref_bytes.extend_from_slice(&p.to_le_bytes());
        }
        if crc.checksum(&ref_bytes) != self.ref_crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Reference checksum mismatch",
            ));
        }

        if crc.checksum(&self.data) != self.data_crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Data checksum mismatch",
            ));
        }

        let mut ticks = Vec::with_capacity(self.num_ticks as usize);
        let n = self.symbols.len();

        let mut first = HashMap::new();
        for (i, sym) in self.symbols.iter().enumerate() {
            first.insert(sym.clone(), self.ref_frame[i]);
        }
        ticks.push(Tick {
            timestamp: self.base_ts,
            prices: first,
        });

        let mut curr = self.ref_frame.clone();
        let mut pos = 0;
        let bm_bytes = (n + 7) / 8;

        while pos < self.data.len() {
            if pos + 4 > self.data.len() {
                break;
            }

            let ts_delta = u32::from_le_bytes(self.data[pos..pos + 4].try_into().unwrap());
            pos += 4;

            if pos + bm_bytes > self.data.len() {
                break;
            }
            let bm = &self.data[pos..pos + bm_bytes];
            pos += bm_bytes;

            for idx in 0..n {
                if bm[idx / 8] & (1 << (idx % 8)) != 0 {
                    let enc = DeltaEncoding::decode(&self.data, &mut pos)?;
                    let delta_bp = enc.to_basis();
                    curr[idx] *= 1.0 + delta_bp as f64 / 10000.0;
                }
            }

            let mut prices = HashMap::new();
            for (i, sym) in self.symbols.iter().enumerate() {
                prices.insert(sym.clone(), curr[i]);
            }
            ticks.push(Tick {
                timestamp: self.base_ts + ts_delta as u64,
                prices,
            });
        }

        Ok(ticks)
    }

    pub fn serialize(&self) -> io::Result<Vec<u8>> {
        let crc = Crc32::new();
        let mut buf = Vec::new();

        buf.write_all(&[self.version])?;
        buf.write_all(&(self.symbols.len() as u16).to_le_bytes())?;
        buf.write_all(&self.num_ticks.to_le_bytes())?;
        buf.write_all(&self.base_ts.to_le_bytes())?;

        for sym in &self.symbols {
            buf.write_all(&[sym.len() as u8])?;
            buf.write_all(sym.as_bytes())?;
        }

        for &p in &self.ref_frame {
            buf.write_all(&p.to_le_bytes())?;
        }

        buf.write_all(&self.ref_crc.to_le_bytes())?;
        buf.write_all(&self.data_crc.to_le_bytes())?;
        buf.write_all(&(self.data.len() as u32).to_le_bytes())?;
        buf.write_all(&self.data)?;

        let overall_crc = crc.checksum(&buf);
        buf.write_all(&overall_crc.to_le_bytes())?;

        Ok(buf)
    }

    pub fn deserialize(data: &[u8]) -> io::Result<Self> {
        let crc = Crc32::new();

        if data.len() < 4 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Data too short"));
        }

        let crc_pos = data.len() - 4;
        let overall_crc = u32::from_le_bytes(data[crc_pos..].try_into().unwrap());

        if crc.checksum(&data[..crc_pos]) != overall_crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Overall checksum mismatch",
            ));
        }

        let mut pos = 0;
        let version = data[pos];
        pos += 1;

        let n = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;

        let num_ticks = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
        pos += 4;

        let base_ts = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;

        let mut symbols = Vec::with_capacity(n);
        for _ in 0..n {
            let len = data[pos] as usize;
            pos += 1;
            let sym = String::from_utf8(data[pos..pos + len].to_vec())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            symbols.push(sym);
            pos += len;
        }

        let mut ref_frame = Vec::with_capacity(n);
        for _ in 0..n {
            ref_frame.push(f64::from_le_bytes(data[pos..pos + 8].try_into().unwrap()));
            pos += 8;
        }

        let ref_crc = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
        pos += 4;

        let data_crc = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
        pos += 4;

        let comp_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        let comp_data = data[pos..pos + comp_len].to_vec();

        Ok(CompressedTimeSeries {
            version,
            symbols,
            base_ts,
            ref_frame,
            data: comp_data,
            num_ticks,
            ref_crc,
            data_crc,
            overall_crc,
        })
    }

    pub fn compression_ratio(&self, orig_size: usize) -> f64 {
        let comp_size = self.serialize().unwrap().len();
        1.0 - (comp_size as f64 / orig_size as f64)
    }

    pub fn write_to_file<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        let mut file = File::create(path)?;
        file.write_all(&self.serialize()?)?;
        file.sync_all()
    }

    pub fn read_from_file<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let mut buf = Vec::new();
        File::open(path)?.read_to_end(&mut buf)?;
        Self::deserialize(&buf)
    }

    pub fn write_to<W: Write>(&self, w: &mut W) -> io::Result<usize> {
        let ser = self.serialize()?;
        w.write_all(&ser)?;
        Ok(ser.len())
    }

    pub fn read_from<R: Read>(r: &mut R) -> io::Result<Self> {
        let mut buf = Vec::new();
        r.read_to_end(&mut buf)?;
        Self::deserialize(&buf)
    }

    pub fn to_blob(&self) -> io::Result<Vec<u8>> {
        self.serialize()
    }

    pub fn from_blob(blob: &[u8]) -> io::Result<Self> {
        Self::deserialize(blob)
    }

    pub fn metadata(&self) -> CompressionMetadata {
        CompressionMetadata {
            version: self.version,
            num_symbols: self.symbols.len(),
            num_ticks: self.num_ticks as usize,
            base_timestamp: self.base_ts,
            symbols: self.symbols.clone(),
            compressed_size: self.data.len(),
            reference_checksum: self.ref_crc,
            data_checksum: self.data_crc,
            overall_checksum: self.overall_crc,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn make_ticks() -> Vec<Tick> {
        vec![
            Tick {
                timestamp: 1000,
                prices: [("AAPL", 150.0), ("GOOGL", 2800.0)]
                    .iter()
                    .map(|(k, v)| (k.to_string(), *v))
                    .collect(),
            },
            Tick {
                timestamp: 1001,
                prices: [("AAPL", 150.5), ("GOOGL", 2805.0)]
                    .iter()
                    .map(|(k, v)| (k.to_string(), *v))
                    .collect(),
            },
            Tick {
                timestamp: 1002,
                prices: [("AAPL", 150.3), ("GOOGL", 2803.0)]
                    .iter()
                    .map(|(k, v)| (k.to_string(), *v))
                    .collect(),
            },
        ]
    }

    #[test]
    fn test_compress_decompress_roundtrip() {
        let ticks = make_ticks();
        let compressed = CompressedTimeSeries::compress(&ticks).unwrap();
        let decompressed = compressed.decompress().unwrap();

        assert_eq!(ticks.len(), decompressed.len());
        for (orig, decomp) in ticks.iter().zip(decompressed.iter()) {
            assert_eq!(orig.timestamp, decomp.timestamp);
            assert_eq!(orig.prices.len(), decomp.prices.len());
            for (sym, &price) in &orig.prices {
                let decomp_price = decomp.prices.get(sym).unwrap();
                let rel_error = ((price - decomp_price) / price).abs();
                assert!(
                    rel_error < 0.01,
                    "Price mismatch for {}: expected {}, got {} (rel error: {})",
                    sym,
                    price,
                    decomp_price,
                    rel_error
                );
            }
        }
    }

    #[test]
    fn test_serialize_deserialize_roundtrip() {
        let ticks = make_ticks();
        let compressed = CompressedTimeSeries::compress(&ticks).unwrap();
        let serialized = compressed.serialize().unwrap();
        let deserialized = CompressedTimeSeries::deserialize(&serialized).unwrap();

        assert_eq!(compressed.version, deserialized.version);
        assert_eq!(compressed.symbols, deserialized.symbols);
        assert_eq!(compressed.base_ts, deserialized.base_ts);
        assert_eq!(compressed.num_ticks, deserialized.num_ticks);
        assert_eq!(compressed.ref_crc, deserialized.ref_crc);
        assert_eq!(compressed.data_crc, deserialized.data_crc);
    }

    #[test]
    fn test_empty_ticks_error() {
        let result = CompressedTimeSeries::compress(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_checksum_validation() {
        let ticks = make_ticks();
        let mut compressed = CompressedTimeSeries::compress(&ticks).unwrap();

        compressed.data[0] ^= 0xFF;
        let result = compressed.decompress();
        assert!(result.is_err());
    }

    #[test]
    fn test_blob_operations() {
        let ticks = make_ticks();
        let compressed = CompressedTimeSeries::compress(&ticks).unwrap();
        let blob = compressed.to_blob().unwrap();
        let restored = CompressedTimeSeries::from_blob(&blob).unwrap();

        assert_eq!(compressed.symbols, restored.symbols);
        assert_eq!(compressed.num_ticks, restored.num_ticks);
    }

    #[test]
    fn test_writer_reader() {
        let ticks = make_ticks();
        let compressed = CompressedTimeSeries::compress(&ticks).unwrap();

        let mut buf = Vec::new();
        let written = compressed.write_to(&mut buf).unwrap();
        assert!(written > 0);

        let mut cursor = Cursor::new(buf);
        let restored = CompressedTimeSeries::read_from(&mut cursor).unwrap();

        assert_eq!(compressed.symbols, restored.symbols);
    }

    #[test]
    fn test_metadata() {
        let ticks = make_ticks();
        let compressed = CompressedTimeSeries::compress(&ticks).unwrap();
        let meta = compressed.metadata();

        assert_eq!(meta.version, 1);
        assert_eq!(meta.num_symbols, 2);
        assert_eq!(meta.num_ticks, 3);
        assert_eq!(meta.base_timestamp, 1000);
        assert_eq!(meta.symbols.len(), 2);
    }

    #[test]
    fn test_compression_ratio() {
        let ticks = make_ticks();
        let orig_size = ticks.len() * std::mem::size_of::<Tick>() * 10;
        let compressed = CompressedTimeSeries::compress(&ticks).unwrap();
        let ratio = compressed.compression_ratio(orig_size);

        assert!(ratio > 0.0 && ratio < 1.0);
    }

    #[test]
    fn test_corrupted_overall_checksum() {
        let ticks = make_ticks();
        let compressed = CompressedTimeSeries::compress(&ticks).unwrap();
        let mut serialized = compressed.serialize().unwrap();

        let len = serialized.len();
        serialized[len - 1] ^= 0xFF;

        let result = CompressedTimeSeries::deserialize(&serialized);
        assert!(result.is_err());
    }

    #[test]
    fn test_single_tick() {
        let ticks = vec![Tick {
            timestamp: 1000,
            prices: [("AAPL", 150.0)]
                .iter()
                .map(|(k, v)| (k.to_string(), *v))
                .collect(),
        }];

        let compressed = CompressedTimeSeries::compress(&ticks).unwrap();
        let decompressed = compressed.decompress().unwrap();

        assert_eq!(decompressed.len(), 1);
        assert_eq!(decompressed[0].timestamp, 1000);
    }
}
