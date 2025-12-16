static POLY: u32 = 0xEDB88320; 

pub struct Crc32 {
    table: [u32; 256]
}

impl Crc32 {
    pub fn new() -> Self {
        let mut table = [0u32; 256];
        for i in 0..256 {
            let mut crc = i as u32;
            for _ in 0..8 {
                if crc & 1 != 0 {
                    crc = (crc >> 1) ^ POLY;
                } else {
                    crc >>= 1;
                }
            }
            table[i] = crc;
        }
        Crc32 { table }
    }

    pub fn checksum(&self, data: &[u8]) -> u32 {
        let mut crc = 0xFFFFFFFF;
        for &byte in data {
            let i = ((crc ^ byte as u32) & 0xFF) as usize;
            crc = (crc >> 8) ^ self.table[i];
        }
        !crc
    }
}

impl Default for Crc32 {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crc32() {
        let crc = Crc32::new();
        let data: Vec<u8> = vec![0xC0, 0xFF, 0xEE];
        let checksum = crc.checksum(&data);
        assert_eq!(checksum, 0xBA787D5F)
    }
}
