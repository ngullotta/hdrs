pub struct Crc32 {
    table: [u32; 256],
}

impl Crc32 {
    pub fn new() -> Self {
        let mut table = [0u32; 256];
        for i in 0..256 {
            let mut crc = i as u32;
            for _ in 0..8 {
                if crc & 1 != 0 {
                    crc = (crc >> 1) ^ 0xEDB88320;
                } else {
                    crc >>= 1;
                }
            }
            table[i] = crc;
        }
        Crc32 { table }
    }

    pub fn checksum(&self, data: &[u8]) -> u32 {
        let mut crc = 0xFFFFFFFF_u32;
        for &byte in data {
            let index = ((crc ^ byte as u32) & 0xFF) as usize;
            crc = (crc >> 8) ^ self.table[index];
        }
        !crc
    }
}

impl Default for Crc32 {
    fn default() -> Self {
        Self::new()
    }
}