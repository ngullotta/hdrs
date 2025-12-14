use std::io;

/// Variable-length integer encoding for deltas
#[derive(Debug, Clone, Copy)]
pub enum DeltaEncoding {
    Tiny(i8),      // -7 to +7 basis points (4 bits)
    Small(i16),    // -127 to +127 basis points (8 bits)
    Large(i32),    // Full value
}

impl DeltaEncoding {
    pub fn from_basis_points(bp: i32) -> Self {
        if bp >= -7 && bp <= 7 {
            DeltaEncoding::Tiny(bp as i8)
        } else if bp >= -127 && bp <= 127 {
            DeltaEncoding::Small(bp as i16)
        } else {
            DeltaEncoding::Large(bp)
        }
    }

    pub fn to_basis_points(&self) -> i32 {
        match self {
            DeltaEncoding::Tiny(v) => *v as i32,
            DeltaEncoding::Small(v) => *v as i32,
            DeltaEncoding::Large(v) => *v,
        }
    }

    pub fn encode(&self, buffer: &mut Vec<u8>) {
        match self {
            DeltaEncoding::Tiny(v) => {
                // Pack into 4 bits with 0b00 prefix
                buffer.push(((*v as u8) & 0x0F));
            }
            DeltaEncoding::Small(v) => {
                // 0b01 prefix + 8 bits
                buffer.push(0b01000000 | ((*v as u8) & 0x3F));
                buffer.push(((*v >> 6) as u8) & 0xFF);
            }
            DeltaEncoding::Large(v) => {
                // 0b11 prefix + 32 bits
                buffer.push(0b11000000);
                buffer.extend_from_slice(&v.to_le_bytes());
            }
        }
    }

    pub fn decode(buffer: &[u8], pos: &mut usize) -> io::Result<Self> {
        if *pos >= buffer.len() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Buffer underrun"));
        }

        let first = buffer[*pos];
        let prefix = first >> 6;

        match prefix {
            0b00 => {
                // Tiny: 4 bits
                let val = (first & 0x0F) as i8;
                let val = if val > 7 { val - 16 } else { val };
                *pos += 1;
                Ok(DeltaEncoding::Tiny(val))
            }
            0b01 => {
                // Small: 14 bits
                if *pos + 1 >= buffer.len() {
                    return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Buffer underrun"));
                }
                let low = (first & 0x3F) as i16;
                let high = buffer[*pos + 1] as i16;
                let val = (high << 6) | low;
                let val = if val > 8191 { val - 16384 } else { val };
                *pos += 2;
                Ok(DeltaEncoding::Small(val))
            }
            _ => {
                // Large: 32 bits
                if *pos + 4 >= buffer.len() {
                    return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Buffer underrun"));
                }
                *pos += 1;
                let mut bytes = [0u8; 4];
                bytes.copy_from_slice(&buffer[*pos..*pos + 4]);
                *pos += 4;
                Ok(DeltaEncoding::Large(i32::from_le_bytes(bytes)))
            }
        }
    }
}