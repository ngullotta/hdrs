use std::io;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DeltaEncoding {
    Tiny(i8),
    Small(i16),
    Large(i32),
}

impl DeltaEncoding {
    pub fn from_basis(bp: i32) -> io::Result<Self> {
        if bp >= -8 && bp <= 7 {
            return Ok(DeltaEncoding::Tiny(bp as i8));
        } else if bp >= -8192 && bp <= 8191 {
            return Ok(DeltaEncoding::Small(bp as i16));
        } else if bp <= i32::MAX {
            return Ok(DeltaEncoding::Large(bp));
        }
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Basis must be between -8 and i32::MAX",
        ));
    }

    pub fn to_basis(&self) -> i32 {
        match self {
            DeltaEncoding::Tiny(v) => *v as i32,
            DeltaEncoding::Small(v) => *v as i32,
            DeltaEncoding::Large(v) => *v,
        }
    }

    pub fn encode(&self, buf: &mut Vec<u8>) {
        match self {
            DeltaEncoding::Tiny(v) => {
                // Pack into 4 bits with 0b00 prefix
                // @ToDo -> Maybe pack two of these together?
                buf.push((*v as u8) & 0x0F);
            }
            DeltaEncoding::Small(v) => {
                // 0b01 prefix + remaining bits + 8 bits
                buf.push(0b01000000 | ((*v as u8) & 0x3F));
                buf.push(((*v >> 6) as u8) & 0xFF);
            }
            DeltaEncoding::Large(v) => {
                // 0b11 prefix + 32 bits
                buf.push(0b11000000);
                buf.extend_from_slice(&v.to_le_bytes());
            }
        }
    }

    pub fn decode(buf: &[u8], pos: &mut usize) -> io::Result<Self> {
        if *pos >= buf.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Buffer underrun",
            ));
        }

        let first = buf[*pos];
        let pre = first >> 6;

        match pre {
            // Tiny
            0b00 => {
                let v = (first & 0x0F) as i8;
                let v = if v > 7 { v - 16 } else { v };
                *pos += 1;
                Ok(DeltaEncoding::Tiny(v))
            }
            // Small
            0b01 => {
                if *pos + 1 >= buf.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "Buffer underrun",
                    ));
                }
                let l = (first & 0x3F) as i16;
                let h = buf[*pos + 1] as i16;
                let v = (h << 6) | l;
                let v = if v > 8191 { v - 16384 } else { v };
                *pos += 2;
                Ok(DeltaEncoding::Small(v))
            }
            // Large
            _ => {
                if *pos + 4 >= buf.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "Buffer underrun",
                    ));
                }
                *pos += 1;
                let mut bytes = [0u8; 4];
                bytes.copy_from_slice(&buf[*pos..*pos + 4]);
                *pos += 4;
                Ok(DeltaEncoding::Large(i32::from_le_bytes(bytes)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delta_encoding() {
        let buf = [0b00111111];
        let mut pos: usize = 0;
        let res = DeltaEncoding::decode(&buf, &mut pos).unwrap();
        assert_eq!(res, DeltaEncoding::Tiny(-1))
    }
}
