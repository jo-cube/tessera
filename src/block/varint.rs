//! Canonical unsigned LEB128.
//!
//! Each `u64` has exactly one accepted encoding, which keeps block bytes
//! deterministic and turns overlong encodings into detectable corruption.

use super::Corruption;

/// Encoded length of `u64::MAX`.
pub(super) const MAX_LEN: usize = 10;

pub(super) fn write(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push(value as u8 | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

pub(super) const fn encoded_len(value: u64) -> usize {
    let significant_bits = (u64::BITS - (value | 1).leading_zeros()) as usize;
    significant_bits.div_ceil(7)
}

/// Reads one varint from the front of `input`, advancing it on success.
pub(super) fn read(input: &mut &[u8]) -> Result<u64, Corruption> {
    let mut value = 0u64;
    for (i, &byte) in input.iter().take(MAX_LEN).enumerate() {
        let payload = u64::from(byte & 0x7f);
        // The tenth byte can only hold the top bit of a u64.
        if i == MAX_LEN - 1 && payload > 1 {
            return Err(Corruption::InvalidVarint);
        }
        value |= payload << (7 * i);
        if byte & 0x80 == 0 {
            // A zero final byte would mean a shorter encoding exists.
            if byte == 0 && i > 0 {
                return Err(Corruption::InvalidVarint);
            }
            *input = &input[i + 1..];
            return Ok(value);
        }
    }
    Err(if input.len() < MAX_LEN {
        Corruption::Truncated
    } else {
        Corruption::InvalidVarint
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_every_length_boundary() {
        let mut values = vec![0, u64::MAX];
        for bits in (7..64).step_by(7) {
            values.extend([(1u64 << bits) - 1, 1u64 << bits]);
        }
        for value in values {
            let mut encoded = Vec::new();
            write(&mut encoded, value);
            assert_eq!(encoded.len(), encoded_len(value), "length of {value}");

            encoded.push(0xAA); // Bytes after the varint must be left in place.
            let mut input = encoded.as_slice();
            assert_eq!(read(&mut input), Ok(value));
            assert_eq!(input, &[0xAA]);
        }
    }

    #[test]
    fn known_encodings_are_little_endian_groups() {
        for (value, expected) in [
            (0, &[0x00][..]),
            (127, &[0x7f]),
            (128, &[0x80, 0x01]),
            (300, &[0xac, 0x02]),
        ] {
            let mut encoded = Vec::new();
            write(&mut encoded, value);
            assert_eq!(encoded, expected);
        }
    }

    #[test]
    fn rejects_malformed_encodings_without_advancing() {
        let too_big_tenth_byte = [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02];
        let eleven_bytes = [0x80; 11];
        for (bytes, expected) in [
            (&[][..], Corruption::Truncated),
            (&[0x80], Corruption::Truncated),
            (&[0x80, 0x00], Corruption::InvalidVarint),
            (&too_big_tenth_byte, Corruption::InvalidVarint),
            (&eleven_bytes, Corruption::InvalidVarint),
        ] {
            let mut input = bytes;
            assert_eq!(read(&mut input), Err(expected), "{bytes:02x?}");
            assert_eq!(input, bytes);
        }
    }
}
