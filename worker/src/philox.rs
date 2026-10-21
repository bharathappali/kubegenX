//! Philox4x32-10. Must match controller/internal/philox.

const M0: u32 = 0xD2511F53;
const M1: u32 = 0xCD9E8D57;
const W0: u32 = 0x9E3779B9;
const W1: u32 = 0xBB67AE85;

pub fn sum32(parts: &[u64]) -> (u32, u32) {
    let mut h: u64 = 0x9E3779B97F4A7C15;
    for p in parts {
        h ^= p
            .wrapping_add(0x9E3779B97F4A7C15)
            .wrapping_add(h.wrapping_shl(6))
            .wrapping_add(h.wrapping_shr(2));
        h = h.wrapping_mul(0xBF58476D1CE4E5B9);
        h ^= h >> 30;
    }
    h ^= h >> 27;
    h = h.wrapping_mul(0x94D049BB133111EB);
    h ^= h >> 31;
    (h as u32, (h >> 32) as u32)
}

pub fn four(mut k0: u32, mut k1: u32, mut c: [u32; 4]) -> [u32; 4] {
    for r in 0..10 {
        if r != 0 {
            k0 = k0.wrapping_add(W0);
            k1 = k1.wrapping_add(W1);
        }
        let p0 = (M0 as u64).wrapping_mul(c[0] as u64);
        let p1 = (M1 as u64).wrapping_mul(c[2] as u64);
        let hi0 = (p0 >> 32) as u32;
        let lo0 = p0 as u32;
        let hi1 = (p1 >> 32) as u32;
        let lo1 = p1 as u32;
        c = [hi1 ^ c[1] ^ k0, lo1, hi0 ^ c[3] ^ k1, lo0];
    }
    c
}

pub fn u32(seed: u64, feature: u32, entity: u32, chunk: u32, sample: u32, lane: u32) -> u32 {
    let mixed = ((feature as u64) << 32) | (entity as u64);
    let (k0, k1) = sum32(&[seed, mixed]);
    four(k0, k1, [chunk, sample, lane, entity])[0]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn golden() {
        assert_eq!(
            four(0, 0, [0, 0, 0, 0]),
            [0x6627e8d5, 0xe169c58d, 0xbc57ac4c, 0x9b00dbd8]
        );
        assert_eq!(u32(424242, 1, 7, 0, 0, 0), 2916870100);
        assert_eq!(u32(424242, 1, 7, 1, 3, 2), 3711581225);
    }
}
