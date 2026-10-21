// Package philox implements Philox4x32-10 (Salmon et al., Random123).
// The round function matches Random123: ten rounds, key bumped between rounds.
package philox

const (
	m0 = 0xD2511F53
	m1 = 0xCD9E8D57
	w0 = 0x9E3779B9
	w1 = 0xBB67AE85
)

// Sum32 is a tiny non-cryptographic mix used only to derive a Philox key.
func Sum32(parts ...uint64) (uint32, uint32) {
	var h uint64 = 0x9E3779B97F4A7C15
	for _, p := range parts {
		h ^= p + 0x9E3779B97F4A7C15 + (h << 6) + (h >> 2)
		h *= 0xBF58476D1CE4E5B9
		h ^= h >> 30
	}
	h ^= h >> 27
	h *= 0x94D049BB133111EB
	h ^= h >> 31
	return uint32(h), uint32(h >> 32)
}

// Four returns the 4-word Philox4x32-10 output for key (k0,k1) and counter.
func Four(k0, k1 uint32, ctr [4]uint32) [4]uint32 {
	c := ctr
	key0, key1 := k0, k1
	for r := 0; r < 10; r++ {
		if r != 0 {
			key0 += w0
			key1 += w1
		}
		p0 := uint64(m0) * uint64(c[0])
		p1 := uint64(m1) * uint64(c[2])
		hi0 := uint32(p0 >> 32)
		lo0 := uint32(p0)
		hi1 := uint32(p1 >> 32)
		lo1 := uint32(p1)
		c = [4]uint32{
			hi1 ^ c[1] ^ key0,
			lo1,
			hi0 ^ c[3] ^ key1,
			lo0,
		}
	}
	return c
}

// U32 is a single addressable uniform uint32.
// Key material is (seed, feature, entity). Counter is (chunk, sample, lane, entity).
func U32(seed uint64, feature, entity, chunk, sample, lane uint32) uint32 {
	k0, k1 := Sum32(seed, uint64(feature)<<32|uint64(entity))
	out := Four(k0, k1, [4]uint32{chunk, sample, lane, entity})
	return out[0]
}

// Below returns true when U32 % mod < n. n and mod must be > 0.
func Below(seed uint64, feature, entity, chunk, sample, lane, n, mod uint32) bool {
	if mod == 0 {
		return false
	}
	return U32(seed, feature, entity, chunk, sample, lane)%mod < n
}
