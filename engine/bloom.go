package engine

const (
	bloomBitsPerKey = 10
	bloomProbes     = 7
)

// hashKey is inline FNV-1a (64 bit), allocation free.
func hashKey(key []byte) uint64 {
	h := uint64(14695981039346656037)
	for _, c := range key {
		h ^= uint64(c)
		h *= 1099511628211
	}
	return h
}

// Bloom filter layout: probes(1) | bits.
func buildBloom(hashes []uint64) []byte {
	nbits := len(hashes) * bloomBitsPerKey
	if nbits < 64 {
		nbits = 64
	}
	nbytes := (nbits + 7) / 8
	nbits = nbytes * 8
	b := make([]byte, 1+nbytes)
	b[0] = bloomProbes
	for _, h := range hashes {
		h1, h2 := uint32(h), uint32(h>>32)|1
		for i := uint32(0); i < bloomProbes; i++ {
			pos := (h1 + i*h2) % uint32(nbits)
			b[1+pos/8] |= 1 << (pos % 8)
		}
	}
	return b
}

func bloomMayContain(b []byte, h uint64) bool {
	if len(b) < 2 {
		return true
	}
	nbits := uint32(len(b)-1) * 8
	h1, h2 := uint32(h), uint32(h>>32)|1
	for i := uint32(0); i < uint32(b[0]); i++ {
		pos := (h1 + i*h2) % nbits
		if b[1+pos/8]&(1<<(pos%8)) == 0 {
			return false
		}
	}
	return true
}
