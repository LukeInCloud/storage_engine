import hashlib

BLOOM_BITS_PER_KEY = 10
BLOOM_PROBES = 7


def hash_key(key: bytes) -> int:
    """Deterministic 64-bit key hash (BLAKE2b; computed in C, unlike a byte loop)."""
    return int.from_bytes(hashlib.blake2b(key, digest_size=8).digest(), "little")


def build_bloom(hashes) -> bytes:
    """Bloom filter layout: probes(1) | bits."""
    nbits = max(len(hashes) * BLOOM_BITS_PER_KEY, 64)
    nbytes = (nbits + 7) // 8
    nbits = nbytes * 8
    b = bytearray(1 + nbytes)
    b[0] = BLOOM_PROBES
    for h in hashes:
        h1, h2 = h & 0xFFFFFFFF, ((h >> 32) & 0xFFFFFFFF) | 1
        for i in range(BLOOM_PROBES):
            pos = ((h1 + i * h2) & 0xFFFFFFFF) % nbits
            b[1 + pos // 8] |= 1 << (pos % 8)
    return bytes(b)


def bloom_may_contain(b: bytes, h: int) -> bool:
    if len(b) < 2:
        return True
    nbits = (len(b) - 1) * 8
    h1, h2 = h & 0xFFFFFFFF, ((h >> 32) & 0xFFFFFFFF) | 1
    for i in range(b[0]):
        pos = ((h1 + i * h2) & 0xFFFFFFFF) % nbits
        if not b[1 + pos // 8] & (1 << (pos % 8)):
            return False
    return True
