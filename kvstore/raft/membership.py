"""Cluster membership: who is in the cluster and who votes.

A Membership is immutable. It is replicated as KIND_CONFIG log entries and
persisted as the "base" configuration (the one in effect at the start of the
retained log) in the `members` file of the node directory.

Changes follow the single-server rule (Raft dissertation, section 4.1): one
node is added or promoted at a time, so any two consecutive configurations
share a majority. A new node first joins as a *learner* (replicated to, not
counted for quorum or elections) and becomes a voter once it has caught up.
"""

import os
import struct
import zlib
from typing import Dict, FrozenSet, Optional, Tuple

from ..util import VarintError, append_uvarint, read_uvarint, write_file_atomic

MEMBERS_FILE = "members"


class Membership:
    """members maps node ID -> opaque info (the replica layer keeps the node's
    addresses there); voters is the subset of IDs that vote."""

    __slots__ = ("members", "voters")

    def __init__(self, members: Optional[Dict[str, str]] = None, voters=()):
        self.members: Dict[str, str] = dict(members or {})
        self.voters: FrozenSet[str] = frozenset(voters)
        if not self.voters <= self.members.keys():
            raise ValueError("raft: voters must be members")

    def __eq__(self, o) -> bool:
        return isinstance(o, Membership) and self.members == o.members and self.voters == o.voters

    def __repr__(self) -> str:
        return "Membership(voters=%s, learners=%s)" % (sorted(self.voters), sorted(self.learners()))

    def learners(self):
        return sorted(set(self.members) - self.voters)

    def with_learner(self, nid: str, info: str) -> "Membership":
        m = dict(self.members)
        m[nid] = info
        return Membership(m, self.voters)

    def without(self, nid: str) -> "Membership":
        m = {k: v for k, v in self.members.items() if k != nid}
        return Membership(m, self.voters - {nid})

    def with_voter(self, nid: str) -> "Membership":
        if nid not in self.members:
            raise KeyError(nid)
        return Membership(self.members, self.voters | {nid})

    def encode(self) -> bytes:
        b = bytearray()
        append_uvarint(b, len(self.members))
        for nid in sorted(self.members):
            for s in (nid, self.members[nid]):
                raw = s.encode()
                append_uvarint(b, len(raw))
                b += raw
            b.append(1 if nid in self.voters else 0)
        return bytes(b)

    @staticmethod
    def decode(data: bytes) -> "Membership":
        try:
            n, pos = read_uvarint(data, 0)
            members, voters = {}, []
            for _ in range(n):
                parts = []
                for _ in range(2):
                    ln, pos = read_uvarint(data, pos)
                    if ln > len(data) - pos:
                        raise ValueError("raft: malformed membership")
                    parts.append(bytes(data[pos:pos + ln]).decode())
                    pos += ln
                if pos >= len(data):
                    raise ValueError("raft: malformed membership")
                members[parts[0]] = parts[1]
                if data[pos]:
                    voters.append(parts[0])
                pos += 1
            return Membership(members, voters)
        except VarintError:
            raise ValueError("raft: malformed membership") from None


_HDR = struct.Struct("<QI")  # index, crc32 of the encoded membership


def save_base(directory: str, index: int, m: Membership) -> None:
    """Durably record that `m` is the configuration in effect at log `index`."""
    body = m.encode()
    write_file_atomic(os.path.join(directory, MEMBERS_FILE), _HDR.pack(index, zlib.crc32(body)) + body,
                      directory)


def load_base(directory: str) -> Optional[Tuple[int, Membership]]:
    try:
        with open(os.path.join(directory, MEMBERS_FILE), "rb") as f:
            data = f.read()
    except FileNotFoundError:
        return None
    if len(data) < _HDR.size:
        raise ValueError("raft members: truncated")
    index, crc = _HDR.unpack_from(data, 0)
    body = data[_HDR.size:]
    if zlib.crc32(body) != crc:
        raise ValueError("raft members: corrupt")
    return index, Membership.decode(body)
