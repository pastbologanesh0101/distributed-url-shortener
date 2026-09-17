"""
Consistent hashing ring.

Physical nodes are hashed to many points ("virtual nodes") around a
circular hash space of size 2**128 (we use MD5 as a stable, well-spread
hash function -- this is not for security, just uniform distribution).
A key is owned by whichever node's virtual-node point is the first one
clockwise from the key's own hash position.

Virtual nodes matter because with only a handful of points per physical
node, the ring can be badly unbalanced (some nodes get far more of the
keyspace than others). With ~100+ virtual nodes per physical node, load
spreads much more evenly.

The core property this module exists to provide: adding or removing a
single node only remaps keys that fall in the (small) arc(s) owned by
that node -- not the whole keyspace. See tests/test_ring.py for a
quantitative proof, and dus/naive.py for the mod-N approach this beats.
"""
from __future__ import annotations

import bisect
import hashlib
from typing import Dict, List, Optional


def stable_hash(key: str) -> int:
    """Deterministic, uniformly-distributed hash of a string, as an int."""
    return int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16)


class ConsistentHashRing:
    """A consistent hashing ring with virtual nodes.

    Usage:
        ring = ConsistentHashRing(vnodes=100)
        ring.add_node("node-0")
        ring.add_node("node-1")
        owner = ring.get_node("my-short-code")
        replicas = ring.get_preference_list("my-short-code", 3)
    """

    def __init__(self, vnodes: int = 100):
        if vnodes < 1:
            raise ValueError("vnodes must be >= 1")
        self.vnodes = vnodes
        self._ring: Dict[int, str] = {}          # hash point -> node_id
        self._sorted_points: List[int] = []       # sorted keys of _ring
        self._nodes: List[str] = []                # physical node ids, insertion order

    def _points_for(self, node_id: str) -> List[int]:
        return [stable_hash(f"{node_id}#vn{i}") for i in range(self.vnodes)]

    def add_node(self, node_id: str) -> None:
        """Add a physical node to the ring (no-op if already present)."""
        if node_id in self._nodes:
            return
        self._nodes.append(node_id)
        for point in self._points_for(node_id):
            self._ring[point] = node_id
        self._sorted_points = sorted(self._ring.keys())

    def remove_node(self, node_id: str) -> None:
        """Remove a physical node from the ring (no-op if absent)."""
        if node_id not in self._nodes:
            return
        self._nodes.remove(node_id)
        for point in self._points_for(node_id):
            self._ring.pop(point, None)
        self._sorted_points = sorted(self._ring.keys())

    @property
    def nodes(self) -> List[str]:
        return list(self._nodes)

    def get_node(self, key: str) -> Optional[str]:
        """Return the single owning (primary) node for a key, or None if
        the ring is empty."""
        pref = self.get_preference_list(key, 1)
        return pref[0] if pref else None

    def get_preference_list(self, key: str, count: int) -> List[str]:
        """Return up to `count` distinct physical nodes, in ring order,
        starting from the primary owner of `key`. Used to place the
        primary copy plus R-1 replicas of a key.
        """
        if not self._sorted_points or count <= 0:
            return []
        h = stable_hash(key)
        start = bisect.bisect(self._sorted_points, h)
        n = len(self._sorted_points)
        target = min(count, len(self._nodes))

        result: List[str] = []
        seen = set()
        for step in range(n):
            idx = (start + step) % n
            node_id = self._ring[self._sorted_points[idx]]
            if node_id not in seen:
                seen.add(node_id)
                result.append(node_id)
                if len(result) == target:
                    break
        return result
