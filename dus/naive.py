"""
Naive mod-N hashing, included purely as a comparison baseline.

This is the "obvious" way to shard keys across N nodes: hash the key and
take it mod the node count. It is perfectly balanced -- but when N
changes (a node joins or leaves), almost every key's `hash(key) % N`
result changes too, since the modulus itself changed. That forces a
near-total reshuffle/resync of data across the cluster for a single
node join/leave, which is exactly what consistent hashing (dus/ring.py)
avoids. tests/test_ring.py quantifies the difference directly.
"""
from .ring import stable_hash


def naive_mod_node(key: str, num_nodes: int) -> int:
    """Return the index (0..num_nodes-1) of the node owning `key` under
    naive mod-N hashing."""
    if num_nodes <= 0:
        raise ValueError("num_nodes must be > 0")
    return stable_hash(key) % num_nodes
