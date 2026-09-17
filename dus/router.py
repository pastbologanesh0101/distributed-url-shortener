"""
Coordinator / Router.

Given a short code, the router uses the consistent hash ring to compute
the key's *preference list*: the primary node plus (R-1) replica nodes,
walking clockwise around the ring. Writes go to all live nodes in the
preference list; reads/redirects try them in order and fail over to the
next replica if one is down (simulating a crashed/partitioned node).

Consistency model: click counts use an eventual-consistency model. A
redirect() increments the click count on every currently-live replica
it can reach; a replica that was down at the time of a click will not
have that click counted until it is resynced. This mirrors a common
real-world tradeoff (e.g. Dynamo-style stores): favor availability of
reads/writes over strict linearizability of counters. This is
intentional and is directly exercised by
test_click_counts_eventually_consistent_when_replica_down in
tests/test_router.py.
"""
from __future__ import annotations

import random
import string
import threading
from typing import Dict, List, Optional, Tuple

from .node import NodeDownError, StorageNode
from .ring import ConsistentHashRing


class NoAvailableReplicaError(Exception):
    """Raised when no live replica in a key's preference list could
    serve a read or accept a write."""


class Router:
    def __init__(self, replicas: int = 3, vnodes: int = 100):
        if replicas < 1:
            raise ValueError("replicas must be >= 1")
        self.replicas = replicas
        self.ring = ConsistentHashRing(vnodes=vnodes)
        self.nodes: Dict[str, StorageNode] = {}
        self._lock = threading.Lock()

    # ---- cluster membership ------------------------------------------------

    def add_node(self, node_id: str) -> StorageNode:
        with self._lock:
            node = self.nodes.get(node_id) or StorageNode(node_id)
            self.nodes[node_id] = node
            self.ring.add_node(node_id)
            return node

    def remove_node(self, node_id: str) -> None:
        with self._lock:
            self.ring.remove_node(node_id)
            self.nodes.pop(node_id, None)

    def mark_down(self, node_id: str) -> None:
        self.nodes[node_id].set_up(False)

    def mark_up(self, node_id: str) -> None:
        self.nodes[node_id].set_up(True)

    # ---- key routing --------------------------------------------------------

    def preference_list(self, short_code: str) -> List[str]:
        """The ordered list of node ids (primary first) responsible for a
        key: primary + (R-1) replicas."""
        return self.ring.get_preference_list(short_code, self.replicas)

    # ---- create / read / redirect -------------------------------------------

    @staticmethod
    def generate_code(length: int = 6) -> str:
        alphabet = string.ascii_letters + string.digits
        return "".join(random.choice(alphabet) for _ in range(length))

    def create(self, long_url: str, short_code: Optional[str] = None) -> str:
        """Create a short code for long_url, writing it to all live nodes
        in its preference list. Raises NoAvailableReplicaError only if
        every replica is down."""
        if short_code is None:
            short_code = self.generate_code()
        pref = self.preference_list(short_code)
        if not pref:
            raise RuntimeError("no nodes in cluster")

        wrote = 0
        for node_id in pref:
            try:
                self.nodes[node_id].put(short_code, long_url)
                wrote += 1
            except NodeDownError:
                continue
        if wrote == 0:
            raise NoAvailableReplicaError(f"could not write {short_code!r} to any replica")
        return short_code

    def read(self, short_code: str) -> str:
        """Read a long_url by short_code, trying replicas in preference
        order and failing over past any that are down."""
        pref = self.preference_list(short_code)
        for node_id in pref:
            try:
                rec = self.nodes[node_id].get(short_code)
            except NodeDownError:
                continue
            if rec is not None:
                return rec.long_url
        raise NoAvailableReplicaError(f"no live replica holds {short_code!r}")

    def redirect(self, short_code: str) -> str:
        """Read a long_url and increment the click count on every
        currently-reachable replica (eventual consistency -- see module
        docstring)."""
        pref = self.preference_list(short_code)
        long_url: Optional[str] = None
        for node_id in pref:
            node = self.nodes[node_id]
            try:
                rec = node.get(short_code)
            except NodeDownError:
                continue
            if rec is None:
                continue
            if long_url is None:
                long_url = rec.long_url
            try:
                node.increment_click(short_code)
            except NodeDownError:
                pass
        if long_url is None:
            raise NoAvailableReplicaError(f"no live replica holds {short_code!r}")
        return long_url

    def click_counts(self, short_code: str) -> Dict[str, int]:
        """Return the click count as seen by each currently-live replica,
        keyed by node id -- useful for observing eventual consistency."""
        pref = self.preference_list(short_code)
        result: Dict[str, int] = {}
        for node_id in pref:
            try:
                rec = self.nodes[node_id].get(short_code)
            except NodeDownError:
                continue
            if rec is not None:
                result[node_id] = rec.click_count
        return result

    def owning_nodes_snapshot(self) -> Dict[str, List[str]]:
        """Debug/demo helper: node_id -> list of short_codes currently
        stored on that node (live nodes only)."""
        out: Dict[str, List[str]] = {}
        for node_id, node in self.nodes.items():
            try:
                out[node_id] = node.keys()
            except NodeDownError:
                out[node_id] = []
        return out

    # ---- rebalancing on membership change ------------------------------------

    def rebalance_after_add(self, new_node_id: str) -> int:
        """
        Resync the cluster after a node has been added to the ring.

        Scans every record currently held anywhere in the cluster (a
        stand-in for what a real system would stream via inter-node
        replication/anti-entropy), and for each key whose preference list
        now includes the new node, copies it there if missing. It also
        prunes copies sitting on nodes that no longer belong to that key's
        (new) preference list, so the cluster converges to the ring's
        current mapping.

        Returns the number of records copied onto the new node -- this is
        the quantity the demo/tests compare against a naive mod-N
        rehash, which would require touching nearly every key.
        """
        all_records: Dict[str, Tuple[str, int]] = {}
        for node in self.nodes.values():
            for code, rec in node.dump().items():
                if code not in all_records or rec.click_count > all_records[code][1]:
                    all_records[code] = (rec.long_url, rec.click_count)

        moved = 0
        for code, (long_url, clicks) in all_records.items():
            pref = self.preference_list(code)

            if new_node_id in pref:
                new_node = self.nodes[new_node_id]
                if code not in new_node.dump():
                    new_node.raw_put(code, long_url, clicks)
                    moved += 1

            for node_id, node in self.nodes.items():
                if node_id != new_node_id and node_id not in pref and code in node.dump():
                    node.raw_delete(code)

        return moved
