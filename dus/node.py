"""
Simulated storage node.

Each StorageNode is an independent shard: an in-memory
short_code -> Record(long_url, click_count) map, guarded by its own lock
(as if it were a separate process/machine). Nodes can be marked
down/up to simulate crashes and recoveries -- while down, normal
put/get/increment/delete/keys operations raise NodeDownError, which is
how the Router detects failure and fails over to a surviving replica.

raw_put/raw_delete/dump bypass the up-check; the Router uses these
internally to resync data during rebalancing (they represent
administrative/inter-node replication traffic, not client traffic).
"""
from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class Record:
    long_url: str
    click_count: int = 0


class NodeDownError(Exception):
    """Raised when a client operation is attempted against a node that
    has been marked down (simulating a crashed/unreachable node)."""


class StorageNode:
    def __init__(self, node_id: str):
        self.node_id = node_id
        self._store: Dict[str, Record] = {}
        self._lock = threading.RLock()
        self._up = True

    # ---- health -----------------------------------------------------------

    def is_up(self) -> bool:
        return self._up

    def set_up(self, up: bool) -> None:
        self._up = up

    def _check_up(self) -> None:
        if not self._up:
            raise NodeDownError(f"node {self.node_id!r} is down")

    # ---- client-facing operations (require the node to be up) -------------

    def put(self, short_code: str, long_url: str, click_count: int = 0) -> None:
        self._check_up()
        with self._lock:
            self._store[short_code] = Record(long_url=long_url, click_count=click_count)

    def get(self, short_code: str) -> Optional[Record]:
        self._check_up()
        with self._lock:
            rec = self._store.get(short_code)
            return None if rec is None else Record(rec.long_url, rec.click_count)

    def increment_click(self, short_code: str) -> Optional[int]:
        self._check_up()
        with self._lock:
            rec = self._store.get(short_code)
            if rec is None:
                return None
            rec.click_count += 1
            return rec.click_count

    def delete(self, short_code: str) -> None:
        self._check_up()
        with self._lock:
            self._store.pop(short_code, None)

    def keys(self):
        self._check_up()
        with self._lock:
            return list(self._store.keys())

    def __len__(self) -> int:
        with self._lock:
            return len(self._store)

    # ---- internal / administrative operations (bypass the up-check) -------
    # Used by the Router for resync/rebalance traffic, which is modeled as
    # inter-node replication rather than client traffic.

    def dump(self) -> Dict[str, Record]:
        with self._lock:
            return {k: Record(v.long_url, v.click_count) for k, v in self._store.items()}

    def raw_put(self, short_code: str, long_url: str, click_count: int = 0) -> None:
        with self._lock:
            self._store[short_code] = Record(long_url=long_url, click_count=click_count)

    def raw_delete(self, short_code: str) -> None:
        with self._lock:
            self._store.pop(short_code, None)
