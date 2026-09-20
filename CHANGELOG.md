# Changelog

## v0.1.0 -- Initial release

The initial commit (`3b6bb2a`) established the whole simulated
distributed URL shortener in one pass:

- **`dus/ring.py`** -- `ConsistentHashRing`: MD5-based consistent
  hashing with ~100-150 virtual nodes per physical node, and
  `get_preference_list()` for computing an ordered primary+replica
  list for a key.
- **`dus/node.py`** -- `StorageNode`: an in-memory, thread-lock-guarded
  shard (`short_code -> Record(long_url, click_count)`) that can be
  marked up/down to simulate a crash, raising `NodeDownError` for
  client operations while down; `dump`/`raw_put`/`raw_delete` bypass
  the up-check for inter-node resync traffic.
- **`dus/router.py`** -- `Router`: the coordinator. Computes
  preference lists, replicates writes to all live replicas, serves
  reads/redirects with failover past down nodes, tracks click counts
  under an eventual-consistency model, and rebalances the cluster
  after a node join via `rebalance_after_add`.
- **`dus/naive.py`** -- `naive_mod_node`: the `hash(key) % N` baseline
  used only as a quantitative comparison against consistent hashing.
- **`demo.py`** -- end-to-end scripted demo: builds a 6-node cluster
  with replication factor 3, creates 300 short URLs, kills a node and
  shows reads still succeed via a surviving replica, adds a 7th node
  and measures the fraction of keys remapped versus what naive mod-N
  hashing would have required.
- **Tests** (`tests/test_ring.py`, `tests/test_node.py`,
  `tests/test_router.py`) -- 20 unit tests covering deterministic
  key assignment, bounded remapping on node add/remove, replication
  landing on distinct nodes, failover on node-down, eventual
  consistency of click counts, and rebalance-after-add correctness.
- **CI** (`.github/workflows/tests.yml`) -- runs the unit test suite
  and the demo script as a smoke test on Python 3.11 and 3.12, on
  every push and pull request.
- **`LICENSE`** -- MIT.
- **`README.md`** -- project overview, the "why consistent hashing"
  argument with real numbers from the demo, architecture diagram, and
  running/testing instructions.

## Unreleased

- Added `CONTRIBUTING.md`.
- Added tests for empty-ring lookups and node removal.
- `Router.create()` now rejects a blank `long_url` or a blank custom
  `short_code` with `ValueError` instead of silently creating an
  unusable entry.
- Added a Troubleshooting/FAQ section to the README.
