# Distributed URL Shortener

A **simulated distributed system**, not just another CRUD app. This
project shows how a URL shortener would actually be sharded and
replicated across a cluster of nodes: consistent hashing, replica
placement, coordinator-style routing, and failover when a node goes
down — all running in one Python process as multiple in-memory "node"
objects, so it's runnable and testable with zero infrastructure.

There is a separate, much simpler single-node Flask+SQLite URL
shortener (`~/url-shortener`) that is a plain CRUD app. This repo is
deliberately about the distributed-systems mechanics instead: sharding,
consistent hashing, replication, and node failure/recovery.

## Why consistent hashing instead of naive `hash(key) % N`?

The obvious way to shard `N` nodes is `node_index = hash(key) % N`. It's
perfectly balanced, but it has a fatal flaw for a live system: **when
`N` changes, almost every key's assignment changes too**, because the
modulus itself changed. Adding a 7th node to a 6-node cluster means
`hash(key) % 6` and `hash(key) % 7` agree only for a small, essentially
random subset of keys — in practice this project's demo shows naive
mod-N remapping **~87%** of keys for a single node join. In a real
system that means re-copying almost the entire dataset just to add one
machine.

**Consistent hashing** fixes this by hashing both nodes and keys onto
the same circular hash space (a "ring"). Each key is owned by the first
node clockwise from its hash position. When a node joins or leaves, it
only takes over (or gives up) the narrow arc of the ring it newly
owns/vacates — every other key's owner is untouched. This project's
demo shows the same node-join only remaps **~13%** of keys — roughly a
**6-7x** reduction in data movement, and it only gets better as the
cluster grows (expected fraction moved on an add is ~`1/(N+1)`).

Each physical node is also hashed to ~100-150 *virtual nodes* scattered
around the ring, which is what keeps load roughly balanced across
physical nodes despite the ring being populated by a handful of
pseudo-random hash points.

`dus/naive.py` implements the naive mod-N scheme purely as a comparison
baseline — it's exercised directly in `tests/test_ring.py` and
`demo.py` to make the contrast quantitative, not just asserted.

## Architecture

```
                         ┌─────────────────────────┐
   client (demo.py) ---> │        Router            │   coordinator:
                         │  (dus/router.py)          │   - computes preference
                         │                           │     list via the ring
                         │   ┌───────────────────┐   │   - fans writes out to
                         │   │ ConsistentHashRing │   │     R replicas
                         │   │   (dus/ring.py)     │   │   - reads/redirects fail
                         │   └───────────────────┘   │     over past down nodes
                         └───────────┬───────────────┘
                                     │  preference_list(short_code)
                                     │  = [primary, replica_1, replica_2, ...]
              ┌──────────────┬──────┴───────┬──────────────┬──────────────┐
              ▼              ▼              ▼              ▼              ▼
        ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐
        │ node-0   │   │ node-1   │   │ node-2   │   │ node-3   │   │ node-4   │  ...
        │(dus/node │   │          │   │  (down!) │   │          │   │          │
        │   .py)   │   │          │   │  reads   │   │          │   │          │
        │ short_   │   │ short_   │   │ failover │   │ short_   │   │ short_   │
        │ code ->  │   │ code ->  │   │  past    │   │ code ->  │   │ code ->  │
        │ (url,    │   │ (url,    │   │  it to a │   │ (url,    │   │ (url,    │
        │  clicks) │   │  clicks) │   │  replica │   │  clicks) │   │  clicks) │
        └──────────┘   └──────────┘   └──────────┘   └──────────┘   └──────────┘
```

- **`ConsistentHashRing`** (`dus/ring.py`) — maps short codes and node
  ids onto a shared circular hash space using virtual nodes; computes a
  key's ordered *preference list* (primary + R-1 replicas).
- **`StorageNode`** (`dus/node.py`) — an independent in-memory shard:
  `short_code -> (long_url, click_count)`. Can be marked up/down to
  simulate a crash; raises `NodeDownError` for client ops while down.
- **`Router`** (`dus/router.py`) — the coordinator. Computes preference
  lists, replicates writes to all live replicas, serves reads/redirects
  with failover past down nodes, and rebalances the cluster after a
  node join (`rebalance_after_add`).
- **`naive_mod_node`** (`dus/naive.py`) — the `hash(key) % N` baseline
  used only for comparison in tests and the demo.

### Replication & consistency model

Every key is written to `R` replicas (primary + `R-1` clockwise
neighbors on the ring). Reads try replicas in preference order and
return the first hit, skipping any that are down. `redirect()` (used
for click tracking) increments the click count on every replica it can
currently reach — a replica that's down when a click happens misses
that click until it's back up and resynced. This is an **eventual
consistency** model for click counts, favoring availability over strict
linearizability, which is directly asserted in
`test_click_counts_are_eventually_consistent_across_replicas`.

### Node failure & rebalancing

- `router.mark_down(node_id)` / `mark_up(node_id)` simulate a crash and
  recovery. Reads/writes automatically fail over to surviving replicas
  while a node is down.
- `router.rebalance_after_add(new_node_id)` re-syncs the cluster after
  a join: it copies any key whose preference list now includes the new
  node, and prunes stale copies from nodes that no longer own that key
  — converging the cluster to the ring's new mapping while moving only
  the keys that actually need to move.

## Running the demo

```bash
python3 demo.py
```

The cluster shape is configurable via CLI flags instead of editing the
script, e.g. to try a bigger cluster with a lower replication factor
and a reproducible run:

```bash
python3 demo.py --nodes 10 --replicas 2 --urls 1000 --seed 42
```

Run `python3 demo.py --help` for the full flag list (`--nodes`,
`--replicas`, `--urls`, `--vnodes`, `--seed`).

Example output (abridged):

```
[2] Created 300 short URLs. Primary-key distribution across nodes:
    node-0: 64 primary keys
    node-1: 56 primary keys
    ...

[3] Simulating failure of node 'node-1' (primary for 'JkqjR6')...
    Read for 'JkqjR6' still succeeded via a surviving replica -> https://example.com/article/0
    300/300 keys still readable with node down (100.0%) -- fault tolerance from replication.

[4] Adding a 7th node to the cluster...
    Consistent hashing: 39/300 primary-key assignments changed (13.0%); 135 records physically copied to node-new.
    Naive mod-N hashing would have remapped: 260/300 keys (86.7%)

    --> Consistent hashing moved 13.0% of keys vs 86.7% for naive mod-N: roughly a 6.7x reduction
        in data movement for a single node join.

[6] Verified all 300 URLs still resolve correctly after rebalance -- no data lost.
```

## Running the tests

```bash
python3 -m unittest discover -s tests -v
```

20 unit tests cover:
- deterministic key -> node assignment on the ring
- bounded (small) fraction of keys remapped when a node is added, contrasted against naive mod-N hashing remapping nearly everything
- key reassignment without loss when a node is removed
- replication landing on R distinct nodes
- reads succeeding via a surviving replica when a node is down (and failing only when *all* replicas are down)
- short-code creation / read / redirect round-tripping through the router
- click-count behavior under the eventual-consistency model
- rebalance-after-add moving few keys with zero data loss

CI (`.github/workflows/tests.yml`) runs the full suite plus the demo
script as a smoke test on Python 3.11, 3.12, and 3.13 on every
push/PR.

## Troubleshooting / FAQ

**Q: I added a node and `owning_nodes_snapshot()`/reads still show old
data on nodes that shouldn't own those keys anymore. Is that a bug?**

No — `ring.add_node()` only updates the ring's key-to-node *mapping*.
Nothing physically moves data until you call
`router.rebalance_after_add(new_node_id)`, which copies the keys the
new node now owns and prunes stale copies elsewhere. If you call
`add_node` directly on `Router` without following it with
`rebalance_after_add`, the new node is a legitimate ring member but
starts out empty, and old replicas linger until you rebalance. This
mirrors real systems, where ring membership changes and data movement
are separate steps (the latter is often async/throttled).

**Q: I called `router.mark_down(node_id)` but writes/reads to that node
still raise `NodeDownError` even after I never called `mark_up`. Is
something stuck?**

That's expected — `mark_down`/`mark_up` are the *only* things that
flip a node's up/down flag, and they're sticky until you explicitly
call the other one. There's no timeout or auto-recovery, since this
project models the failure/recovery *mechanism* (failover +
resync-on-add), not a specific failure-detector policy.

**Q: Why did `click_counts(short_code)` return different numbers for
different replicas after I brought a node back up with `mark_up`?**

This is the eventual-consistency model working as designed, not a bug:
`redirect()` only increments the click count on replicas that were
*reachable at the time of that specific call*. `mark_up` makes a node
accept traffic again, but it does not retroactively replay clicks it
missed while down. If you need replicas to agree again, you'd need an
explicit resync step (the project doesn't implement click-count
anti-entropy — only `rebalance_after_add` resyncs record
existence/values, not historical click deltas).

**Q: Why does `Router.create()` sometimes raise `NoAvailableReplicaError`
even though the cluster has nodes?**

It only raises that when *every* node in the key's preference list
(primary + replicas) is currently marked down — a partial outage still
succeeds by writing to whichever replicas are up. If you're seeing it
unexpectedly, check whether you marked down more nodes than your
`replicas` count can tolerate.

## Project layout

```
distributed-url-shortener/
├── dus/
│   ├── ring.py     # consistent hashing ring (+ virtual nodes)
│   ├── node.py      # simulated storage node (shard)
│   ├── router.py    # coordinator: routing, replication, failover, rebalance
│   └── naive.py     # naive mod-N baseline, for comparison only
├── tests/
│   ├── test_ring.py
│   ├── test_node.py
│   └── test_router.py
├── demo.py           # end-to-end scripted demo
├── .github/workflows/tests.yml
├── LICENSE
└── README.md
```

Pure Python standard library — no external dependencies.
