#!/usr/bin/env python3
"""
Demo: Distributed URL Shortener.

Creates a simulated cluster of storage nodes, distributes short URLs
across them via consistent hashing with replication, kills a node and
shows reads still succeed from a surviving replica, then adds a node
and measures how few keys actually had to move -- compared against
what naive mod-N hashing would have required.

Run with:  python demo.py
"""
from collections import Counter

from dus.naive import naive_mod_node
from dus.router import Router


def print_distribution(router: Router, codes):
    counts = Counter()
    for code in codes:
        pref = router.preference_list(code)
        counts[pref[0]] += 1
    for node_id in sorted(router.nodes):
        print(f"    {node_id}: {counts.get(node_id, 0)} primary keys")


def main():
    print("=" * 72)
    print("Distributed URL Shortener -- demo")
    print("=" * 72)

    NUM_NODES = 6
    REPLICAS = 3
    NUM_URLS = 300

    router = Router(replicas=REPLICAS, vnodes=150)
    for i in range(NUM_NODES):
        router.add_node(f"node-{i}")

    print(f"\n[1] Cluster started with {NUM_NODES} nodes, replication factor R={REPLICAS}.")

    urls = [f"https://example.com/article/{i}" for i in range(NUM_URLS)]
    pairs = [(router.create(url), url) for url in urls]
    codes = [c for c, _ in pairs]

    print(f"\n[2] Created {NUM_URLS} short URLs. Primary-key distribution across nodes:")
    print_distribution(router, codes)

    sample_code, sample_url = pairs[0]
    pref = router.preference_list(sample_code)
    print(f"\n    Example: short_code={sample_code!r} -> {sample_url}")
    print(f"    Preference list (primary + {REPLICAS - 1} replicas): {pref}")

    # --- simulate a node failure -----------------------------------------
    victim = pref[0]
    print(f"\n[3] Simulating failure of node {victim!r} (primary for {sample_code!r})...")
    router.mark_down(victim)

    url = router.read(sample_code)
    assert url == sample_url
    print(f"    Read for {sample_code!r} still succeeded via a surviving replica -> {url}")

    ok = sum(1 for code, expected in pairs if _safe_read(router, code) == expected)
    print(
        f"    {ok}/{NUM_URLS} keys still readable with node down "
        f"({ok / NUM_URLS:.1%}) -- fault tolerance from replication."
    )

    router.mark_up(victim)
    print(f"    Node {victim!r} restored.")

    # --- add a node and measure remapping ---------------------------------
    print(f"\n[4] Adding a 7th node to the cluster...")
    before = {code: router.preference_list(code)[0] for code in codes}
    router.add_node("node-new")
    moved_to_new = router.rebalance_after_add("node-new")
    after = {code: router.preference_list(code)[0] for code in codes}

    moved = sum(1 for c in codes if before[c] != after[c])
    frac_ring = moved / len(codes)
    print(
        f"    Consistent hashing: {moved}/{len(codes)} primary-key assignments changed "
        f"({frac_ring:.1%}); {moved_to_new} records physically copied to node-new."
    )

    naive_before = {code: naive_mod_node(code, NUM_NODES) for code in codes}
    naive_after = {code: naive_mod_node(code, NUM_NODES + 1) for code in codes}
    naive_moved = sum(1 for c in codes if naive_before[c] != naive_after[c])
    frac_naive = naive_moved / len(codes)
    print(f"    Naive mod-N hashing would have remapped: {naive_moved}/{len(codes)} keys ({frac_naive:.1%})")

    improvement = frac_naive / max(frac_ring, 1e-9)
    print(
        f"\n    --> Consistent hashing moved {frac_ring:.1%} of keys vs "
        f"{frac_naive:.1%} for naive mod-N: roughly a {improvement:.1f}x reduction "
        f"in data movement for a single node join."
    )

    print(f"\n[5] Final distribution across all {NUM_NODES + 1} nodes:")
    print_distribution(router, codes)

    for code, expected in pairs:
        got = router.read(code)
        assert got == expected, f"data loss detected for {code}!"
    print(f"\n[6] Verified all {NUM_URLS} URLs still resolve correctly after rebalance -- no data lost.")

    print("\n" + "=" * 72)
    print("Demo complete.")
    print("=" * 72)


def _safe_read(router: Router, code: str):
    try:
        return router.read(code)
    except Exception:
        return None


if __name__ == "__main__":
    main()
