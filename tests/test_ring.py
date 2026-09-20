import unittest

from dus.naive import naive_mod_node
from dus.ring import ConsistentHashRing


def make_ring(node_count: int, vnodes: int = 100) -> ConsistentHashRing:
    ring = ConsistentHashRing(vnodes=vnodes)
    for i in range(node_count):
        ring.add_node(f"node-{i}")
    return ring


class TestConsistentHashRing(unittest.TestCase):
    def test_deterministic_assignment(self):
        """The same key must always resolve to the same node while the
        ring is unchanged."""
        ring = make_ring(5)
        keys = [f"key{i}" for i in range(200)]
        first = [ring.get_node(k) for k in keys]
        second = [ring.get_node(k) for k in keys]
        self.assertEqual(first, second)

    def test_all_nodes_receive_keys(self):
        """With enough keys and virtual nodes, every physical node should
        end up owning at least one key (no node is starved)."""
        ring = make_ring(5)
        keys = [f"key{i}" for i in range(2000)]
        assigned = {ring.get_node(k) for k in keys}
        self.assertEqual(assigned, set(ring.nodes))

    def test_adding_node_remaps_small_bounded_fraction(self):
        """Adding one node to a 10-node ring should remap well under half
        the keys -- the defining property of consistent hashing."""
        node_count = 10
        ring = make_ring(node_count)
        keys = [f"url-{i}" for i in range(5000)]
        before = {k: ring.get_node(k) for k in keys}

        ring.add_node("node-new")
        after = {k: ring.get_node(k) for k in keys}

        moved = sum(1 for k in keys if before[k] != after[k])
        fraction_moved = moved / len(keys)

        # Theoretical expectation is close to 1/(node_count+1) ~= 9%.
        # Assert generously to avoid flakiness while still proving the point.
        self.assertGreater(fraction_moved, 0.0)
        self.assertLess(fraction_moved, 0.5)

    def test_removing_node_reassigns_without_loss(self):
        """Removing a node must not lose any key -- every key still
        resolves to some remaining live node, and only keys that were
        actually on the removed node should move."""
        ring = make_ring(6)
        keys = [f"url-{i}" for i in range(1000)]
        before = {k: ring.get_node(k) for k in keys}

        ring.remove_node("node-2")
        after = {k: ring.get_node(k) for k in keys}

        self.assertTrue(all(v in ring.nodes for v in after.values()))
        for k in keys:
            if before[k] != "node-2":
                self.assertEqual(before[k], after[k])
            else:
                self.assertNotEqual(after[k], "node-2")

    def test_preference_list_returns_distinct_nodes_in_ring_order(self):
        ring = make_ring(8)
        pref = ring.get_preference_list("some-key", 3)
        self.assertEqual(len(pref), 3)
        self.assertEqual(len(set(pref)), 3)
        # first entry of preference list must equal the primary owner
        self.assertEqual(pref[0], ring.get_node("some-key"))

    def test_preference_list_caps_at_available_node_count(self):
        ring = make_ring(2)
        pref = ring.get_preference_list("k", 5)
        self.assertEqual(len(pref), 2)

    def test_consistent_hashing_beats_naive_mod_n_on_node_add(self):
        """This is the key comparative result: consistent hashing remaps
        a small fraction of keys on node add, while naive mod-N hashing
        remaps almost all of them."""
        node_count = 10
        ring = make_ring(node_count)
        keys = [f"url-{i}" for i in range(5000)]

        before_ring = {k: ring.get_node(k) for k in keys}
        ring.add_node("node-new")
        after_ring = {k: ring.get_node(k) for k in keys}
        frac_ring = sum(1 for k in keys if before_ring[k] != after_ring[k]) / len(keys)

        before_naive = {k: naive_mod_node(k, node_count) for k in keys}
        after_naive = {k: naive_mod_node(k, node_count + 1) for k in keys}
        frac_naive = sum(1 for k in keys if before_naive[k] != after_naive[k]) / len(keys)

        self.assertLess(frac_ring, 0.3)
        self.assertGreater(frac_naive, 0.8)
        self.assertLess(frac_ring, frac_naive)

    def test_empty_ring_returns_no_owner(self):
        """A ring with no nodes added yet must fail closed: no primary
        owner and no preference list, rather than raising or returning
        a stale/garbage node id."""
        ring = ConsistentHashRing(vnodes=10)
        self.assertIsNone(ring.get_node("some-key"))
        self.assertEqual(ring.get_preference_list("some-key", 3), [])

    def test_naive_mod_node_is_deterministic_and_spreads_keys(self):
        self.assertEqual(naive_mod_node("abc", 4), naive_mod_node("abc", 4))
        buckets = {naive_mod_node(f"k{i}", 4) for i in range(50)}
        self.assertGreater(len(buckets), 1)
        self.assertTrue(all(0 <= b < 4 for b in buckets))


if __name__ == "__main__":
    unittest.main()
