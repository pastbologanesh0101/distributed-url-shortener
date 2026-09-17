import unittest

from dus.router import NoAvailableReplicaError, Router


class TestRouterBasics(unittest.TestCase):
    def setUp(self):
        self.router = Router(replicas=3, vnodes=100)
        for i in range(5):
            self.router.add_node(f"node-{i}")

    def test_create_and_read_round_trip(self):
        code = self.router.create("https://example.com/foo")
        self.assertEqual(self.router.read(code), "https://example.com/foo")

    def test_custom_short_code_round_trip(self):
        code = self.router.create("https://example.com/bar", short_code="mybar1")
        self.assertEqual(code, "mybar1")
        self.assertEqual(self.router.read("mybar1"), "https://example.com/bar")

    def test_replication_writes_land_on_r_distinct_nodes(self):
        self.router.create("https://example.com/baz", short_code="repltest")
        pref = self.router.preference_list("repltest")
        self.assertEqual(len(pref), 3)
        self.assertEqual(len(set(pref)), 3)

        holders = [
            node_id
            for node_id, node in self.router.nodes.items()
            if node.get("repltest") is not None
        ]
        self.assertEqual(set(holders), set(pref))

    def test_read_survives_single_node_failure(self):
        self.router.create("https://example.com/qux", short_code="failtest")
        pref = self.router.preference_list("failtest")

        self.router.mark_down(pref[0])
        self.assertEqual(self.router.read("failtest"), "https://example.com/qux")

    def test_read_fails_only_when_all_replicas_down(self):
        self.router.create("https://example.com/quux", short_code="deadtest")
        pref = self.router.preference_list("deadtest")

        for node_id in pref:
            self.router.mark_down(node_id)
        with self.assertRaises(NoAvailableReplicaError):
            self.router.read("deadtest")

        # restoring one replica should immediately make reads succeed again
        self.router.mark_up(pref[0])
        self.assertEqual(self.router.read("deadtest"), "https://example.com/quux")

    def test_redirect_increments_click_count_on_all_live_replicas(self):
        self.router.create("https://example.com/clicks", short_code="clicktest")
        self.router.redirect("clicktest")
        self.router.redirect("clicktest")

        counts = self.router.click_counts("clicktest")
        self.assertEqual(len(counts), 3)
        self.assertTrue(all(c == 2 for c in counts.values()))

    def test_click_counts_are_eventually_consistent_across_replicas(self):
        """Documents/tests our chosen consistency model: a replica that
        is down when a click happens misses that click until it comes
        back and is resynced -- click counts are eventually, not
        strongly, consistent."""
        self.router.create("https://example.com/ec", short_code="ectest")
        pref = self.router.preference_list("ectest")

        self.router.mark_down(pref[0])
        self.router.redirect("ectest")
        self.router.mark_up(pref[0])

        counts = self.router.click_counts("ectest")
        self.assertEqual(counts[pref[0]], 0)
        for node_id in pref[1:]:
            self.assertEqual(counts[node_id], 1)

    def test_rebalance_after_add_moves_few_keys_without_data_loss(self):
        pairs = [
            (self.router.create(f"https://example.com/{i}"), f"https://example.com/{i}")
            for i in range(300)
        ]

        self.router.add_node("node-new")
        moved = self.router.rebalance_after_add("node-new")

        self.assertGreater(moved, 0)
        self.assertLess(moved, len(pairs))

        for code, expected_url in pairs:
            self.assertEqual(self.router.read(code), expected_url)

    def test_create_fails_when_all_target_replicas_down(self):
        pref = self.router.preference_list("willfail")
        for node_id in pref:
            self.router.mark_down(node_id)
        with self.assertRaises(NoAvailableReplicaError):
            self.router.create("https://example.com/nope", short_code="willfail")


if __name__ == "__main__":
    unittest.main()
