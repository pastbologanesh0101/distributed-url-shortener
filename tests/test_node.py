import unittest

from dus.node import NodeDownError, StorageNode


class TestStorageNode(unittest.TestCase):
    def test_put_get_round_trip(self):
        node = StorageNode("n0")
        node.put("abc123", "https://example.com")
        rec = node.get("abc123")
        self.assertEqual(rec.long_url, "https://example.com")
        self.assertEqual(rec.click_count, 0)

    def test_operations_raise_when_down(self):
        node = StorageNode("n0")
        node.put("abc123", "https://example.com")
        node.set_up(False)
        with self.assertRaises(NodeDownError):
            node.get("abc123")
        with self.assertRaises(NodeDownError):
            node.put("xyz789", "https://example.org")

    def test_raw_operations_bypass_down_check(self):
        node = StorageNode("n0")
        node.set_up(False)
        # raw_put/dump are used for inter-node resync traffic and must
        # work even while the node is marked down for client traffic.
        node.raw_put("abc123", "https://example.com", click_count=5)
        dumped = node.dump()
        self.assertEqual(dumped["abc123"].click_count, 5)


if __name__ == "__main__":
    unittest.main()
