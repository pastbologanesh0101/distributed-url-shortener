import random
import unittest

import demo


class TestDemoCli(unittest.TestCase):
    def test_parse_args_defaults_match_original_hardcoded_demo(self):
        args = demo.parse_args([])
        self.assertEqual(args.nodes, 6)
        self.assertEqual(args.replicas, 3)
        self.assertEqual(args.urls, 300)
        self.assertEqual(args.vnodes, 150)
        self.assertIsNone(args.seed)

    def test_parse_args_accepts_overrides(self):
        args = demo.parse_args(["--nodes", "10", "--replicas", "2", "--urls", "50", "--seed", "42"])
        self.assertEqual(args.nodes, 10)
        self.assertEqual(args.replicas, 2)
        self.assertEqual(args.urls, 50)
        self.assertEqual(args.seed, 42)

    def test_seed_makes_generated_short_codes_reproducible(self):
        """The --seed flag exists so a demo run can be reproduced --
        verify it actually makes Router.generate_code deterministic."""
        from dus.router import Router

        random.seed(123)
        first = [Router.generate_code() for _ in range(5)]

        random.seed(123)
        second = [Router.generate_code() for _ in range(5)]

        self.assertEqual(first, second)


if __name__ == "__main__":
    unittest.main()
