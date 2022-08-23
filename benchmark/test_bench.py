import unittest
from collections import namedtuple
from benchmark.bench import is_pr, is_version_tag


class TestBench(unittest.TestCase):
    Test = namedtuple("Test", ["input", "expected"])

    def test_is_pr(self):
        test_cases = [
            self.Test("478", True),
            self.Test("10", True),
            self.Test("0", False),
            self.Test(None, False),
            self.Test("", False),
        ]
        for test in test_cases:
            self.assertEqual(is_pr(test.input), test.expected)

    def test_is_version_tag(self):
        test_cases = [
            self.Test("v1.0.0", True),
            self.Test("v2.0.0", True),
            self.Test("v1.4.0", True),
            self.Test("v2.0.0-beta", True),
            self.Test("v2.0.0-beta.2", True),
            self.Test("v1.4.0-rc.1", True),
            self.Test("v.1.4.0", False),
            self.Test("", False),
            self.Test(None, False),
        ]
        for test in test_cases:
            self.assertEqual(is_version_tag(test.input), test.expected)
