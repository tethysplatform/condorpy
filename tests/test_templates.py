import unittest
from condorpy import templates


def load_tests(loader, tests, pattern):
    return unittest.TestLoader().loadTestsFromTestCase(JobTest)


class TestTemplates(unittest.TestCase):

    def test_base(self):
        self.fail()