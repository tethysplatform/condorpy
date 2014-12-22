import unittest
from condorpy import Templates
from condorpy import Job


def load_tests(loader, tests, pattern):
    return unittest.TestLoader().loadTestsFromTestCase(TestTemplates)


class TestTemplates(unittest.TestCase):

    def setUp(self):
        pass


    def tearDown(self):
        pass

    def test_base(self):
        base = Templates.base
        expected = base
        actual = dict
        msg = 'checking that base is an instance of dict'
        self.assertIsInstance(expected, actual, '%s\nExpected: %s\nActual: %s\n' % (msg, expected, actual))
