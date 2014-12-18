import unittest
from condorpy import templates
from condorpy import Job


def load_tests(loader, tests, pattern):
    return unittest.TestLoader().loadTestsFromTestCase(TestTemplates)


class TestTemplates(unittest.TestCase):

    def test_base(self):
        job = Job('newJob',templates.base())
        print job