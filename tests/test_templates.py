import unittest
from condorpy import Templates
from condorpy import Job


def load_tests(loader, tests, pattern):
    return unittest.TestLoader().loadTestsFromTestCase(TestTemplates)


class TestTemplates(unittest.TestCase):

    def test_base(self):
        base = Templates.base
        job = Job('newJob',Templates.base)
        print job
        job.set('job_name','')
        self.assertEqual(job._attributes, base)
        job.set('executable','test.py')
        print job

        job2 = Job('job2',Templates.base)
        job2.set('job_name','')
        self.assertEqual(job2._attributes, base)

        vanilla_job = Job('vanilla', Templates.vanilla_transfer_files)
        print vanilla_job