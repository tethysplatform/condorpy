'''
Created on Aug 22, 2014

@author: sdc50
'''
import unittest
from condorpy import Job
from condorpy import Templates
import os


def load_tests(loader, tests, pattern):
    return unittest.TestLoader().loadTestsFromTestCase(TestJob)

class TestJob(unittest.TestCase):


    def setUp(self):
        self.job_name = 'job_name'
        self.job = Job(self.job_name)


    def tearDown(self):
        pass


    def test__init__(self):
        pass

    def test__str__(self):
        pass

    def test__repr__(self):
        pass

    def test__copy__(self):
        pass

    def test__deepcopy__(self):
        pass

    def test__getattr__(self):
        pass

    def test__setattr__(self):
        pass

    def test_name(self):
        expected = self.job_name
        actual = self.job.name
        msg = 'checking initialization of name'
        self.assertEqual(expected, actual, '%s\nExpected: %s\nActual: %s\n' % (msg, expected, actual))

        new_name = 'new_name'
        self.job.name = new_name

        expected = new_name
        actual = self.job.name
        msg = 'checking assignment of name'
        self.assertEqual(expected, actual, '%s\nExpected: %s\nActual: %s\n' % (msg, expected, actual))


    def test_attributes(self):
        pass

    def test_num_jobs(self):
        pass

    def test_cluster_id(self):
        pass

    def test_job_file(self):
        job_file_name = '%s.job' % (self.job_name)
        job_file = os.path.join(os.getcwd(), job_file_name)
        expected = job_file
        actual = self.job.job_file
        msg = 'checking resolving attribute function for job file'
        self.assertEqual(expected, actual, '%s\nExpected: %s\nActual: %s\n' % (msg, expected, actual))

        init_dir = 'init_dir'
        self.job.initialdir = init_dir
        job_file = os.path.join(init_dir, job_file_name)
        self.assertEqual(job_file, self.job.job_file)

    def test_log_file(self):
        pass

    def test_initial_dir(self):
        pass

    def test_submit(self):
        pass

    def test_remove(self):
        pass

    def test_edit(self):
        expected = NotImplementedError
        actual = self.job.edit
        self.assertRaises(expected, actual)

    def test_status(self):
        expected = NotImplementedError
        actual = self.job.edit
        self.assertRaises(expected, actual)

    def test_wait(self):
        pass

    def test_get(self):
        pass

    def test_set(self):
        pass

    def test_delete(self):
        pass


    def test_write_job_file(self):
        pass

    def test_list_attributes(self):
        pass

    def test_make_dir(self):
        pass

    def test_make_job_dirs(self):
        pass

    def test_resolve_attribute(self):
        job = Job(self.job_name, Templates.vanilla_base)
        expected = self.job_name
        actual = job._resolve_attribute('initialdir')
        msg = 'checking resolving attribute function'
        self.assertEqual(expected, actual, '%s\nExpected: %s\nActual: %s\n' % (msg, expected, actual))

    def test_resolve_attribute_match(self):
        pass

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()