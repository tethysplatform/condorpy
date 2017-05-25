'''
Created on Aug 22, 2014

@author: sdc50
'''
import unittest
from condorpy import Job
from condorpy import Templates
import os
import shutil
from collections import OrderedDict


def load_tests(loader, tests, pattern):
    return unittest.TestLoader().loadTestsFromTestCase(TestJob)

class TestJob(unittest.TestCase):

    expected = None
    actual = None
    msg = None
    base_dir = os.path.join(os.path.dirname(__file__))

    @property
    def output(self):
        return '%s\nExpected: %s\nActual:   %s\n' % (self.msg, self.expected, self.actual)

    @property
    def assert_args(self):
        return (self.expected, self.actual, self.output)


    def setUp(self):
        self.job_name = 'job_name'
        self.job = Job(self.job_name)

    def tearDown(self):
        pass

    def test__init__(self):
        attributes = dict()
        attributes['job_name'] = self.job_name

        self.expected = {'_name': self.job_name,
                    '_attributes': attributes,
                    '_num_jobs': 1,
                    '_cluster_id': 0,
                    '_job_file': '',
                    '_remote': None,
                    '_remote_id': None,
                    '_remote_input_files': None,
                    '_cwd': '.'}
        self.actual = self.job.__dict__
        self.msg = 'testing initialization with default values'
        self.assertDictEqual(*self.assert_args)

        exe = 'exe'
        args = 'args'
        num_jobs = '5'
        self.job = Job(self.job_name, OrderedDict(), num_jobs, executable=exe, arguments=args)
        attributes.update({'executable': exe, 'arguments': args})

        self.expected.update({'_name': self.job_name,
                    '_attributes': attributes,
                    '_num_jobs': int(num_jobs)})
        self.actual = self.job.__dict__
        self.actual['_attributes'] = dict(self.actual['_attributes'])
        self.msg = 'testing initialization with all values supplied'
        self.assertDictEqual(*self.assert_args)

        num_jobs = 'five'
        self.assertRaises(ValueError, Job, self.job_name, num_jobs=num_jobs)


    def test__str__(self):
        expected = 'job_name = %s\n\nqueue 1\n' % (self.job_name)
        actual = self.job.__str__()
        msg = 'testing to string with default initialization'
        self.assertEqual(expected, actual, '%s\nExpected: %s\nActual:   %s\n' % (msg, expected, actual))


    def test__repr__(self):
        expected = '<' \
                   'Job: name=%s, num_jobs=%d, cluster_id=%s>' % (self.job_name, 1, 0)
        actual = self.job.__repr__()
        msg = 'testing repr with default initialization'
        self.assertEqual(expected, actual, '%s\nExpected: %s\nActual:   %s\n' % (msg, expected, actual))

    def test__copy__(self):
        original = self.job
        copy = self.job.__copy__()
        expected = original.name
        actual = copy.name
        msg = 'testing that name of copy is equal to original'
        self.assertEqual(expected, actual, '%s\nExpected: %s\nActual:   %s\n' % (msg, expected, actual))

        expected = original.attributes
        actual = copy.attributes
        msg = 'testing that attributes dictionary of copy is equal to original'
        self.assertDictEqual(expected, actual, '%s\nExpected: %s\nActual:   %s\n' % (msg, expected, actual))
        msg = "testing that attributes is the same instance as the original's"
        self.assertIs(expected, actual, '%s\nExpected: %s\nActual:   %s\n' % (msg, expected, actual))


    def test__deepcopy__(self):
        original = self.job
        memo = dict()
        copy = self.job.__deepcopy__(memo)
        expected = self.job.name
        actual = copy.name
        msg = 'testing that name of deepcopy is equal to original'
        self.assertEqual(expected, actual, '%s\nExpected: %s\nActual:   %s\n' % (msg, expected, actual))

        expected = original.attributes
        actual = copy.attributes
        msg = 'testing that attributes dictionary of copy is equal to original'
        self.assertDictEqual(expected, actual, '%s\nExpected: %s\nActual:   %s\n' % (msg, expected, actual))
        msg = "testing that attributes is not the same instance as the original's"
        self.assertIsNot(expected, actual, '%s\nExpected: %s\nActual:   %s\n' % (msg, expected, actual))


    def test__getattr__(self):
        exe = 'exe'
        self.job = Job(self.job_name, executable=exe)
        expected = exe
        actual = self.job.executable
        msg = 'testing that existing value is returned'
        self.assertEqual(expected, actual, '%s\nExpected: %s\nActual:   %s\n' % (msg, expected, actual))

        pass

    def test__setattr__(self):
        pass

    def test_name(self):
        expected = self.job_name
        actual = self.job.name
        msg = 'checking initialization of name'
        self.assertEqual(expected, actual, '%s\nExpected: %s\nActual:   %s\n' % (msg, expected, actual))

        new_name = 'new_name'
        self.job.name = new_name

        expected = new_name
        actual = self.job.name
        msg = 'checking assignment of name'
        self.assertEqual(expected, actual, '%s\nExpected: %s\nActual:   %s\n' % (msg, expected, actual))


    def test_attributes(self):
        pass

    def test_num_jobs(self):
        pass

    def test_cluster_id(self):
        pass

    def test_job_file(self):
        job_file_name = '%s.job' % (self.job_name)
        job_file = os.path.join(os.path.relpath(os.getcwd()), job_file_name)
        expected = job_file
        actual = self.job.job_file
        msg = 'checking resolving attribute function for job file'
        self.assertEqual(expected, actual, '%s\nExpected: %s\nActual:   %s\n' % (msg, expected, actual))

        init_dir = 'init_dir'
        self.job.initialdir = init_dir
        job_file = os.path.join(init_dir, job_file_name)
        self.assertEqual(job_file, self.job.job_file)

    def test_log_file(self):
        self.job = Job(self.job_name, Templates.base)
        log_file = '%s/%s/%s.%s.log' % (self.job.initial_dir, self.job.logdir, self.job_name, self.job.cluster_id)
        expected = log_file
        actual = self.job.log_file
        msg = 'checking resolving attribute function for log file'
        self.assertEqual(expected, actual, '%s\nExpected: %s\nActual:   %s\n' % (msg, expected, actual))


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
        non_existent_attr = 'not-there'
        expected = None
        actual = self.job.get(non_existent_attr)
        msg = 'testing that None is returned when attribute does not exist'
        self.assertIsNone(actual, '%s\nExpected: %s\nActual:   %s\n' % (msg, expected, actual))

        expected = 'expected'
        actual = self.job.get(non_existent_attr, expected)
        msg = 'testing that supplied value is returned when attribute does not exist'
        self.assertEqual(expected, actual, '%s\nExpected: %s\nActual:   %s\n' % (msg, expected, actual))

        exe = 'exe'
        self.job = Job(self.job_name, executable=exe)
        expected = exe
        actual = self.job.get('executable')
        msg = 'testing that existing value is returned'
        self.assertEqual(expected, actual, '%s\nExpected: %s\nActual:   %s\n' % (msg, expected, actual))


    def test_set(self):
        key = 'was-not-there'
        value = 'now-it-is'
        self.job.set(key, value)
        expected = value
        actual = self.job.attributes[key]
        msg = 'testing that attribute that previously does not exist is set correctly'
        self.assertEqual(expected, actual, '%s\nExpected: %s\nActual:   %s\n' % (msg, expected, actual))

        key = 'was-already-there'
        value = 'used-to-be-this'
        new_value = 'now-it-is-this'
        self.job.set(key, value)
        self.job.set(key,new_value)
        expected = new_value
        actual = self.job.attributes[key]
        msg = 'testing that attribute that previously existed is re-set correctly'
        self.assertEqual(expected, actual, '%s\nExpected: %s\nActual:   %s\n' % (msg, expected, actual))

        key = 'python boolean'
        value = True
        self.job.set(key, value)
        expected = 'true'
        actual = self.job.attributes[key]
        msg = 'testing that an attribute can be set with the Python boolean value "True"'
        self.assertEqual(expected, actual, '%s\nExpected: %s\nActual:   %s\n' % (msg, expected, actual))

        key = 'python boolean'
        value = False
        self.job.set(key, value)
        expected = 'false'
        actual = self.job.attributes[key]
        msg = 'testing that an attribute can be set with the Python boolean value "False"'
        self.assertEqual(expected, actual, '%s\nExpected: %s\nActual:   %s\n' % (msg, expected, actual))

        key = 'python list'
        value = ['file.txt', 1]
        self.job.set(key, value)
        expected = ', '.join([str(i) for i in value])
        actual = self.job.attributes[key]
        msg = 'testing that an attribute can be set with a Python list'
        self.assertEqual(expected, actual, '%s\nExpected: %s\nActual:   %s\n' % (msg, expected, actual))

    def test_delete(self):
        key = 'was-not-there'
        value = 'now-it-is'
        self.job.set(key, value)
        self.job.delete(key)
        member = key
        container = self.job.attributes
        msg = 'testing that attribute is removed when deleted'
        self.assertNotIn(member, container, msg)


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
        self.assertEqual(expected, actual, '%s\nExpected: %s\nActual:   %s\n' % (msg, expected, actual))

    def test_resolve_attribute_match(self):
        pass

    def test_remote(self):
        working_dir = os.path.join(self.base_dir, 'test_files', 'working_dir')
        self.job = Job('remote_test',
                       Templates.vanilla_transfer_files,
                       host='localhost',
                       username=os.environ['USER'],
                       private_key='~/.ssh/id_rsa',
                       remote_input_files=['../copy_test.py', 'input.txt'],
                       transfer_input_files='../input.txt',
                       working_directory=working_dir)

        remote_base_path = os.path.expanduser('~/' + self.job._remote_id)
        if os.path.exists(remote_base_path):
            raise
        self.job._write_job_file()
        self.assertTrue(os.path.exists(remote_base_path))

        self.job.sync_remote_output()
        local_output = os.path.join(working_dir, self.job.name)
        self.assertTrue(os.path.exists(local_output))
        shutil.rmtree(remote_base_path)
        shutil.rmtree(local_output)

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()