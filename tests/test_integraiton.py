import unittest
from condorpy import Job
from condorpy import Templates
import os
import shutil

class TestIntegration(unittest.TestCase):

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

    def test_submit(self):
        working_dir = os.path.join(self.base_dir, 'test_files', 'working_dir')

        self.job = Job('remote_test',
                       Templates.vanilla_transfer_files,
                       host='localhost',
                       username=os.environ['USER'],
                       private_key='~/.ssh/id_rsa',
                       remote_input_files=['../copy_test.py', 'input.txt'],
                       transfer_input_files='../input.txt',
                       executable=os.path.join(self.base_dir, 'test_files', 'copy_test.py'),
                       working_directory=working_dir)

        remote_base_path = os.path.expanduser('~/' + self.job._remote_id)
        if os.path.exists(remote_base_path):
            raise
        self.job.submit()
        self.assertTrue(os.path.exists(remote_base_path))
        self.job.wait()
        self.job.sync_remote_output()
        local_output = os.path.join(working_dir, self.job.name)
        self.assertTrue(os.path.exists(local_output))
        output = os.path.join(local_output, 'output.txt')

        self.assertTrue(os.path.exists(output))
        shutil.rmtree(remote_base_path)
        shutil.rmtree(local_output)