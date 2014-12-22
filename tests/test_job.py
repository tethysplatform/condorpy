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


    def test_constructor(self):
        """check initialization of instance variables

        :return: None
        """

        #check that instance variables are instantiated with default constructor
        '''
        self.job = Job()
        self.assertIsInstance(self.job.ad,classad.ClassAd, 'ad must be instance of classad.Classad')
        self.assertIsNone(self.job.cluster_id,'cluster id should be set to None')
        self.assertIsInstance(self.job.schedd,htcondor.Schedd,'schedd must be an instance of htcondor.Schedd')

        #check that classad with attributes is instantiated when passed to constructor
        ad = classad.ClassAd({'Foo':'Bar'})
        self.job = Job(ad)
        self.assertEqual(ad, self.job.ad,'ad was not properly assigned')

        #check that classad without attributes is instantiated when passed to constructor
        assert(isinstance(ad,classad.ClassAd))
        self.job = Job(ad)
        self.assertEqual(ad, self.job.ad,'ad was not properly assigned')
        '''
        '''
        expected = None
        actual = None
        msg = ''
        self.assertEqual(expected, actual, '%s\nExpected: %s\nActual: %s\n' % (msg, expected, actual))

        '''
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


    def test_resolve_attribute(self):
        job = Job(self.job_name, Templates.vanilla_base)
        expected = self.job_name
        actual = job._resolve_attribute('initialdir')
        msg = 'checking resolving attribute function'
        self.assertEqual(expected, actual, '%s\nExpected: %s\nActual: %s\n' % (msg, expected, actual))

    def test_str(self):
        expected = None
        actual = None
        msg = ''
        self.assertEqual(expected, actual, '%s\nExpected: %s\nActual: %s\n' % (msg, expected, actual))

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

    def test_make_job_dirs(self):
        expected = None
        actual = None
        msg = ''
        self.assertEqual(expected, actual, '%s\nExpected: %s\nActual: %s\n' % (msg, expected, actual))

    def test_submit(self):
        expected = None
        actual = None
        msg = ''
        self.assertEqual(expected, actual, '%s\nExpected: %s\nActual: %s\n' % (msg, expected, actual))

    def test_remove(self):
        expected = None
        actual = None
        msg = ''
        self.assertEqual(expected, actual, '%s\nExpected: %s\nActual: %s\n' % (msg, expected, actual))

    def test_edit(self):
        expected = None
        actual = None
        msg = ''
        self.assertEqual(expected, actual, '%s\nExpected: %s\nActual: %s\n' % (msg, expected, actual))

    def test_status(self):
        expected = None
        actual = None
        msg = ''
        self.assertEqual(expected, actual, '%s\nExpected: %s\nActual: %s\n' % (msg, expected, actual))

    def test_get(self):
        expected = None
        actual = None
        msg = ''
        self.assertEqual(expected, actual, '%s\nExpected: %s\nActual: %s\n' % (msg, expected, actual))

    def test_set(self):
        expected = None
        actual = None
        msg = ''
        self.assertEqual(expected, actual, '%s\nExpected: %s\nActual: %s\n' % (msg, expected, actual))

##########################################
#
#    Unit Tests
#
##########################################
'''
def runTests():
    print 'testing'
    # call tests here
    test1()
    testTemplates()
    noCmdTest()
    condorErrorTest()
    print 'passed'

def test1():
    job = Job()
    print job
    job.set('executable', 'echo')
    print job
    job = Job()
    print job
    #job.submit()

def testTemplates():
    print 'Testing classad templates . . .'
    import classad_templates as tmplt
    job = Job(tmplt.ST_GSSHA)
    job.set('initialdir', './test')
    print job
    #job.submit(2)

def noCmdTest():
    print 'Testing no executable . . .'

    job = Job()
    print job
    try:
        job.submit()
        raise
    except NoExecutable as e:
        print str(e)

def condorErrorTest():
    print 'Testing condor errors'
    job = Job()
    job.set('executable', 'job.py')
    job.set('not_an_attr', 'foobar')
    try:
        job.submit()
    except Exception as e:
        print str(e)'''

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()