'''
Created on Aug 22, 2014

@author: sdc50
'''
import unittest
from condorpy import Job

def load_tests(loader, tests, pattern):
    return unittest.TestLoader().loadTestsFromTestCase(JobTest)

class JobTest(unittest.TestCase):


    def setUp(self):
        pass


    def tearDown(self):
        pass


    def test_Name(self):
        self.assertEquals(1, 1, 'This was supposed to fail')
        

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