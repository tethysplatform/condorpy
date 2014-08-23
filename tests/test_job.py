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
        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()