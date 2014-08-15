'''
Created on May 23, 2014

@author: sdc50
'''

try:
    import htcondor
except:
    import htcondorwin as htcondor

try:
    import classad
except:
    import classadwin as classad
    
import classad_translation as translate
import os

class Job(object):
    '''
    classdocs
    http://research.cs.wisc.edu/htcondor/manual/v7.8/condor_submit.html#man-condor-submit
    
    '''
    

    def __init__(self, ad=None):
        '''
        Constructor
        '''
        self.ad = ad if ad else classad.ClassAd()
        self.clusterId = None
        self.schedd = htcondor.Schedd()
        
    def __str__(self):
        return self.ad.__str__()
        
    def __repr__(self):
        return self.ad.__repr__()
    
    def _makedir(self,dir):
        try:
            os.mkdir(dir)
        except:
            pass
        
    def _makeJobDirs(self):
        initdir = self.ad.get('Iwd')
        self._makedir(initdir)
        log = self.ad.get('UserLog')
        if log:
            logdir = os.path.dirname(log)
            self._makedir(os.path.join(initdir,logdir))
        
    def submit(self, queue=1):
        '''
        
        '''
        
        if not self.ad.get('Cmd'):
            raise NoExecutable('You cannot submit a job without an executable')
        
        
        self._makeJobDirs()
        
        self.clusterId = self.schedd.submit(self.ad, queue)
        return self.clusterId
        
    def remove(self):
        '''
        
        '''
        
        self.schedd.act(htcondor.JobAction.Remove, self.clusterId)
        
    def edit(self):
        '''
        
        '''
        
        pass
    
    def status(self):
        '''
        '''
        
        pass
    
    def get(self, attr, value=None):
        '''
        
        '''
        
        adAttr,adValue = translate.toAd(attr,value)
        
        return self.ad.get(adAttr, value)
        
    def set(self, attr, value):
        '''
        
        '''
        
        adAttr, adValue = translate.toAd(attr.lower(),value)
        self.ad.__setitem__(adAttr, adValue)
        
class NoExecutable(Exception):
    pass
    
        
##########################################
#
#    Unit Tests
#
##########################################

def runTests():    
    print('testing')
    # call tests here
    test1()
    testTemplates()
    noCmdTest()
    condorErrorTest()
    print('passed')
 
def test1():
    job = Job()
    print(job)
    job.set('executable', 'echo')
    print(job)
    job = Job()
    print(job)
    #job.submit()
    
def testTemplates():
    print('Testing classad templates . . .')
    import classad_templates as tmplt
    job = Job(tmplt.ST_GSSHA)
    job.set('initialdir','./test')
    print(job)
    #job.submit(2)
    
def noCmdTest():
    print('Testing no executable . . .')
    
    job = Job()
    print(job)
    try:
        job.submit()
        raise
    except NoExecutable as e:
        print str(e)
        
def condorErrorTest():
    print('Testing condor errors')
    job = Job()
    job.set('executable','job.py')
    job.set('not_an_attr','foobar')
    try:
        job.submit()
    except Exception as e:
        print str(e)
    
if __name__ == '__main__':
    runTests()        
        