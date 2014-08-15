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

class Job(object):
    '''
    classdocs
    http://research.cs.wisc.edu/htcondor/manual/v7.8/condor_submit.html#man-condor-submit
    
    '''
    

    def __init__(self, ad=classad.ClassAd()):
        '''
        Constructor
        '''
        self.ad = ad
        self.clusterId = None
        self.schedd = htcondor.Schedd()
        
    def __str__(self):
        return self.ad.__str__()
        
    def __repr__(self):
        return self.ad.__repr__()
    
        
    def submit(self, queue=1):
        '''
        
        '''
        
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
    #test2()
    print('passed')
 
def test1():
    job = Job()
    job.set('executable', 'echo')
    print(job)
    
def testTemplates():
    print('Testing classad templates . . .')
    import classad_templates as tmplt
    job = Job(tmplt.ST_GSSHA)
    print(job)
    job.submit(2)
    
##def test2(): 
    
if __name__ == '__main__':
    runTests()        
        