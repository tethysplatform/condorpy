'''
Created on May 23, 2014

@author: sdc50
'''

import htcondor
import classad

class Job(object):
    '''
    classdocs
    http://research.cs.wisc.edu/htcondor/manual/v7.8/condor_submit.html#man-condor-submit
    
    '''
    

    def __init__(self, ad):
        '''
        Constructor
        '''
        self.setAd(ad)
        self.clusterId = None
        self.schedd = htcondor.Schedd()
        
    def getAd(self, attr, value):
        '''
        
        '''
        pass
    
    def setAd(self, ad):
        '''
        
        '''
        self.ad = ad
        
    def submit(self, queue=1):
        '''
        
        '''
        
        self.clusterId = self.schedd.submit(self.add, queue)
        return self.clusterId
        
    def remove(self):
        '''
        
        '''
        
        self.schedd.act(htcondor.JobAction.Remove, self.clusterId)
        
    def edit(self):
        '''
        
        '''
        
        pass
    
    
        
        