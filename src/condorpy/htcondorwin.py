'''
Created on Aug 1, 2014

@author: sdc50
'''

import re, os, subprocess
import classad_translation as translate


def platform():
    '''
    
    '''
    
    raise NotImplementedError("This method has not been implemented in the unofficial version of htcondor")

def version():
    '''
    
    '''
    
    raise NotImplementedError("This method has not been implemented in the unofficial version of htcondor")

def reload_config():
    '''
    
    '''
    
    raise NotImplementedError("This method has not been implemented in the unofficial version of htcondor")

def send_command():
    '''
    
    '''
    
    raise NotImplementedError("This method has not been implemented in the unofficial version of htcondor")

'''

'''
param = None


##################
#
# Private Functions
#
##################
def _writeJobFile(path, ad, count):
        jobFileName = os.path.join(path,'condor.job')
        jobFile = open(jobFileName, 'w')
        jobFile.write('\n'.join(_translate(ad)))
        jobFile.write('\nqueue %d' % count)
        jobFile.close()
        return jobFileName

def _translate(ad):
    list = []
    for k,v in ad.attributes.iteritems():
        key, value = translate.toJob(k,v)
        list.append(key + ' = ' + str(value))
    return list

def _enum(**enums):
    return type('Enum', (), enums)
    
AdTypes = _enum()
DaemonCommands = _enum()
DaemonTypes = _enum()
JobAction = _enum(Remove=1, Hold=2)




class Schedd(object):
    '''
    classdocs
    '''


    def __init__(self, classad=None):
        '''
        Constructor
        
        Create an instance of the Schedd class.
        
        Optional parameter classad describes the location of the remote condor_schedd daemon. If the parameter is omitted, the local condor_schedd daemon is used.
        '''
        
        self.ad = classad
        
    def act( self, action, job_spec ):
        '''
        Change status of job(s) in the condor_schedd daemon. The integer return 
        value is a ClassAd object describing the number of jobs changed.
        
        Parameter action is the action to perform; must be of the enum JobAction.
        
        Parameter job_spec is the job specification. It can either be a list of 
        job IDs or a string specifying a constraint to match jobs.
        '''
        
        if(action == JobAction.Remove):
            args = ['condor_rm', job_spec] ##TODO job_spec is assumed to be just the clusterID, but it need to be processed to be compatible
    
            process = subprocess.Popen(args, stdout = subprocess.PIPE, stderr=subprocess.PIPE)
            out,err = process.communicate()
            print(out,err)
        else:
            raise NotImplementedError("This action is not yet implemented")
        
    def edit( self, job_spec, attr, value ):
        '''        
        Edit one or more jobs in the queue.
        
        Parameter job_spec is either a list of jobs, with each given as ClusterId.ProcId or a string containing a constraint to match jobs against.
        
        Parameter attr is the attribute name of the attribute to edit.
        
        Parameter value is the new value of the job attribute. It should be a string, which will be converted to a ClassAd expression, or an ExprTree object.
        '''
        
        raise NotImplementedError("This method has not been implemented in the unofficial version of htcondor classad")
    
    def query( self, constraint = True, attr_list = [] ):
        '''
        Query the condor_schedd daemon for jobs. Returns a list of ClassAds 
        representing the matching jobs, containing at least the requested 
        attributes requested by the second parameter.
        
        The optional parameter constraint provides a constraint for filtering 
        out jobs. It defaults to True.
        
        Parameter attr_list is a list of attributes for the condor_schedd 
        daemon to project along. It defaults to having the condor_schedd daemon 
        return all attributes.
        '''
        
        raise NotImplementedError("This method has not been implemented in the unofficial version of htcondor classad")
    
    def submit( self, ad, count = 1, spool = False, ad_results = None ):
        '''
        Submit one or more jobs to the condor_schedd daemon. Returns the newly 
        created cluster ID.
        
        This method requires the invoker to provide a ClassAd for the new job 
        cluster; such a ClassAd contains attributes with different names than 
        the commands in a submit description file. As an example, the stdout 
        file is referred to as output in the submit description file, but Out 
        in the ClassAd. To generate an example ClassAd, take a sample submit 
        description file and invoke
        
        condor_submit -dump <filename> [cmdfile]
        
        Then, load the resulting contents of <filename> into Python.
        
        Parameter ad is the ClassAd describing the job cluster.
        
        Parameter count is the number of jobs to submit to the cluster. 
        Defaults to 1.
        
        Parameter spool inserts the necessary attributes into the job for it to 
        have the input files spooled to a remote condor_schedd daemon. This 
        parameter is necessary for jobs submitted to a remote condor_schedd.
        
        Parameter ad_results, if set to a list, will contain the job ClassAds 
        resulting from the job submission. These are useful for interacting 
        with the job spool at a later time.
        '''
        
        
        initdir = ad.get('Iwd')
        if not initdir:
            initdir = os.getcwd()
        
        jobFile = _writeJobFile(initdir, ad, count)
        
        
        args = ['condor_submit', jobFile]

        process = subprocess.Popen(args, stdout = subprocess.PIPE, stderr=subprocess.PIPE)
        out,err = process.communicate()
        if err:
            if re.match('WARNING',err):
                print err
            else:
                raise Exception(err)
            
        cluster = re.split(' |\.',out)[-2]
        return cluster

        #wait for job to finish
        #logFile = "%s/logs/condor.log" % (initdir) ##TODO - read logfile attribute from class ad
        #process = subprocess.Popen(['condor_wait', logFile], stdout = subprocess.PIPE, stderr=subprocess.PIPE)
        #process.communicate()
        
        
    def spool( self, ad_list ):
        '''
        Spools the files specified in a list of job ClassAds to the condor_schedd. Throws a RuntimeError exception if there are any errors.
        
        Parameter ad_list is a list of ClassAds containing job descriptions; typically, this is the list filled by the ad_results argument of the submit method call.
        '''
        
        raise NotImplementedError("This method has not been implemented in the unofficial version of htcondor classad")
    
    def retrieve( self, job_spec ):
        '''
        Retrieve the output sandbox from one or more jobs.
        
        Parameter job_spec is an expression string matching the list of job output sandboxes to retrieve.
        '''
        
        raise NotImplementedError("This method has not been implemented in the unofficial version of htcondor classad")
    

class Collector():
    '''
    
    '''
    
    def __init__(self):
        '''
        
        '''
        
        raise NotImplementedError("This class has not been implemented in the unofficial version of htcondor")


class SecMan():
    '''
    
    '''
    
    def __init__(self):
        '''
        
        '''
        
        raise NotImplementedError("This class has not been implemented in the unofficial version of htcondor")
        