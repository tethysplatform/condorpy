'''
Created on May 23, 2014

@author: sdc50
'''
#TODO: add ability to get stats about the job (i.e. number of jobs, run time, etc.)
#TODO: figure out macros to get clusterid and process id for log files
#TODO: print job to command line
#TODO: write job file (submit description file) method in the job class


try:
    import htcondor
except ImportError:
    import condorpy.pseudohtcondor as htcondor

try:
    import classad
except ImportError:
    import condorpy.pseudoclassad as classad

import condorpy.classad_translation as translate
import os, subprocess

class Job(object):
    """classdocs

    http://research.cs.wisc.edu/htcondor/manual/v7.8/condor_submit.html#man-condor-submit

    """


    def __init__(self, ad=None):
        """Constructor

        """
        self.ad = ad if ad is not None else classad.ClassAd()
        if ad:
            assert(self.ad is ad)
        self.num_jobs = 0
        self.cluster_id = None
        self.schedd = htcondor.Schedd()

    def __str__(self):
        """docstring

        """

        return self.ad.__str__()

    def __repr__(self):
        """docstring

        """
        return self.ad.__repr__()

    def _make_dir(self, dir_name):
        """docstring

        """
        try:
            os.mkdir(dir_name)
        except OSError:
            pass

    def _make_job_dirs(self):
        """docstring

        """
        initdir = self.ad.get('Iwd')
        self._make_dir(initdir)
        log = self.ad.get('UserLog')
        if log:
            logdir = os.path.dirname(log)
            self._make_dir(os.path.join(initdir, logdir))

    def submit(self, queue=1):
        """docstring

        """

        if not self.ad.get('Cmd'):
            raise NoExecutable('You cannot submit a job without an executable')

        self._make_job_dirs()

        self.cluster_id = self.schedd.submit(self.ad, queue)
        self.num_jobs = queue
        return self.cluster_id

    def _submit( self, ad, count = 1, spool = False, ad_results = None ):
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

        jobFile = self._writeJobFile(initdir, ad, count)

        args = ['condor_submit', jobFile]

        process = subprocess.Popen(args, stdout = subprocess.PIPE, stderr=subprocess.PIPE)
        out,err = process.communicate()
        if err:
            if re.match('WARNING',err):
                print(err)
            else:
                raise Exception(err)

        cluster = re.split(' |\.',out)[-2]
        return cluster

        #wait for job to finish
        #logFile = "%s/logs/condor.log" % (initdir) ##TODO - read logfile attribute from class ad
        #process = subprocess.Popen(['condor_wait', logFile], stdout = subprocess.PIPE, stderr=subprocess.PIPE)
        #process.communicate()

    def _writeJobFile(path, ad, count):
        jobFileName = os.path.join(path,'condor.job')
        jobFile = open(jobFileName, 'w')
        jobFile.write('\n'.join(_translate(ad)))
        jobFile.write('\nqueue %d' % count)
        jobFile.close()
        return jobFileName

    def remove(self):
        """docstring

        """

        self.schedd.act(htcondor.JobAction.Remove, self.cluster_id)

    def edit(self):
        """docstring

        """

        pass

    def status(self):
        """docstring

        """

        pass

    def get(self, attr, value=None):
        """docstring

        """

        ad_attr, ad_value = translate.toAd(attr, value)

        return self.ad.get(ad_attr, ad_value)

    def set(self, attr, value):
        """docstring

        """

        ad_attr, ad_value = translate.toAd(attr.lower(), value)
        self.ad.__setitem__(ad_attr, ad_value)

class NoExecutable(Exception):
    """docstring

    """
    pass
