'''
Created on May 23, 2014

@author: sdc50
'''

try:
    import htcondor
except ImportError:
    import condorpy.pseudohtcondor as htcondor

try:
    import classad
except ImportError:
    import condorpy.pseudoclassad as classad

import condorpy.classad_translation as translate
import os

class Job(object):
    '''classdocs

    http://research.cs.wisc.edu/htcondor/manual/v7.8/condor_submit.html#man-condor-submit

    '''


    def __init__(self, ad=None):
        """Constructor

        """
        self.ad = ad if ad is not None else classad.ClassAd()
        if ad:
            assert(self.ad is ad)
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
        return self.cluster_id

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
