'''
Created on May 23, 2014

@author: sdc50
'''
#TODO: add ability to get stats about the job (i.e. number of jobs, run time, etc.)
#TODO: figure out macros to get clusterid and process id for log files
#TODO: print job to command line


import os, subprocess, re

class Job(object):
    """classdocs

    http://research.cs.wisc.edu/htcondor/manual/v7.8/condor_submit.html#man-condor-submit

    """


    def __init__(self, name, attributes=None, executable=None, num_jobs=None):
        """Constructor

        """
        assert isinstance(attributes, dict)
        self._name = name
        self._attributes = attributes or dict()
        self._executable = executable
        self._num_jobs = 1
        self._log_file = ""
        self._cluster_id = None
        self._initial_dir = os.getcwd()
        self.set('job_name',self.name)

    def __str__(self):
        """docstring

        """

        return '\n'.join(self._list_attributes()) + '\n\nqueue %d\n' % (self.num_jobs)

    def __repr__(self):
        """docstring

        """
        return self._attributes.__repr__()

    @property
    def name(self):
        """

        :return:
        """
        return self._name

    @name.setter
    def name(self,name):
        """

        :param name:
        :return:
        """
        self._name = name

    @property
    def executable(self):
        """

        :return:
        """
        return self._executable

    @executable.setter
    def executable(self, executable):
        """

        :param executable:
        :return:
        """
        self._executable = executable

    @property
    def num_jobs(self):
        """

        :return:
        """
        return self._num_jobs

    @num_jobs.setter
    def num_jobs(self, num_jobs):
        """

        :param num_jobs:
        :return:
        """
        self._num_jobs = num_jobs

    @property
    def log_file(self):
        """

        :return:
        """
        self._log_file = self._attributes['log']
        if not self._log_file:
            self._log_file = "%s.log" % (self.name)
        return self._log_file

    @property
    def cluster_id(self):
        """

        :return:
        """
        return self._cluster_id

    @property
    def initial_dir(self):
        """

        :return:
        """
        self._initial_dir = self._attributes['initialdir']
        if not self._initial_dir:
            self._initial_dir = os.getcwd()
        return self._initial_dir

    @initial_dir.setter
    def initial_dir(self, initial_dir):
        """

        :param initial_dir:
        :return:
        """
        self._initial_dir = initial_dir

    def submit(self, queue=None, options=None):
        """docstring

        """

        if not self.executable:
            raise NoExecutable('You cannot submit a job without an executable')

        self._make_job_dirs()
        self._num_jobs = queue or self.num_jobs

        job_file = self._write_job_file()

        args = ['condor_submit', options, job_file]

        process = subprocess.Popen(args, stdout = subprocess.PIPE, stderr=subprocess.PIPE)
        out,err = process.communicate()

        if err:
            if re.match('WARNING',err):
                print(err)
            else:
                raise Exception(err)
        print out
        self._cluster_id = re.split(' |\.',out)[-2]
        return self.cluster_id

    def remove(self):
        """docstring

        """
        options = self.cluster_id ##TODO allow other options (like specific processes in a cluster to be removed)
        args = ['condor_rm', options]
        process = subprocess.Popen(args, stdout = subprocess.PIPE, stderr=subprocess.PIPE)
        out,err = process.communicate()
        print(out,err)

    def edit(self):
        """interface for CLI edit commands

        """
        raise NotImplementedError("This method is not yet implemented")

    def status(self):
        """docstring

        """
        raise NotImplementedError("This method is not yet implemented")

    def wait(self):
        """

        :return:
        """
        process = subprocess.Popen(['condor_wait', self.log_file], stdout = subprocess.PIPE, stderr=subprocess.PIPE)
        process.communicate()

    def get(self, attr, value=None):
        """get attribute from job file

        """
        return self._attributes[attr] or value

    def set(self, attr, value):
        """set attribute in job file

        """
        self._attributes[attr] = value

    def delete(self, attr):
        """delete attribute from job file
        :param attr:
        :return:none
        """
        self._attributes.pop(attr)


    def _write_job_file(self):
        job_file_name = '%s.job' % (self.name)
        job_file_path = os.path.join(self.initial_dir,job_file_name)
        job_file = open(job_file_path, 'w')
        job_file.write(self.__str__())
        job_file.close()
        return job_file_path

    def _list_attributes(self):
        list = []
        for k,v in self._attributes.iteritems():
            if str(v) != '':
                list.append(k + ' = ' + str(v))
        return list

    def _make_dir(self, dir_name):
        """docstring

        """
        os.makedirs(dir_name)
        # try:
        #     os.mkdir(dir_name)
        # except OSError:
        #     pass

    def _make_job_dirs(self):
        """docstring

        """
        self._make_dir(self.initial_dir)
        #TODO: which log dirs should I create?
        if self.log_file:
            log_dir = os.path.dirname(self.log_file)
            self._make_dir(os.path.join(self.initial_dir, log_dir))


class NoExecutable(Exception):
    """docstring

    """
    pass
