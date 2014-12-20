'''
Created on May 23, 2014

@author: sdc50
'''
#TODO: add ability to get stats about the job (i.e. number of jobs, run time, etc.)
#TODO: add ability to submit to remote schedulers

import os, subprocess, re

class Job(object):
    """classdocs

    http://research.cs.wisc.edu/htcondor/manual/v7.8/condor_submit.html#man-condor-submit

    """


    def __init__(self, name, attributes=None, executable=None, num_jobs=None):
        """Constructor

        """
        if attributes:
            assert isinstance(attributes, dict)
        self._name = name
        self._attributes = attributes or dict()
        self.executable = executable
        self._num_jobs = 1
        self._job_file = ""
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
        return self.attributes.__repr__()

    def __getattr__(self, item):
        """

        :param item:
        :return:
        """
        self.get(item)

    def __setattr__(self, key, value):
        """

        :param key:
        :param value:
        :return:
        """
        self.set(key, value)

    @property
    def name(self):
        """

        :return:
        """
        self._name = self.get('job_name')
        return self._name

    @name.setter
    def name(self,name):
        """

        :param name:
        :return:
        """
        self.set('job_name', name)

    @property
    def attributes(self):
        """

        :return:
        """
        return self._attributes

    @property
    def job_file(self):
        """

        :return:
        """
        #TODO: should the job file be just the name or the name and initdir?
        job_file_name = '%s.job' % (self.name)
        job_file_path = os.path.join(self.initialdir,job_file_name)
        self._job_file = job_file_path
        return self._job_file


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
        self._log_file = self._resolve_attribute('log')
        if not self._log_file:
            self._log_file = '%s.log' % (self.name)
        return self._log_file

    @property
    def cluster_id(self):
        """

        :return:
        """
        return self._cluster_id

    @property
    def initialdir(self):
        """

        :return:
        """
        self._initial_dir = self._resolve_attribute('initialdir')
        if not self._initial_dir:
            self._initial_dir = os.getcwd()
        return self._initial_dir

    @initialdir.setter
    def initialdir(self, initial_dir):
        """

        :param initial_dir:
        :return:
        """
        self.set('initialdir', initial_dir)

    def submit(self, queue=None, options=None):
        """docstring

        """

        if not self.executable:
            raise NoExecutable('You cannot submit a job without an executable')

        self._num_jobs = queue or self.num_jobs

        self._write_job_file()

        args = ['condor_submit']
        if options:
            args.append(options)
        args.append(self.job_file)

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
        try:
            value = self.attributes[attr]
        except KeyError:
            pass
        return value

    def set(self, attr, value):
        """set attribute in job file

        """
        self.attributes[attr] = value

    def delete(self, attr):
        """delete attribute from job file
        :param attr:
        :return:none
        """
        self.attributes.pop(attr)


    def _write_job_file(self):
        self._make_job_dirs()
        job_file = open(self.job_file, 'w')
        job_file.write(self.__str__())
        job_file.close()

    def _list_attributes(self):
        list = []
        for k,v in self.attributes.iteritems():
            if v:
                list.append(k + ' = ' + str(v))
        return list

    def _make_dir(self, dir_name):
        """docstring

        """
        try:
            os.makedirs(dir_name)
        except OSError:
            pass

    def _make_job_dirs(self):
        """docstring

        """
        self._make_dir(self.initialdir)
        log_dir = self._resolve_attribute('logdir')
        if log_dir:
            self._make_dir(os.path.join(self.initialdir, log_dir))

    def _resolve_attribute(self, attribute):
        """

        :return:
        """
        value = self.get(attribute)
        if not value:
            return None
        resolved_value = re.sub('\$\((.*?)\)',self._resolve_attribute_match, value)
        return resolved_value

    def _resolve_attribute_match(self, match):
        """

        :param match:
        :return:
        """
        return self.get(match.group(1), match.group(0))



class NoExecutable(Exception):
    """docstring

    """
    pass
