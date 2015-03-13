# Copyright (c) 2015 Scott Christensen
#
# This file is part of condorpy
#
# condorpy is free software: you can redistribute it and/or modify it under
# the terms of the BSD 2-Clause License. A copy of the BSD 2-Clause License
# should have be distributed with this file.

import os, subprocess, re, uuid
from collections import OrderedDict
from tethyscluster.sshutils import SSHClient
from tethyscluster.exception import RemoteCommandFailed, SSHError

class Job(object):
    """classdocs

    http://research.cs.wisc.edu/htcondor/manual/v7.8/condor_submit.html#man-condor-submit

    """


    def __init__(self,
                 name,
                 attributes=None,
                 executable=None,
                 arguments=None,
                 num_jobs=1,
                 host=None,
                 username=None,
                 password=None,
                 private_key=None,
                 private_key_pass=None):
        """Constructor

        """
        object.__setattr__(self, '_name', name)
        if attributes:
            assert isinstance(attributes, dict)
        object.__setattr__(self, '_attributes', attributes or OrderedDict())
        object.__setattr__(self, '_num_jobs', int(num_jobs))
        object.__setattr__(self, '_cluster_id', 0)
        object.__setattr__(self, '_job_file', '')
        object.__setattr__(self, '_remote', None)
        object.__setattr__(self, '_remote_input_files', None)
        if host:
            object.__setattr__(self, '_remote', SSHClient(host, username, password, private_key, private_key_pass))
            object.__setattr__(self, '_remote_id', uuid.uuid4().hex)
        self.job_name = name
        self.executable = executable
        self.arguments = arguments



    def __str__(self):
        """docstring

        """

        return '\n'.join(self._list_attributes()) + '\n\nqueue %d\n' % (self.num_jobs)

    def __repr__(self):
        """docstring

        """
        return '<Job: name=%s, num_jobs=%d, cluster_id=%s>' % (self.name, self.num_jobs, self.cluster_id)

    def __copy__(self):
        """

        :return:
        """
        copy = Job(self.name)
        copy.__dict__.update(self.__dict__)
        return copy

    def __deepcopy__(self, memo):
        """

        :return:
        """
        from copy import deepcopy
        copy = self.__copy__()
        copy._attributes = deepcopy(self.attributes, memo)
        return copy

    def __getattr__(self, item):
        """

        :param item:
        :return:
        """
        return self.get(item)

    def __setattr__(self, key, value):
        """

        :param key:
        :param value:
        :return:
        """
        if  key in self.__dict__ or '_' + key in self.__dict__:
            object.__setattr__(self, key, value)
        else:
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
        self._num_jobs = int(num_jobs)

    @property
    def cluster_id(self):
        """

        :return:
        """
        return self._cluster_id

    @property
    def job_file(self):
        """

        :return:
        """
        job_file_name = '%s.job' % (self.name)
        job_file_path = os.path.join(self.initial_dir, job_file_name)
        self._job_file = job_file_path
        return self._job_file

    @property
    def log_file(self):
        """

        :return:
        """
        log_file = self.get('log')
        if not log_file:
            log_file = '%s.log' % (self.name)
            self.set('log', log_file)
        return os.path.join(self.initial_dir, self._resolve_attribute('log'))

    @property
    def initial_dir(self):
        """

        :return:
        """
        initial_dir = self._resolve_attribute('initialdir')
        if not initial_dir:
            initial_dir = os.path.relpath(os.getcwd())
        if self._remote and os.path.isabs(initial_dir):
                raise Exception('Cannot define an absolute path as an initial_dir on a remote scheduler')
        return initial_dir

    @property
    def remote_input_files(self):
        return self._remote_input_files

    @remote_input_files.setter
    def remote_input_files(self, files):
        self._remote_input_files = files

    def submit(self, queue=None, options=[]):
        """docstring

        """

        if not self.executable:
            raise NoExecutable('You cannot submit a job without an executable')

        self._num_jobs = queue or self.num_jobs

        self._write_job_file()

        args = ['condor_submit']
        args.extend(options)
        args.append(self.job_file)

        out, err = self._execute(args)
        if err:
            if re.match('WARNING',err):
                print(err)
            else:
                raise Exception(err)
        print(out)
        try:
            self._cluster_id = int(re.search('(?<=cluster |\*\* Proc )(\d*)', out).group(1))
        except:
            self._cluster_id = -1
        return self.cluster_id

    def remove(self, options=[], job_num=None):
        """docstring

        """
        args = ['condor_rm']
        args.extend(options)
        job_id = '%s.%s' % (self.cluster_id, job_num) if job_num else self.cluster_id
        args.append(job_id)
        out, err = self._execute(args)
        print(out,err)

    def edit(self):
        """interface for CLI edit commands

        """
        raise NotImplementedError("This method is not yet implemented")

    def status(self):
        """docstring

        """
        raise NotImplementedError("This method is not yet implemented")

    def wait(self, options=[], job_num=None):
        """

        :return:
        """
        args = ['condor_wait']
        args.extend(options)
        job_id = '%s.%s' % (self.cluster_id, job_num) if job_num else str(self.cluster_id)
        abs_log_file = os.path.abspath(self.log_file)
        args.extend([abs_log_file, job_id])
        out, err = self._execute(args)

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

    def sync_remote_output(self):
        self._copy_output_from_remote()

    def _execute(self, args):
        out = None
        err = None
        if self._remote:
            cmd = ' '.join(args)
            try:
                cmd = 'cd %s && %s' % (self._remote_id, cmd)
                out = '\n'.join(self._remote.execute(cmd))
            except RemoteCommandFailed as e:
                err = e.output
            except SSHError as e:
                err = e.msg
        else:
            process = subprocess.Popen(args, stdout = subprocess.PIPE, stderr=subprocess.PIPE)
            out,err = process.communicate()

        return out, err

    def _copy_input_files_to_remote(self):
        self._remote.put(self.remote_input_files, self._remote_id)

    def _copy_output_from_remote(self):
        self._remote.get(os.path.join(self._remote_id, self.initial_dir))

    def _write_job_file(self):
        self._make_job_dirs()
        job_file = self._open(self.job_file, 'w')
        job_file.write(self.__str__())
        job_file.close()
        if self._remote:
            self._copy_input_files_to_remote()

    def _list_attributes(self):
        list = []
        for k,v in self.attributes.iteritems():
            if v:
                list.append(k + ' = ' + str(v))
        return list

    def _open(self, file_name, mode='w'):
        if self._remote:
            return self._remote.remote_file(os.path.join(self._remote_id,file_name), mode)
        else:
            return open(file_name, mode)

    def _make_dir(self, dir_name):
        """docstring

        """
        try:
            if self._remote:
                self._remote.makedirs(os.path.join(self._remote_id,dir_name))
            else:
                os.makedirs(dir_name)
        except OSError:
            pass

    def _make_job_dirs(self):
        """docstring

        """
        self._make_dir(self.initial_dir)
        log_dir = self._resolve_attribute('logdir')
        if log_dir:
            self._make_dir(os.path.join(self.initial_dir, log_dir))

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
        if match.group(1) == 'cluster':
            return str(self.cluster_id)

        return self.get(match.group(1), match.group(0))

    def __del__(self):
        if self._remote:
            self._remote.execute('rm -rf %s' % (self._remote_id,))
            self._remote.close()
            del self._remote



class NoExecutable(Exception):
    """docstring

    """
    pass



