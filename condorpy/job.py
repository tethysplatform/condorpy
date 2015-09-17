# Copyright (c) 2015 Scott Christensen
#
# This file is part of condorpy
#
# condorpy is free software: you can redistribute it and/or modify it under
# the terms of the BSD 2-Clause License. A copy of the BSD 2-Clause License
# should have been distributed with this file.

import os, subprocess, re, uuid
from collections import OrderedDict

from static import CONDOR_JOB_STATUSES
from logger import log
from exceptions import NoExecutable, RemoteError, HTCondorError

from tethyscluster.sshutils import SSHClient
from tethyscluster.exception import RemoteCommandFailed, SSHError

class Job(object):
    """Represents a HTCondor job and the submit description file.

    This class provides an object model representation for a computing job on HTCondor. It offers a wrapper for the
    command line interface of HTCondor for interacting with the job. Jobs may be submitted to a local installation
    of HTCondor or to a remote instance through ssh. An instance of this class will have certain pre-defined attributes
    which are defined below. Any other attribute may be defined and is equivalent to calling the set method. This is
    for ease in assigning attributes to the submit description file. For more information on valid attributes of the
    submit description file see: http://research.cs.wisc.edu/htcondor/manual/current/condor_submit.html.

    For more information about HTCondor see: http://research.cs.wisc.edu/htcondor/

    Args:

        name (str): A name to represent the job. This will be used to name the submit description file.
        attributes (dict, optional): A dictionary of attributes that will be assigned to the submit description
            file. Defaults to None.
        executable (str, optional): The path to the executable file for the job. This path can be absolute or
            relative to the current working directory when the code is executed. When submitting to a remote
            scheduler the executable will be copied into the current working directory of the remote machine. This
            is a shortcut to setting the 'Executable' attribute in the submit description file. Defaults to None.
        arguments (str, optional): A space or comma delimited list of arguments for the executable. This is a
            shortcut to setting the 'Arguments' attribute in the submit description file. Defaults to None.
        num_jobs (int, optional): The number of sub-jobs that will be queued by this job. This is the argument
            of the 'Queue' attribute in the submit description file. It can also be set when calling the submit
            method. Defaults to 1.
        host (str, optional): The host name of a remote server running an HTCondor scheduler daemon. Defaults to
            None.
        username (str, optional): The username for logging in to 'host'. Defaults to None.
        password (str, optional): The password for 'username' when logging in to 'host'. Defaults to None.
        private_key (str, optional): The path to a private key file providing passwordless ssh on 'host'.
            Defaults to None.
        private_key_pass (str, optional): The passphrase for the 'private_key' if required.
        remote_input_files (list, optional): A list of files to be copied to a remote server for remote job submission.
        working_directory (str, optional): The file path where execute calls should be run from.

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
                 private_key_pass=None,
                 remote_input_files=None,
                 working_directory='.',
                 **kwargs):

        object.__setattr__(self, '_name', name)
        if attributes:
            assert isinstance(attributes, dict)
        object.__setattr__(self, '_attributes', attributes or OrderedDict())
        object.__setattr__(self, '_num_jobs', int(num_jobs))
        object.__setattr__(self, '_cluster_id', 0)
        object.__setattr__(self, '_job_file', '')
        object.__setattr__(self, '_remote', None)
        object.__setattr__(self, '_remote_input_files', remote_input_files or None)
        object.__setattr__(self, '_cwd', working_directory)
        if host:
            object.__setattr__(self, '_remote', SSHClient(host, username, password, private_key, private_key_pass))
            object.__setattr__(self, '_remote_id', uuid.uuid4().hex)
        self.job_name = name
        self.executable = executable
        self.arguments = arguments
        if kwargs:
            for attr, value in kwargs.iteritems():
                self.set(attr, value)

    def __str__(self):
        return '\n'.join(self._list_attributes()) + '\n\nqueue %d\n' % (self.num_jobs)

    def __repr__(self):
        return '<Job: name=%s, num_jobs=%d, cluster_id=%s>' % (self.name, self.num_jobs, self.cluster_id)

    def __copy__(self):
        copy = Job(self.name)
        copy.__dict__.update(self.__dict__)
        return copy

    def __deepcopy__(self, memo):
        from copy import deepcopy
        copy = self.__copy__()
        copy._attributes = deepcopy(self.attributes, memo)
        return copy

    def __getattr__(self, item):
        """
        A shortcut for the 'get' method.

        Args:
            item (str): The name of the attribute to get.

        Returns:
            The value assigned to 'item' if defined. Otherwise None.
        """
        return self.get(item)

    def __setattr__(self, key, value):
        """
        A shortcut for the 'set' method.

        Args:
            key (str): The name of the attribute to set.
            value (str): The value to assign to 'key'.
        """
        if  key in self.__dict__ or '_' + key in self.__dict__:
            object.__setattr__(self, key, value)
        else:
            self.set(key, value)

    def set_cwd(fn):
        """
        Decorator to set the specified working directory to execute the function, and then restore the previous cwd.
        """
        def wrapped(self, *args, **kwargs):
            log.info('Calling function: %s with args=%s', fn, args if args else [])
            cwd = os.getcwd()
            log.info('Saved cwd: %s', cwd)
            os.chdir(self._cwd)
            log.info('Changing working directory to: %s', self._cwd)
            result = fn(self, *args, **kwargs)
            os.chdir(cwd)
            log.info('Restored working directory to: %s', os.getcwd())
            return result
        return wrapped

    @property
    def name(self):

        self._name = self.get('job_name')
        return self._name

    @name.setter
    def name(self,name):
        self.set('job_name', name)

    @property
    def attributes(self):
        return self._attributes

    @property
    def num_jobs(self):
        return self._num_jobs

    @num_jobs.setter
    def num_jobs(self, num_jobs):
        self._num_jobs = int(num_jobs)

    @property
    def cluster_id(self):
        """The id assigned to the job (called a cluster in HTConodr) when the job is submitted.

        """
        return self._cluster_id

    @property
    def status(self):
        """The job status

        """
        status_dict = self.statuses
        #determin job status
        status = "Various"
        for key,val in status_dict.iteritems():
            if val == self.num_jobs:
                status = key
        return status

    @property
    def statuses(self):
        """
        Return dictionary of all process statuses
        """
        return self._update_status()

    @property
    def job_file(self):
        """The path to the submit description file representing this job.

        """
        job_file_name = '%s.job' % (self.name)
        job_file_path = os.path.join(self.initial_dir, job_file_name)
        self._job_file = job_file_path
        return self._job_file

    @property
    def log_file(self):
        """The path to the log file for this job.

        """
        log_file = self.get('log')
        if not log_file:
            log_file = '%s.log' % (self.name)
            self.set('log', log_file)
        return os.path.join(self.initial_dir, self.get('log'))

    @property
    def initial_dir(self):
        """The initial directory defined for the job.

        All input files, and log files are relative to this directory. Output files will be copied into this
        directory by default. This directory will be created if it doesn't already exist when the job is submitted.

        Note:
            The executable file is defined relative to the current working directory, NOT to the initial directory.
            The initial directory is created in the current working directory.

        """
        initial_dir = self.get('initialdir')
        if not initial_dir:
            initial_dir = os.curdir
        if self._remote and os.path.isabs(initial_dir):
                raise RemoteError('Cannot define an absolute path as an initial_dir on a remote scheduler')
        return initial_dir

    @property
    def remote_input_files(self):
        """A list of paths to files or directories to be copied to a remote server for remote job submission.

        """
        return self._remote_input_files

    @remote_input_files.setter
    def remote_input_files(self, files):
        """A list of paths to files or directories to be copied to a remote server for remote job submission.

        Args:
            files (list or tuple of strings): A list or tuple of file paths to all input files and the executable that
            are required to be copied to the remote server when submitting the job remotely.

        Note:
            File paths defined for remote_input_files should be relative to the current working directory on the
            client machine. They are copied into the working directory on the remote. Input file paths defined for
            the submit description file should be relative to the initial directory on the remote server.

        """
        self._remote_input_files = list(files)

    def submit(self, queue=None, options=[]):
        """Submits the job either locally or to a remote server if it is defined.

        Args:
            queue (int, optional): The number of sub-jobs to run. This argmuent will set the num_jobs attribute of
                this object. Defaults to None, meaning the value of num_jobs will be used.
            options (list of str, optional): A list of command line options for the condor_submit command. For
                details on valid options see: http://research.cs.wisc.edu/htcondor/manual/current/condor_submit.html.
                Defaults to an empty list.

        """

        if not self.executable:
            log.error('Job %s was submitted with no executable', self.name)
            raise NoExecutable('You cannot submit a job without an executable')

        self._num_jobs = queue or self.num_jobs

        self._write_job_file()

        args = ['condor_submit']
        args.extend(options)
        args.append(self.job_file)

        log.info('Submitting job %s with options: %s', self.name, args)
        out, err = self._execute(args)
        if err:
            if re.match('WARNING',err):
                print(err)
            else:
                raise HTCondorError(err)
        print(out)
        try:
            self._cluster_id = int(re.search('(?<=cluster |\*\* Proc )(\d*)', out).group(1))
        except:
            self._cluster_id = -1
        return self.cluster_id

    def remove(self, options=[], sub_job_num=None):
        """Removes a job from the job queue, or from being executed.

        Args:
            options (list of str, optional): A list of command line options for the condor_rm command. For
                details on valid options see: http://research.cs.wisc.edu/htcondor/manual/current/condor_rm.html.
                Defaults to an empty list.
            job_num (int, optional): The number of sub_job to remove rather than the whole cluster. Defaults to None.

        """
        args = ['condor_rm']
        args.extend(options)
        job_id = '%s.%s' % (self.cluster_id, sub_job_num) if sub_job_num else self.cluster_id
        args.append(job_id)
        out, err = self._execute(args)
        print(out,err)

    def edit(self):
        """Interface for CLI edit command.

        Note:
            This method is not implemented.

        """
        raise NotImplementedError("This method is not yet implemented")

    def wait(self, options=[], sub_job_num=None):
        """Wait for the job, or a sub-job to complete.

        Args:
            options (list of str, optional): A list of command line options for the condor_wait command. For
                details on valid options see: http://research.cs.wisc.edu/htcondor/manual/current/condor_wait.html.
                Defaults to an empty list.
            job_num (int, optional): The number
        """
        args = ['condor_wait']
        args.extend(options)
        job_id = '%s.%s' % (self.cluster_id, sub_job_num) if sub_job_num else str(self.cluster_id)
        if self._remote:
            abs_log_file = self.log_file
        else:
            abs_log_file = os.path.abspath(self.log_file)
        args.extend([abs_log_file, job_id])
        out, err = self._execute(args)
        return out, err

    def get(self, attr, value=None, resolve=True):
        """Get the value of an attribute from submit description file.

        Args:
            attr (str): The name of the attribute whose value should be returned.
            value (str, optional): A default value to return if 'attr' doesn't exist. Defaults to None.
            resolve (bool, optional): If True then resolve references to other attributes in the value of 'attr'. If
                False then return the raw value of 'attr'. Defaults to True.

        Returns:
            str: The value assigned to 'attr' if 'attr' exists, otherwise 'value'.
        """
        try:
            if resolve:
                value = self._resolve_attribute(attr)
            else:
                value = self.attributes[attr]
        except KeyError:
            pass
        return value

    def set(self, attr, value):
        """Set the value of an attribute in the submit description file.

        Args:
            attr (str): The name of the attribute to set.
            value (str): The value to assign to 'attr'.

        """
        if value is False:
            value = 'false'
        elif value is True:
            value = 'true'
        elif isinstance(value, list) or isinstance(value, tuple):
            value = ', '.join([str(i) for i in value])
        self.attributes[attr] = value

    def delete(self, attr):
        """Delete an attribute from the submit description file

        Args:
            attr (str): The name of the attribute to delete.

        """
        self.attributes.pop(attr)

    def sync_remote_output(self):
        """Sync the initial directory containing the output and log files with the remote server.

        """
        self._copy_output_from_remote()

    def _update_status(self, sub_job_num=None):
        """Gets the job status.

        Return:
            str: The current status of the job

        """
        job_id = '%s.%s' % (self.cluster_id, sub_job_num) if sub_job_num else str(self.cluster_id)
        format = ['-format', '"%d"', 'JobStatus']
        args = ['condor_q', job_id]
        args.extend(format)
        out, err = self._execute(args)
        if err:
            log.error('Error while updating status for job %s: %s', job_id, err)
            raise HTCondorError(err)
        if not out:
            args = ['condor_history', job_id]
            args.extend(format)
            out, err = self._execute(args)
        if err:
            log.error('Error while updating status for job %s: %s', job_id, err)
            raise HTCondorError(err)
        if not out:
            log.error('Error while updating status for job %s: Job not found.', job_id)
            raise HTCondorError('Job not found.')

        out = out.strip('\"')
        log.info('Job %s status: %s', job_id, out)

        if not sub_job_num:
            assert len(out) == self.num_jobs

        #initialize status dictionary
        status_dict = dict()
        for val in CONDOR_JOB_STATUSES.itervalues():
            status_dict[val] = 0

        for status_code_str in out:
            status_code = int(status_code_str)
            key = CONDOR_JOB_STATUSES[status_code]
            status_dict[key] += 1

        return status_dict

    @set_cwd
    def _execute(self, args):
        out = None
        err = None
        if self._remote:
            log.info('Executing remote command %s', ' '.join(args))
            cmd = ' '.join(args)
            try:
                cmd = 'cd %s && %s' % (self._remote_id, cmd)
                out = '\n'.join(self._remote.execute(cmd))
            except RemoteCommandFailed as e:
                err = e.output
            except SSHError as e:
                err = e.msg
        else:
            log.info('Executing local command %s', ' '.join(args))
            process = subprocess.Popen(args, stdout = subprocess.PIPE, stderr=subprocess.PIPE)
            out,err = process.communicate()

        log.info('Execute results - out: %s, err: %s', out, err)
        return out, err

    @set_cwd
    def _copy_input_files_to_remote(self):
        self._remote.put(self.remote_input_files, self._remote_id)

    @set_cwd
    def _copy_output_from_remote(self):
        self._remote.get(os.path.join(self._remote_id, self.initial_dir))

    @set_cwd
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

    @set_cwd
    def _make_dir(self, dir_name):
        try:
            log.info('making directory %s', dir_name)
            if self._remote:
                self._remote.makedirs(os.path.join(self._remote_id,dir_name))
            else:
                os.makedirs(dir_name)
        except OSError:
            log.warn('Unable to create directory %s. It may already exist.', dir_name)

    def _make_job_dirs(self):
        self._make_dir(self.initial_dir)
        log_dir = self.get('logdir')
        if log_dir:
            self._make_dir(os.path.join(self.initial_dir, log_dir))

    def _resolve_attribute(self, attribute):
        """Recursively replaces references to other attributes with their value.

        Args:
            attribute (str): The name of the attribute to resolve.

        Returns:
            str: The resolved value of 'attribute'.

        """
        value = self.attributes[attribute]
        if not value:
            return None
        resolved_value = re.sub('\$\((.*?)\)',self._resolve_attribute_match, value)
        return resolved_value

    def _resolve_attribute_match(self, match):
        """Replaces a reference to an attribute with the value of the attribute.

        Args:
            match (re.match object): A match object containing a match to a reference to an attribute.

        """
        if match.group(1) == 'cluster':
            return str(self.cluster_id)

        return self.get(match.group(1), match.group(0))

    def close_remote(self):
        """Cleans up and closes connection to remote server if defined.

        """
        if self._remote:
            self._remote.execute('rm -rf %s' % (self._remote_id,))
            self._remote.close()
            del self._remote