# Copyright (c) 2015 Scott Christensen
#
# This file is part of condorpy
#
# condorpy is free software: you can redistribute it and/or modify it under
# the terms of the BSD 2-Clause License. A copy of the BSD 2-Clause License
# should have been distributed with this file.

import os, subprocess, re, uuid
from collections import OrderedDict
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
        """A shortcut for the 'get' method.

        Args:
            item (str): The name of the attribute to get.

        Returns:
            The value assigned to 'item' if defined. Otherwise None.
        """
        return self.get(item)

    def __setattr__(self, key, value):
        """A shortcut for the 'set' method.

        Args:
            key (str): The name of the attribute to set.
            value (str): The value to assign to 'key'.
        """
        if  key in self.__dict__ or '_' + key in self.__dict__:
            object.__setattr__(self, key, value)
        else:
            self.set(key, value)

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

        """
        initial_dir = self.get('initialdir')
        if not initial_dir:
            initial_dir = os.path.relpath(os.getcwd())
        if self._remote and os.path.isabs(initial_dir):
                raise Exception('Cannot define an absolute path as an initial_dir on a remote scheduler')
        return initial_dir

    @property
    def remote_input_files(self):
        """A list of file to be copied to a remote server for remote job submission.

        """
        return self._remote_input_files

    @remote_input_files.setter
    def remote_input_files(self, files):
        """A list of file to be copied to a remote server for remote job submission.

        Args:
            files (list of strings): A list of file paths to all input files and the executable that are required to
                be copied to the remote server when submitting the job remotely.

        Note:
            File paths defined for remote_input_files should be relative to the current working directory on the
            client machine. Input file paths defined for the submit description file should be relative to the
            initial directory on the remote server.

        """
        self._remote_input_files = files

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

    def status(self):
        """Gets the job status.

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
        abs_log_file = os.path.abspath(self.log_file)
        args.extend([abs_log_file, job_id])
        out, err = self._execute(args)

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
        try:
            if self._remote:
                self._remote.makedirs(os.path.join(self._remote_id,dir_name))
            else:
                os.makedirs(dir_name)
        except OSError:
            pass

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
        value = self.get(attribute)
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

    def __del__(self):
        """Cleans up and closes connection to remote server if defined.

        """
        if self._remote:
            self._remote.execute('rm -rf %s' % (self._remote_id,))
            self._remote.close()
            del self._remote



class NoExecutable(Exception):
    """docstring

    """
    pass



