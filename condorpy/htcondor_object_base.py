# Copyright (c) 2015 Scott Christensen
#
# This file is part of condorpy
#
# condorpy is free software: you can redistribute it and/or modify it under
# the terms of the BSD 2-Clause License. A copy of the BSD 2-Clause License
# should have been distributed with this file.

import os
import re
import uuid
import subprocess

from .logger import log
from .exceptions import HTCondorError

from .remote_utils import RemoteClient
from paramiko import SSHException



class HTCondorObjectBase(object):
    """

    """
    NULL_CLUSTER_ID = 0

    def __init__(self,
                 host=None,
                 username=None,
                 password=None,
                 private_key=None,
                 private_key_pass=None,
                 remote_input_files=None,
                 working_directory='.',
                 port=22):
        """


        """
        object.__setattr__(self, '_cluster_id', self.NULL_CLUSTER_ID)
        object.__setattr__(self, '_remote', None)
        object.__setattr__(self, '_remote_input_files', remote_input_files or None)
        object.__setattr__(self, '_cwd', working_directory)
        object.__setattr__(self, '_remote', None)
        object.__setattr__(self, '_remote_id', None)
        if host:
            self.set_scheduler(host=host, port=port, username=username, password=password,
                               private_key=private_key, private_key_pass=private_key_pass)

    @property
    def cluster_id(self):
        """
        The id assigned to the job (called a cluster in HTConodr) when the job is submitted.
        """
        return self._cluster_id

    @property
    def num_jobs(self):
        return 1

    @property
    def scheduler(self):
        """
        The remote scheduler where the job/workflow will be submitted
        """
        return self._remote

    def set_scheduler(self, host, username='root', password=None, private_key=None, private_key_pass=None, port=22):
        """
        Defines the remote scheduler

        Args:
            host (str): the hostname or ip address of the remote scheduler
            username (str, optional): the username used to connect to the remote scheduler. Default is 'root'
            password (str, optional): the password for username on the remote scheduler. Either the password or the private_key must be defined. Default is None.
            private_key (str, optional): the path to the private ssh key used to connect to the remote scheduler. Either the password or the private_key must be defined. Default is None.
            private_key_pass (str, optional): the passphrase for the private_key. Default is None.
            port (int, optional): the SSH port of the remote scheduler. Default is 22.

        Returns:
            An RemoteClient representing the remote scheduler.
        """
        self._remote = RemoteClient(host, username, password, private_key, private_key_pass, port=port)
        self._remote_id = uuid.uuid4().hex

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
            File paths defined for remote_input_files should be relative to the job's working directory on the
            client machine. They are copied into the working directory on the remote. Input file paths defined for
            the submit description file should be relative to the initial directory on the remote server.

        """
        self._remote_input_files = list(files) if files else None

    def set_cwd(fn):
        """
        Decorator to set the specified working directory to execute the function, and then restore the previous cwd.
        """
        def wrapped(self, *args, **kwargs):
            log.info('Calling function: %s with args=%s', fn, args if args else [])
            # Use the CONDORPY_HOME environment variable to force set_cwd to always change to the same directory
            cwd = os.environ.get('CONDORPY_HOME', os.getcwd())
            log.info('Saved cwd: %s', cwd)
            os.chdir(self._cwd)
            log.info('Changing working directory to: %s', self._cwd)
            try:
                return fn(self, *args, **kwargs)
            finally:
                os.chdir(cwd)
                log.info('Restored working directory to: %s', cwd)

        return wrapped

    def submit(self, args):
        """


        """
        out, err = self._execute(args)
        if err:
            if re.match('WARNING|Renaming', err):
                log.warning(err)
            else:
                raise HTCondorError(err)
        log.info(out)
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
        job_id = '%s.%s' % (self.cluster_id, sub_job_num) if sub_job_num else str(self.cluster_id)
        args.append(job_id)
        out, err = self._execute(args)
        return out,err

    def sync_remote_output(self):
        """Sync the initial directory containing the output and log files with the remote server.

        """
        self._copy_output_from_remote()

    def close_remote(self):
        """Cleans up and closes connection to remote server if defined.

        """
        if self._remote:
            try:
                # first see if remote dir is still there
                self._remote.execute('ls %s' % (self._remote_id,))
                if self.status != 'Completed':
                    self.remove()
                self._remote.execute('rm -rf %s' % (self._remote_id,))
            except RuntimeError:
                pass
            self._remote.close()
            del self._remote

    @set_cwd
    def _execute(self, args, shell=False, run_in_job_dir=True):
        out = None
        err = None
        if self._remote:
            log.info('Executing remote command %s', ' '.join(args))
            cmd = ' '.join(args)
            try:
                if run_in_job_dir:
                    cmd = 'cd %s && %s' % (self._remote_id, cmd)
                out = '\n'.join(self._remote.execute(cmd))
            except RuntimeError as e:
                err = str(e)
            except SSHException as e:
                err = str(e)
        else:
            log.info('Executing local command %s', ' '.join(args))
            process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=shell)
            out, err = process.communicate()
            out = out.decode() if isinstance(out, bytes) else out
            err = err.decode() if isinstance(err, bytes) else err

        log.info('Execute results - out: %s, err: %s', out, err)
        return out, err

    @set_cwd
    def _copy_input_files_to_remote(self):
        self._remote.put(self.remote_input_files, self._remote_id)

    @set_cwd
    def _copy_output_from_remote(self):
        self._remote.get(os.path.join(self._remote_id, self.initial_dir))

    @set_cwd
    def _open(self, file_name, mode='w'):
        if self._remote:
            return self._remote.remote_file(os.path.join(self._remote_id, file_name), mode)
        else:
            return open(file_name, mode)

    @set_cwd
    def _make_dir(self, dir_name):
        try:
            log.info('making directory %s', dir_name)
            if self._remote:
                self._remote.makedirs(os.path.join(self._remote_id, dir_name))
            else:
                os.makedirs(dir_name)
        except OSError:
            log.warn('Unable to create directory %s. It may already exist.', dir_name)
