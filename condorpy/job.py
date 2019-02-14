# Copyright (c) 2015 Scott Christensen
#
# This file is part of condorpy
#
# condorpy is free software: you can redistribute it and/or modify it under
# the terms of the BSD 2-Clause License. A copy of the BSD 2-Clause License
# should have been distributed with this file.

import os
import re
from collections import OrderedDict

from .htcondor_object_base import HTCondorObjectBase
from .static import CONDOR_JOB_STATUSES
from .logger import log
from .exceptions import NoExecutable, RemoteError, HTCondorError


class Job(HTCondorObjectBase):
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
        object.__setattr__(self, '_attributes', OrderedDict())
        object.__setattr__(self, '_num_jobs', int(num_jobs))
        object.__setattr__(self, '_job_file', '')
        super(Job, self).__init__(host, username, password, private_key, private_key_pass, remote_input_files, working_directory)

        attributes = attributes or OrderedDict()
        attributes['job_name'] = name
        attributes.update(kwargs)
        for attr, value in list(attributes.items()):
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
        if key in self.__dict__ or '_' + key in self.__dict__:
            object.__setattr__(self, key, value)
        else:
            self.set(key, value)

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

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
    def status(self):
        """The status

        """
        if self.cluster_id == self.NULL_CLUSTER_ID:
            return "Unexpanded"

        status_dict = self.statuses
        # determine job status
        status = "Various"
        for key, val in status_dict.items():
            if val == self.num_jobs:
                status = key
        return status

    @property
    def statuses(self):
        """
        Return dictionary of all process statuses
        """
        if self.cluster_id == self.NULL_CLUSTER_ID:
            return "Unexpanded"

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
            initial_dir = os.curdir #TODO does this conflict with the working directory?
        if self._remote and os.path.isabs(initial_dir):
            raise RemoteError('Cannot define an absolute path as an initial_dir on a remote scheduler')
        return initial_dir

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
        return super(Job, self).submit(args)

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

        The value can be passed in as a Python type (i.e. a list, a tuple or a Python boolean).
        The Python values will be reformatted into strings based on the standards described in
        the HTCondor manual: http://research.cs.wisc.edu/htcondor/manual/current/condor_submit.html

        Args:
            attr (str): The name of the attribute to set.
            value (str): The value to assign to 'attr'.

        """

        def escape_new_syntax(value, double_quote_escape='"'):
            value = str(value)
            value = value.replace("'", "''")
            value = value.replace('"', '%s"' % double_quote_escape)
            if ' ' in value or '\t' in value:
                value = "'%s'" % value
            return value

        def escape_new_syntax_pre_post_script(value):
            return escape_new_syntax(value, '\\')

        def escape_remap(value):
            value = value.replace('=', '\=')
            value = value.replace(';', '\;')
            return value

        def join_function_template(join_string, escape_func):
            return lambda value: join_string.join([escape_func(i) for i in value])

        def quote_join_function_template(join_string, escape_func):
            return lambda value: join_function_template(join_string, escape_func)(value)

        join_functions = {'rempas': quote_join_function_template('; ', escape_remap),
                          'arguments': quote_join_function_template(' ', escape_new_syntax),
                          'Arguments': quote_join_function_template(' ', escape_new_syntax_pre_post_script)
                          }

        if value is False:
            value = 'false'
        elif value is True:
            value = 'true'
        elif isinstance(value, list) or isinstance(value, tuple):
            join_function = join_function_template(', ', str)
            for key in list(join_functions.keys()):
                if attr.endswith(key):
                    join_function = join_functions[key]
            value = join_function(value)

        self.attributes[attr] = value

    def delete(self, attr):
        """Delete an attribute from the submit description file

        Args:
            attr (str): The name of the attribute to delete.

        """
        self.attributes.pop(attr)

    def _update_status(self, sub_job_num=None):
        """Gets the job status.

        Return:
            str: The current status of the job

        """
        job_id = '%s.%s' % (self.cluster_id, sub_job_num) if sub_job_num else str(self.cluster_id)
        format = ['-format', '"%d"', 'JobStatus']
        cmd = 'condor_q {0} {1} && condor_history {0} {1}'.format(job_id, ' '.join(format))
        args = [cmd]
        out, err = self._execute(args, shell=True, run_in_job_dir=False)
        if err:
            log.error('Error while updating status for job %s: %s', job_id, err)
            raise HTCondorError(err)
        if not out:
            log.error('Error while updating status for job %s: Job not found.', job_id)
            raise HTCondorError('Job not found.')

        out = out.replace('\"', '')
        log.info('Job %s status: %s', job_id, out)

        if not sub_job_num:
            if len(out) >= self.num_jobs:
                out = out[:self.num_jobs]
            else:
                msg = 'There are {0} sub-jobs, but {1} status(es).'.format(self.num_jobs, len(out))
                log.error(msg)
                raise HTCondorError(msg)

        #initialize status dictionary
        status_dict = dict()
        for val in CONDOR_JOB_STATUSES.values():
            status_dict[val] = 0

        for status_code_str in out:
            status_code = 0
            try:
                status_code = int(status_code_str)
            except ValueError:
                pass
            key = CONDOR_JOB_STATUSES[status_code]
            status_dict[key] += 1

        return status_dict

    def _list_attributes(self):
        attribute_list = []
        for k, v in list(self.attributes.items()):
            if v:
                attribute_list.append(k + ' = ' + str(v))
        return attribute_list

    def _write_job_file(self):
        self._make_job_dirs()
        job_file = self._open(self.job_file, 'w')
        job_file.write(self.__str__())
        job_file.close()
        if self._remote:
            self._copy_input_files_to_remote()

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
