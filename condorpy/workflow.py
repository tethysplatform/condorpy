# Copyright (c) 2015 Scott Christensen
#
# This file is part of condorpy
#
# condorpy is free software: you can redistribute it and/or modify it under
# the terms of the BSD 2-Clause License. A copy of the BSD 2-Clause License
# should have been distributed with this file.
import os

from condorpy.static import CONDOR_JOB_STATUSES

from condorpy.exceptions import HTCondorError

from .htcondor_object_base import HTCondorObjectBase
from .node import Node
from .logger import log


class Workflow(HTCondorObjectBase):
    """

    """
    def __init__(self,
                 name,
                 config,
                 max_jobs,
                 host=None,
                 username=None,
                 password=None,
                 private_key=None,
                 private_key_pass=None,
                 remote_input_files=None,
                 working_directory='.'):

        self._name = name
        self._config = config
        self._max_jobs = max_jobs
        self._dag_file = ""
        self._node_set = set()
        super(Workflow, self).__init__(host, username, password, private_key, private_key_pass, remote_input_files, working_directory)

    def __str__(self):
        """
        """
        result = []
        if self.config:
            result.append('CONFIG {0}\n'.format(self.config))

        self.complete_node_set()
        list_functions = Node.all_list_functions()
        options = ['']*len(list_functions)
        for node in self._node_set:
            for i, list_function_name in enumerate(list_functions):
                list_function = getattr(node, list_function_name)
                options[i] += list_function()
        result.extend(options)

        if self.max_jobs:
            max_jobs_list = ''
            for category, max_jobs in self.max_jobs.items():
                max_jobs_list += 'MAXJOBS {0} {1}\n'.format(category, str(max_jobs))
            result.append(max_jobs_list)

        return '\n'.join(result)

    def __repr__(self):
        """
        """
        return '<DAG: %s>' % (self.name,)

    @property
    def name(self):
        """
        """
        return self._name

    @property
    def num_jobs(self):
        return len(self._node_set)

    @property
    def config(self):
        """
        """
        return self._config

    @config.setter
    def config(self, config):
        if os.path.exists(config):
            self._config = config

    @property
    def max_jobs(self):
        return self._max_jobs

    def add_max_jobs_throttle(self, category, max_jobs):
        """

        :param category:
        :param max_jobs:
        :return:
        """
        self.max_jobs[category] = max_jobs

    @property
    def node_set(self):
        """
        """
        if self.cluster_id != self.NULL_CLUSTER_ID:
            self.update_node_ids()
        return self._node_set

    @property
    def dag_file(self):
        """
        """
        return '%s.dag' % (self.name,)

    @property
    def initial_dir(self):
        """
        """
        return ''

    @property
    def status(self):
        """
        Returns status of workflow as a whole (DAG status).
        """
        if self.cluster_id == self.NULL_CLUSTER_ID:
            return "Unexpanded"

        return self._update_status()

    @property
    def statuses(self):
        """
        Get status of workflow nodes.
        """
        if self.cluster_id == self.NULL_CLUSTER_ID:
            return "Unexpanded"

        return self._update_statuses()

    def _update_status(self, sub_job_num=None):
        """Gets the workflow status.

        Return:
            str: The current status of the workflow.

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

        out = out.replace('\"', '').split('\n')


        status_code = 0
        for status_code_str in out:
            try:
                status_code = int(status_code_str.strip())
            except:
                pass

        log.info('Job %s status: %d', job_id, status_code)

        key = CONDOR_JOB_STATUSES[status_code]

        return key

    def _update_statuses(self, sub_job_num=None):
        """
        Update statuses of jobs nodes in workflow.
        """
        # initialize status dictionary
        status_dict = dict()

        for val in CONDOR_JOB_STATUSES.values():
            status_dict[val] = 0

        for node in self.node_set:
            job = node.job
            try:
                job_status = job.status
                status_dict[job_status] += 1
            except (KeyError, HTCondorError):
                status_dict['Unexpanded'] += 1

        return status_dict

    def update_node_ids(self, sub_job_num=None):
        """
        Associate Jobs with respective cluster ids.
        """
        # Build condor_q and condor_history commands
        dag_id = '%s.%s' % (self.cluster_id, sub_job_num) if sub_job_num else str(self.cluster_id)
        job_delimiter = '+++'
        attr_delimiter = ';;;'

        format = [
            '-format', '"%d' + attr_delimiter + '"', 'ClusterId',
            '-format', '"%v' + attr_delimiter + '"', 'Cmd',
            '-format', '"%v' + attr_delimiter + '"', 'Args',     # Old way
            '-format', '"%v' + job_delimiter + '"', 'Arguments'  # New way
        ]
        # Get ID, Executable, and Arguments for each job that is either started to be processed or finished in the workflow
        cmd = 'condor_q -constraint DAGManJobID=={0} {1} && condor_history -constraint DAGManJobID=={0} {1}'.format(dag_id, ' '.join(format))

        # 'condor_q -constraint DAGManJobID==1018 -format "%d\n" ClusterId -format "%s\n" CMD -format "%s\n" ARGS && condor_history -constraint DAGManJobID==1018 -format "%d\n" ClusterId -format "%s\n" CMD -format "%s\n" ARGS'
        _args = [cmd]
        out, err = self._execute(_args, shell=True, run_in_job_dir=False)

        if err:
            log.error('Error while associating ids for jobs dag %s: %s', dag_id, err)
            raise HTCondorError(err)
        if not out:
            log.warning('Error while associating ids for jobs in dag %s: No jobs found for dag.', dag_id)

        try:
            # Split into one line per job
            jobs_out = out.split(job_delimiter)

            # Match node to cluster id using combination of cmd and arguments
            for node in self._node_set:
                job = node.job

                # Skip jobs that already have cluster id defined
                if job.cluster_id != job.NULL_CLUSTER_ID:
                    continue

                for job_out in jobs_out:
                    if not job_out or attr_delimiter not in job_out:
                        continue

                    # Split line by attributes
                    cluster_id, cmd, _args, _arguments = job_out.split(attr_delimiter)

                    # If new form of arguments is used, _args will be 'undefined' and _arguments will not
                    if _args == 'undefined' and _arguments != 'undefined':
                        args = _arguments.strip()

                    # If both are undefined, then there are no arguments
                    elif _args == 'undefined' and _arguments == 'undefined':
                        args = None

                    # Otherwise, using old form and _arguments will be 'undefined' and _args will not.
                    else:
                        args = _args.strip()

                    job_cmd = job.executable
                    job_args = job.arguments.strip() if job.arguments else None

                    if job_cmd in cmd and job_args == args:
                        log.info('Linking cluster_id %s to job with command and arguments: %s %s', cluster_id,
                                  job_cmd, job_args)
                        job._cluster_id = int(cluster_id)
                        break

        except ValueError as e:
            log.warning(str(e))

    def add_node(self, node):
        """
        """
        assert isinstance(node, Node)
        self._node_set.add(node)

    def add_job(self, job):
        """

        :param job:
        :return:
        """
        node = Node(job)
        self.add_node(node)
        return node

    def submit(self, options=[]):
        """
        ensures that all relatives of nodes in node_set are also added to the set before submitting
        """
        self.complete_node_set()
        self._write_job_file()

        args = ['condor_submit_dag']
        args.extend(options)
        args.append(self.dag_file)

        log.info('Submitting workflow %s with options: %s', self.name, args)
        return super(Workflow, self).submit(args)

    def wait(self, options=[]):
        """

        :return:
        """
        args = ['condor_wait']
        args.extend(options)
        args.append('%s.dagman.log' % (self.dag_file))

        return self._execute(args)

    def complete_node_set(self):
        """
        """
        complete_node_set = set()
        for node in self.node_set:
            complete_node_set.add(node)
            complete_node_set = complete_node_set.union(node.get_all_family_nodes())

        self._node_set = complete_node_set

    def _write_job_file(self):
        """
        """
        log.debug('writing dag file "%s" in "%s".', self.dag_file, self._cwd)
        self._make_dir(self.initial_dir)
        dag_file = self._open(self.dag_file, 'w')
        dag_file.write(self.__str__())
        dag_file.close()
        for node in self._node_set:
            node.job._remote = self._remote
            node.job._remote_id = self._remote_id
            node.job._cwd = self._cwd
            node.job._write_job_file()


# For backwards compatibility
DAG = Workflow
