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

from htcondor_object_base import HTCondorObjectBase
from node import Node
from logger import log


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
            for category, max_jobs in self.max_jobs.iteritems():
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
        return self.name

    @property
    def status(self):
        return self._update_status()

    def _update_status(self, sub_job_num=None):
        """Gets the workflow status.

        Return:
            str: The current status of the workflow.

        """
        job_id = '%s.%s' % (self.cluster_id, sub_job_num) if sub_job_num else str(self.cluster_id)
        format = ['-format', '"%d"', 'JobStatus']
        cmd = 'condor_q {0} {1} && condor_history {0} {1}'.format(job_id, ' '.join(format))
        args = [cmd]
        out, err = self._execute(args, shell=True)
        if err:
            log.error('Error while updating status for job %s: %s', job_id, err)
            raise HTCondorError(err)
        if not out:
            log.error('Error while updating status for job %s: Job not found.', job_id)
            raise HTCondorError('Job not found.')

        out = out.replace('\"', '')
        log.info('Job %s status: %s', job_id, out)

        for status_code_str in out:
            status_code = int(status_code_str)
            key = CONDOR_JOB_STATUSES[status_code]

        return key

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
        dag_file = self._open(self.dag_file, 'w')
        dag_file.write(self.__str__())
        dag_file.close()
        for node in self._node_set:
            node.job._cwd = self._cwd
            node.job._write_job_file()


# For backwards compatibility
DAG = Workflow
