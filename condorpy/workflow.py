# Copyright (c) 2015 Scott Christensen
#
# This file is part of condorpy
#
# condorpy is free software: you can redistribute it and/or modify it under
# the terms of the BSD 2-Clause License. A copy of the BSD 2-Clause License
# should have been distributed with this file.

from node import Node
from exceptions import HTCondorError
import subprocess, re

class Workflow(object):
    """

    """
    def __init__(self,name):
        """
        """
        self._name = name
        self._dag_file = ""
        self._node_set = set()

    def __str__(self):
        """
        """
        self.complete_node_set()
        jobs = ''
        scripts = ''
        relationships = ''
        options = ''
        for node in self._node_set:
            jobs += str(node)
            scripts += node.list_scripts()
            relationships += node.list_relations()
            options += node.list_options()

        result = '\n'.join((jobs, scripts, relationships, options))

        return result

    def __repr__(self):
        """
        """
        return '<DAG: %s>' % (self.name)

    @property
    def name(self):
        """
        """
        return self._name

    @property
    def cluster_id(self):
        """

        :return:
        """
        return self._cluster_id

    @property
    def node_set(self):
        """
        """
        return self._node_set

    @property
    def dag_file(self):
        """
        """
        return '%s.dag' % (self.name)

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
        self._write_dag_file()
        for node in self._node_set:
            node.job.set('log', node.job.log_file)
            node.job._write_job_file()

        args = ['condor_submit_dag']
        args.extend(options)
        args.append(self.dag_file)

        process = subprocess.Popen(args, stdout = subprocess.PIPE, stderr=subprocess.PIPE)
        out,err = process.communicate()

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

    def wait(self, options=[]):
        """

        :return:
        """
        args = ['condor_wait']
        args.extend(options)
        args.append('%s.dagman.log' % (self.dag_file))

        process = subprocess.Popen(args, stdout = subprocess.PIPE, stderr=subprocess.PIPE)
        process.communicate()

    def complete_node_set(self):
        """
        """
        complete_node_set = set()
        for node in self.node_set:
            complete_node_set.add(node)
            complete_node_set = complete_node_set.union(node.get_all_family_nodes())

        self._node_set = complete_node_set

    def _write_dag_file(self):
        """
        """
        dag_file = open(self.dag_file, 'w')
        dag_file.write(self.__str__())
        dag_file.close()


# For backwards compatibility
DAG = Workflow