# Copyright (c) 2015 Scott Christensen
#
# This file is part of condorpy
#
# condorpy is free software: you can redistribute it and/or modify it under
# the terms of the BSD 2-Clause License. A copy of the BSD 2-Clause License
# should have been distributed with this file.
from .logger import log

from .htcondor_object_base import HTCondorObjectBase
from .exceptions import CircularDependency


class Node(object):
    """

    """

    def __init__(self, job,
                 parents=None,
                 children=None,
                 pre_script=None,
                 pre_script_args=None,
                 post_script=None,
                 post_script_args=None,
                 variables=None,    # VARS JobName macroname="string" [macroname="string"... ]
                 priority=None,     # PRIORITY JobName PriorityValue
                 category=None,     # CATEGORY JobName CategoryName
                 retry=None,        # JobName NumberOfRetries [UNLESS-EXIT value]
                 retry_unless_exit_value=None,
                 pre_skip=None,     # JobName non-zero-exit-code
                 abort_dag_on=None, # JobName AbortExitValue [RETURN DAGReturnValue]
                 abort_dag_on_return_value=None,
                 dir=None,
                 noop=None,
                 done=None,
                 ):
        """
        Node constructor

        Args:
            job:
            parents:
            children:
            ...
        """
        self.job = job
        self._parent_nodes = parents or set()
        self._link_parent_nodes()
        self._child_nodes = children or set()
        self._link_child_nodes()
        self.pre_script = pre_script
        self.pre_script_args = pre_script_args
        self.post_script = post_script
        self.post_script_args = post_script_args
        self.vars = variables or dict()
        self.priority = priority
        self.category = category
        self.retry = retry
        self.retry_unless_exit_value = retry_unless_exit_value
        self.pre_skip = pre_skip
        self.abort_dag_on = abort_dag_on
        self.abort_dag_on_return_value = abort_dag_on_return_value
        self.dir = dir
        self.noop = noop
        self.done = done

    def __str__(self):
        """
        Returns:
            A string representing the node as it should be represented in the dag file.
        """
        result = '%s %s %s\n' % (self.type, self.job.name, self.job.job_file)
        if self.dir:
            result += ' DIR %s' % (self.dir,)
        if self.noop:
            result += ' NOOP'
        if self.done:
            result += ' DONE'
        return result

    def __repr__(self):
        """

        :return:
        """
        return '<Node: %s parents(%s) children(%s)>' % (self.job.name, self._get_parent_names(), self._get_child_names())

    @property
    def type(self):
        return 'JOB'

    @property
    def job(self):
        """

        :return:
        """
        return self._job

    @job.setter
    def job(self, job):
        """

        :param job:
        :return:
        """
        if isinstance(job, HTCondorObjectBase):
            self._job = job
        else:
            raise TypeError('%s is not of type Job or Workflow' % (str(job),))

    @property
    def parent_nodes(self):
        """

        :return:
        """
        return self._parent_nodes

    @property
    def child_nodes(self):
        """

        :return:
        """
        return self._child_nodes

    # @property
    # def pre_script(self):
    #     """
    #
    #     :return:
    #     """
    #     return self.pre_script
    #
    # @pre_script.setter
    # def pre_script(self, script):
    #     """
    #
    #     :param script:
    #     :return:
    #     """
    #     self.pre_script = script
    #
    # @property
    # def pre_script_args(self):
    #     return self.pre_script_args
    #
    # @pre_script_args.setter
    # def pre_script_args(self, args):
    #     self.pre_script_args = args
    #
    # @property
    # def post_script(self):
    #     """
    #
    #     :return:
    #     """
    #     return self.post_script
    #
    # @post_script.setter
    # def post_script(self, script):
    #     """
    #
    #     :param script:
    #     :return:
    #     """
    #     self.post_script = script
    #
    # @property
    # def post_script_args(self):
    #     return self.post_script_args
    #
    # @post_script_args.setter
    # def post_script_args(self, args):
    #     self.post_script_args = args
    #
    # @property
    # def vars(self):
    #     """
    #
    #     """
    #     return self.vars
    #
    # @vars.setter
    # def vars(self, vars):
    #     """
    #     vars setter
    #
    #     Args:
    #         vars ():
    #     """
    #     self.vars = vars
    #
    # @property
    # def priority(self):
    #     """
    #
    #     """
    #     return self.priority
    #
    # @priority.setter
    # def priority(self, priority):
    #     """
    #     priority setter
    #
    #     Args:
    #         priority ():
    #     """
    #     self.priority = priority
    #
    # @property
    # def category(self):
    #     """
    #
    #     """
    #     return self.category
    #
    # @category.setter
    # def category(self, category):
    #     """
    #     category setter
    #
    #     Args:
    #         category ():
    #     """
    #     self.category = category
    #
    # @property
    # def retry(self):
    #     """
    #     An integer indicating the number of times to retry running a node.
    #     """
    #     return self.retry
    #
    # @retry.setter
    # def retry(self, retry):
    #     """
    #     retry setter
    #
    #     Args:
    #         retry ():
    #     """
    #     self.retry = retry
    #
    # @property
    # def retry_unless_exit_value(self):
    #     """
    #     An integer indicating the exit value for which not to retry a job
    #     """
    #     return self.retry_unless_exit_value
    #
    # @retry.setter
    # def retry_unless_exit_value(self, retry_unless_exit_value):
    #     """
    #     retry_unless_exit_value setter
    #
    #     Args:
    #         retry_unless_exit_value ():
    #     """
    #     self.retry_unless_exit_value = retry_unless_exit_value
    #
    # @property
    # def pre_skip(self):
    #     """
    #
    #     """
    #     return self.pre_skip
    #
    # @pre_skip.setter
    # def pre_skip(self, pre_skip):
    #     """
    #     pre_skip setter
    #
    #     Args:
    #         pre_skip ():
    #     """
    #     self.pre_skip = pre_skip
    #
    # @property
    # def abort_dag_on(self):
    #     """
    #
    #     """
    #     return self.abort_dag_on
    #
    # @abort_dag_on.setter
    # def abort_dag_on(self, abort_dag_on):
    #     """
    #     abort_dag_on setter
    #
    #     Args:
    #         abort_dag_on ():
    #     """
    #     self.abort_dag_on = abort_dag_on
    #
    # @property
    # def abort_dag_on_return_value(self):
    #     """
    #
    #     """
    #     return self.abort_dag_on
    #
    # @abort_dag_on_return_value.setter
    # def abort_dag_on_return_value(self, abort_dag_on_return_value):
    #     """
    #     abort_dag_on_return_value setter
    #
    #     Args:
    #         abort_dag_on_return_value ():
    #     """
    #     self.abort_dag_on_return_value = abort_dag_on_return_value
    #
    # @property
    # def dir(self):
    #     """
    #
    #     """
    #     return self._dir
    #
    # @dir.setter
    # def dir(self, dir):
    #     """
    #     dir setter
    #
    #     Args:
    #         dir ():
    #     """
    #     self.dir = dir
    #
    # @property
    # def noop(self):
    #     """
    #
    #     """
    #     return self.noop
    #
    # @noop.setter
    # def noop(self, noop):
    #     """
    #     noop setter
    #
    #     Args:
    #         noop ():
    #     """
    #     self.noop = noop
    #
    # @property
    # def done(self):
    #     """
    #
    #     """
    #     return self.done
    #
    # @done.setter
    # def done(self, done):
    #     """
    #     done setter
    #
    #     Args:
    #         done ():
    #     """
    #     self.done = done

    def add_parent(self, parent):
        """

        :param parent:
        :return:
        """
        assert isinstance(parent, Node)
        self.parent_nodes.add(parent)
        if self not in parent.child_nodes:
            parent.add_child(self)

    def remove_parent(self, parent):
        """

        :param parent:
        :return:
        """
        assert isinstance(parent, Node)
        self.parent_nodes.discard(parent)
        if self in parent.child_nodes:
            parent.remove_child(self)

    def add_child(self, child):
        """

        :param child:
        :return:
        """
        assert isinstance(child, Node)
        self.child_nodes.add(child)
        if self not in child.parent_nodes:
            child.add_parent(self)

    def remove_child(self, child):
        """

        :param child:
        :return:
        """
        assert isinstance(child, Node)
        self.child_nodes.discard(child)
        if self in child.parent_nodes:
            child.remove_parent(self)

    def _add(self):
        """

        :return:
        """
        pass

    def _remove(self):
        """

        :return:
        """
        pass

    def get_all_family_nodes(self):
        """

        :return:
        """
        ancestors = self._get_all_ancestors()
        descendants = self._get_all_descendants()
        family_nodes = ancestors.union(descendants)
        return family_nodes

    def _get_all_ancestors(self):
        """

        :return:
        """
        ancestors = set()
        ancestors = ancestors.union(self.parent_nodes)
        for parent in self.parent_nodes:
            ancestors = ancestors.union(parent._get_all_ancestors())

        if self in ancestors:
            log.error('circular dependancy found in %s. Ancestors: %s ', self, ancestors)
            raise CircularDependency('Node %s contains itself in it\'s list of dependencies.' % (self.job.name,))
        return ancestors

    def _get_all_descendants(self):
        """
        traverses all descendants nodes
        :raises: CircularDependency if self is contained in descendants
        :return: a set containing all descendant nodes
        """
        descendants = set()
        descendants = descendants.union(self.child_nodes)
        for child in self.child_nodes:
            descendants = descendants.union(child._get_all_descendants())

        if self in descendants:
            log.error('circular dependancy found in %s. Descendants: %s ', self, descendants)
            raise CircularDependency('Node %s contains itself in it\'s list of dependencies.' % (self.job.name,))
        return descendants

    def _link_parent_nodes(self):
        """

        :return:
        """
        for parent in self.parent_nodes:
            if self not in parent.child_nodes:
                parent.add_child(self)

    def _link_child_nodes(self):
        """

        :return:
        """
        for child in self.child_nodes:
            if self not in child.parent_nodes:
                child.add_parent(self)

    @classmethod
    def all_list_functions(cls):
        return [cls.__str__.__name__,
                cls.list_vars.__name__,
                cls.list_relations.__name__,
                cls.list_scripts.__name__,
                cls.list_pre_skip.__name__,
                cls.list_category.__name__,
                cls.list_priority.__name__,
                cls.list_options.__name__,
                ]

    def list_relations(self):
        """

        :return:
        """
        result = ''
        if len(self.child_nodes):
            result += 'PARENT %s CHILD %s\n' % (self.job.name, self._get_child_names())
        return result

    def list_scripts(self):
        result = ''
        if self.pre_script:
            result += 'SCRIPT PRE %s %s %s\n' % (self.job.name, self.pre_script, self.pre_script_args or '')
        if self.post_script:
            result += 'SCRIPT POST %s %s %s\n' % (self.job.name, self.post_script, self.post_script_args or '')
        return result

    def list_options(self):
        result = ''
        if self.retry:
            result += 'RETRY %s %d\n' % (self.job.name, self.retry)
        return result

    def list_option(self, option):
        result = ''
        value = getattr(self, option)
        if value:
            result += '%s %s %s\n' % (option.upper(), self.job.name, str(value))
        return result

    def list_vars(self):
        result = ''
        if self.vars:
            result = 'VARS %s' % (self.job.name,)
            for key, value in self.vars.items():
                result += ' %s="%s"' % (key, value)
            result += '\n'
        return result

    def list_priority(self):
        return self.list_option('priority')

    def list_category(self):
        return self.list_option('category')

    #TODO retry=None, # JobName NumberOfRetries [UNLESS-EXIT value]

    def list_pre_skip(self):
        return self.list_option('pre_skip')

    #TODO abort_dag_on=None, # JobName AbortExitValue [RETURN DAGReturnValue]

    def _get_child_names(self):
        """

        :return:
        """
        return self._get_names(self.child_nodes)

    def _get_parent_names(self):
        """

        :return:
        """
        return self._get_names(self.parent_nodes)

    def _get_names(self, nodes):
        """

        :return:
        """
        names = []
        for node in nodes:
            names.append(node.job.name)
        return ' '.join(names)
