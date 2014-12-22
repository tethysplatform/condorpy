__author__ = 'sdc50'

from job import Job

#TODO: add node type (DATA, JOB)
class Node(object):
    """

    """

    def __init__(self, job,
                 parents=None,
                 children=None,
                 variables=None,
                 pre_script=None,
                 post_script=None,
                 retry=0):
        """

        :param job:
        :param parents:
        :param children:
        :param pre_script:
        :param post_script:
        :param retry:
        :return:
        """
        self._job = job
        self._parent_nodes = parents or set()
        self._link_parent_nodes()
        self._child_nodes = children or set()
        self._link_child_nodes()
        self._variables = variables or dict()
        self._pre_script = pre_script
        self._post_script = post_script
        self._retry = retry

    def __str__(self):
        """

        :return:
        """
        return 'JOB %s %s\n' % (self.job.name, self.job.job_file)


    def __repr__(self):
        """

        :return:
        """
        return '<NODE: Job:%s Parents:%s Children:%s>' % (self.job.name, self._get_parent_names(), self._get_child_names())

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
        if isinstance(job, Job):
            self._job = job
        else:
            raise TypeError

    @property
    def pre_script(self):
        """

        :return:
        """
        return self._pre_script

    @pre_script.setter
    def pre_script(self, script):
        """

        :param script:
        :return:
        """
        self._pre_script = script

    @property
    def post_script(self):
        """

        :return:
        """
        return self._pre_script

    @post_script.setter
    def post_script(self, script):
        """

        :param script:
        :return:
        """
        self._post_script = script

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

    @property
    def retry(self):
        """

        :return:
        """
        return self._retry

    @retry.setter
    def retry(self, retry):
        """

        :return:
        """
        self._retry = retry

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
            raise
        #TODO: make loop exception
        return ancestors

    def _get_all_descendants(self):
        """
        traverses all descendants nodes
        :raises: LoopException if self is contained in descendants
        :return: a set containing all descendant nodes
        """
        descendants = set()
        descendants = descendants.union(self.child_nodes)
        for child in self.child_nodes:
            descendants = descendants.union(child._get_all_descendants())

        if self in descendants:
            raise
            #TODO: make loop exception
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

    def list_relations(self):
        """

        :return:
        """
        result = ''
        if len(self.child_nodes):
            result += 'PARENT %s CHILD %s\n' % (self.job.name, self._get_child_names())
        if self.retry:
            result += 'RETRY %s %d\n' % (self.job.name, self.retry)
        return result

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
        names = ''
        for node in nodes:
            names += node.job.name + ' '
        return names