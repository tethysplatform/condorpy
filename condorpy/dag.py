__author__ = 'sdc50'

from job import Job


class DAG(object):
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
        result = ""
        for node in self._node_set:
            result += (str(node))

        return result

    def __repr__(self):
        """
        """
        pass

    @property
    def name(self):
        """
        """
        return self._name

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

    def submit(self):
        """
        ensures that all relatives of nodes in node_set are also added to the set before submitting
        """
        self.complete_set()
        self._write_dag_file()

    def complete_set(self):
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


#TODO: add node type (DATA, JOB)
class Node(object):
    """

    """

    def __init__(self, job=None,
                 parents=set(),
                 children=set(),
                 variables=dict(),
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
        self._parent_nodes = parents
        self._link_parent_nodes()
        self._child_nodes = children
        self._link_child_nodes()
        self._variables = variables
        self._pre_script = pre_script
        self._post_script = post_script
        self._retry = retry

    def __str__(self):
        """

        :return:
        """
        result = 'JOB %s %s\n' % (self.job.name, self.job.job_file)
        child_names = ''
        for child in self.child_nodes:
            child_names += child.name + ' '
        result += 'PARENT %s CHILD %s' % (self.job.name, child_names)
        if self.retry:
            result += 'RETRY %s %d' % (self.job.name, self.retry)

    def __repr__(self):
        """

        :return:
        """
        pass

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
        self._parent_nodes.add(parent)
        parent.add_child(self)

    def remove_parent(self, parent):
        """

        :param parent:
        :return:
        """
        assert isinstance(parent, Node)
        self._parent_nodes.discard(parent)
        parent.remove_child(self)

    def add_child(self, child):
        """

        :param child:
        :return:
        """
        assert isinstance(child, Node)
        self._child_nodes.add(child)
        child.add_parent(self)

    def remove_child(self, child):
        """

        :param child:
        :return:
        """
        assert isinstance(child, Node)
        self._child_nodes.discard(child)
        child.remove_parent(self)

    def get_all_family_nodes(self):
        """

        :return:
        """
        ancestors = self._get_all_ancestors()
        descendants = self._get_all_decendants()
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
            parent.add_child(self)

    def _link_child_nodes(self):
        """

        :return:
        """
        for child in self.child_nodes:
            child.add_parent(self)