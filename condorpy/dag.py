__author__ = 'sdc50'

from job import Job


class DAG(object):
    """

    """
    def __init__(self,name):
        """
        """
        self._name = name

    def __str__(self):
        pass

    def __repr__(self):
        pass




class Node(object):
    """

    """

    def __init__(self, job=None):
        self._job = job
        self._parent_nodes = []
        self._child_nodes = []

    def __str__(self):
        pass

    def __repr__(self):
        pass

    @property
    def job(self):
        return self._job

    @job.setter
    def job(self, job):
        if isinstance(job, Job):
            self._job = job
        else:
            raise TypeError

    @property
    def parent_nodes(self):
        return self._parent_nodes

    def add_parent(self, parent):
        assert isinstance(parent, Node)
        self._parent_nodes.append(parent)

    def add_child(self, child):
        assert isinstance(child, Node)
        self._child_nodes.append(child)