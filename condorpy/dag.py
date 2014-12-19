__author__ = 'sdc50'

from job import Job
import subprocess, re

#TODO: set initialdir that overrides jobs' initaildir?

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
        self.complete_set()
        result = ''
        for node in self._node_set:
            result += str(node)

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

    def add_node(self, node):
        """
        """
        assert isinstance(node, Node)
        self._node_set.add(node)

    def submit(self, options=None):
        """
        ensures that all relatives of nodes in node_set are also added to the set before submitting
        """
        self.complete_set()
        self._write_dag_file()
        for node in self._node_set:
            node.job._write_job_file()

        args = ['condor_submit_dag']
        if options:
            args.append(options)
        args.append(self.dag_file)

        process = subprocess.Popen(args, stdout = subprocess.PIPE, stderr=subprocess.PIPE)
        out,err = process.communicate()

        if err:
            if re.match('WARNING',err):
                print(err)
            else:
                raise Exception(err)
        print out


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
        result = 'JOB %s %s\n' % (self.job.name, self.job.job_file)
        if len(self.child_nodes):
            result += 'PARENT %s CHILD %s\n' % (self.job.name, self._get_child_names())
        if self.retry:
            result += 'RETRY %s %d\n' % (self.job.name, self.retry)
        return result

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