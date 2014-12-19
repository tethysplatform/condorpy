from unittest import TestCase
from condorpy import Job, DAG, Node, Templates

__author__ = 'sdc50'


class TestDAG(TestCase):
    pass


class TestNode(TestCase):
    """

    """

    def setUp(self):
        """

        :return:
        """
        self.job_a = Job('a', Templates.base)
        self.job_b = Job('b', Templates.base)
        self.job_c = Job('c', Templates.base)
        self.job_d = Job('d', Templates.base)

        self.node_a = Node(self.job_a)
        self.node_b = Node(self.job_b)
        self.node_c = Node(self.job_c)
        self.node_d = Node(self.job_d)

        self.node_a.add_child(self.node_b)
        self.node_a.add_child(self.node_c)
        self.node_d.add_parent(self.node_b)
        self.node_d.add_parent(self.node_c)



    def tearDown(self):
        """

        :return:
        """
        pass

    def test_job(self):
        """

        :return:
        """
        dag = DAG('test_dag')
        dag.add_node(self.node_a)
        print dag


    def test_parent_nodes(self):
        self.fail()

    def test_add_parent(self):
        self.fail()

    def test_add_child(self):
        self.fail()