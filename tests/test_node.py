from unittest import TestCase
from condorpy import Job, DAG, Node, Templates

__author__ = 'sdc50'


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

        
    def test__init__(self):
        pass

    def test__str__(self):
        pass

    def test__repr__(self):
        pass
    
    def test_job(self):
        pass

    def test_pre_script(self):
        pass
    
    def test_post_script(self):
        pass
    
    def test_parent_nodes(self):
        pass
    
    def test_child_nodes(self):
        pass
    
    def test_retry(self):
        pass

    def test_add_parent(self):
        pass

    def test_remove_parent(self):
        pass

    def test_add_child(self):
        pass

    def test_remove_child(self):
        pass

    def test_add(self):
        pass

    def test_remove(self):
        pass

    def test_get_all_family_nodes(self):
        pass

    def test_get_all_ancestors(self):
        pass

    def test_get_all_descendants(self):
        pass

    def test_link_parent_nodes(self):
        pass

    def test_link_child_nodes(self):
        pass

    def test_list_relations(self):
        node = Node(self.job_a, post_script='script', post_script_args='%s %s %s' % ('arg1', 'arg2', 'arg3'))
        expected = 'SCRIPT POST a script arg1 arg2 arg3\n'
        actual = node.list_relations()
        msg = 'testing that post script string is formatted correctly'
        self.assertEqual(expected, actual, '%s\nExpected: %s\nActual:   %s\n' % (msg, expected, actual))

        dag = DAG('test')
        dag.add_node(node)
        print dag
    def test_get_child_names(self):
        pass

    def test_get_parent_names(self):
        pass

    def test_get_names(self):
        pass