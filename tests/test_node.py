import os, shutil

from unittest import TestCase
from condorpy import Job, Workflow, Node, Templates

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
        try:
            shutil.rmtree('a')
            shutil.rmtree('b')
            shutil.rmtree('c')
            shutil.rmtree('d')
            os.remove('test_dag.dag')
        except:
            pass

        
    def test__init__(self):
        pass

    def test__str__(self):
        pass

    def test__repr__(self):
        pass
    
    def test_job(self):
        """

        :return:
        """
        pass
        # dag = DAG('test_dag')
        # dag.add_node(self.node_a)
        # self.node_b.pre_script = 'pre'
        # self.node_c.post_script = 'post'
        # self.node_c.post_script_args = 'arg1 arg2'
        # try:
        #     dag.submit()
        # except:
        #     pass
        # expected = 'JOB b b/b.job\n' \
        #            'JOB c c/c.job\n' \
        #            'JOB d d/d.job\n' \
        #            'JOB a a/a.job\n\n' \
        #            'SCRIPT PRE b pre\n' \
        #            'SCRIPT POST c post arg1 arg2\n\n' \
        #            'PARENT b CHILD d\n' \
        #            'PARENT c CHILD d\n' \
        #            'PARENT a CHILD b c\n\n'
        #
        # with open(dag.dag_file, 'r') as dag_file:
        #     actual = dag_file.read()
        # msg = 'testing that the dag file is created properly when the dag is submitted'
        # self.assertEqual(expected, actual, '%s\nExpected: \n%s\nActual:   \n%s\n|' % (msg, expected, actual))

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
        pass

    def test_list_scripts(self):
        node = Node(self.job_a, post_script='script', post_script_args=' '.join(('arg1', 'arg2', 'arg3')))
        expected = 'SCRIPT POST a script arg1 arg2 arg3\n'
        actual = node.list_scripts()
        msg = 'testing that post script string is formatted correctly'
        self.assertEqual(expected, actual, '%s\nExpected: %s\nActual:   %s\n' % (msg, expected, actual))

    def test_list_options(self):
        pass

    def test_get_child_names(self):
        pass

    def test_get_parent_names(self):
        pass

    def test_get_names(self):
        pass