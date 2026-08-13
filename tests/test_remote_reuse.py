'''
Tests for private key caching, remote client reuse, and node id resolution.
'''
import logging
import os
import shutil
import tempfile
import sys
import threading
import unittest
from unittest import mock

import paramiko

from condorpy import Job, Workflow
from condorpy.htcondor_object_base import HTCondorObjectBase
from condorpy.node import Node
from condorpy.remote_utils import RemoteClient, load_private_key
import condorpy.remote_utils as remote_utils


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestPrivateKeyCache))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestRemoteClientReuse))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestNodeIdResolution))
    return suite


class TestPrivateKeyCache(unittest.TestCase):

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.key_path = os.path.join(self.dir, 'id_rsa')
        paramiko.RSAKey.generate(2048).write_private_key_file(self.key_path)
        remote_utils._private_key_cache.clear()

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)
        remote_utils._private_key_cache.clear()

    def test_key_is_decrypted_once(self):
        with mock.patch.object(paramiko.RSAKey, 'from_private_key_file',
                               wraps=paramiko.RSAKey.from_private_key_file) as from_file:
            first = load_private_key(self.key_path)
            second = load_private_key(self.key_path)
        self.assertIs(first, second)
        self.assertEqual(1, from_file.call_count)

    def test_modified_key_file_is_reloaded(self):
        first = load_private_key(self.key_path)
        os.utime(self.key_path, (0, 0))
        second = load_private_key(self.key_path)
        self.assertIsNot(first, second)

    def test_client_uses_cached_key(self):
        a = RemoteClient('host', 'user', private_key=self.key_path)
        b = RemoteClient('host', 'user', private_key=self.key_path)
        self.assertIs(a.private_key, b.private_key)

    def rotate_key(self, mtime):
        paramiko.RSAKey.generate(2048).write_private_key_file(self.key_path)
        os.utime(self.key_path, (mtime, mtime))

    def test_cache_keeps_one_entry_per_key_file(self):
        for i in range(5):
            self.rotate_key(i * 1000)
            load_private_key(self.key_path)
        self.assertEqual(1, len(remote_utils._private_key_cache))

    def test_rotated_key_file_replaces_a_reused_client(self):
        job = Job('rotating')
        job.set_scheduler('host', 'user', private_key=self.key_path)
        first_key = job.scheduler.private_key

        self.rotate_key(9999)
        job.set_scheduler('host', 'user', private_key=self.key_path)

        self.assertIsNot(first_key, job.scheduler.private_key)

    def test_rotated_key_file_keeps_the_remote_working_directory(self):
        job = Job('rotating')
        job.set_scheduler('host', 'user', private_key=self.key_path)
        remote_id = job._remote_id

        self.rotate_key(9999)
        job.set_scheduler('host', 'user', private_key=self.key_path)

        self.assertEqual(remote_id, job._remote_id)

    def test_new_host_gets_a_new_remote_working_directory(self):
        job = Job('moving')
        job.set_scheduler('host', 'user', private_key=self.key_path)
        remote_id = job._remote_id

        job.set_scheduler('other-host', 'user', private_key=self.key_path)

        self.assertNotEqual(remote_id, job._remote_id)


class TestRemoteClientReuse(unittest.TestCase):

    def setUp(self):
        self.job = Job('test_reuse')

    def test_scheduler_client_is_reused(self):
        self.job.set_scheduler('host', 'user', password='pass')
        first = self.job.scheduler
        self.job.set_scheduler('host', 'user', password='pass')
        self.assertIs(first, self.job.scheduler)

    def test_scheduler_client_replaced_when_host_changes(self):
        self.job.set_scheduler('host', 'user', password='pass')
        first = self.job.scheduler
        self.job.set_scheduler('other-host', 'user', password='pass')
        self.assertIsNot(first, self.job.scheduler)

    def test_remote_id_preserved_when_client_reused(self):
        self.job.set_scheduler('host', 'user', password='pass')
        remote_id = self.job._remote_id
        self.job.set_scheduler('host', 'user', password='pass')
        self.assertEqual(remote_id, self.job._remote_id)


class TestNodeIdResolution(unittest.TestCase):

    def setUp(self):
        self.workflow = Workflow('test_nodes', config='', max_jobs=None)
        self.workflow._cluster_id = 42

    def resolved_node(self, name, cluster_id=7):
        node = Node(Job(name))
        node.job._cluster_id = cluster_id
        return node

    def test_node_set_queries_while_nodes_are_unresolved(self):
        self.workflow.add_job(Job('unresolved'))
        with mock.patch.object(Workflow, 'update_node_ids') as update_node_ids:
            self.workflow.node_set
            self.workflow.node_set
            self.assertEqual(2, update_node_ids.call_count)

    def test_node_set_skips_query_when_all_nodes_are_resolved(self):
        self.workflow.add_node(self.resolved_node('done'))
        with mock.patch.object(Workflow, 'update_node_ids') as update_node_ids:
            self.workflow.node_set
            self.workflow.node_set
            update_node_ids.assert_not_called()

    def test_empty_node_set_is_not_queried(self):
        with mock.patch.object(Workflow, 'update_node_ids') as update_node_ids:
            self.workflow.node_set
            update_node_ids.assert_not_called()

    def test_node_added_after_resolution_is_resolved(self):
        self.workflow.add_node(self.resolved_node('done'))
        self.workflow.add_job(Job('new_node'))
        with mock.patch.object(Workflow, 'update_node_ids') as update_node_ids:
            self.workflow.node_set
            update_node_ids.assert_called_once()

    def test_complete_node_set_does_not_mask_unresolved_nodes(self):
        child = self.resolved_node('child')
        self.workflow.add_node(child)
        child.add_parent(Node(Job('parent')))

        with mock.patch.object(Workflow, '_execute', return_value=('', None)):
            self.workflow.node_set
            self.workflow.complete_node_set()

        with mock.patch.object(Workflow, 'update_node_ids') as update_node_ids:
            self.workflow.node_set
            update_node_ids.assert_called_once()

    def test_unparseable_status_records_are_logged(self):
        self.workflow.add_node(self.resolved_node('a', cluster_id=100))
        out = '100;;;2+++GARBAGE_NO_DELIMITER+++999;;;notanint+++'

        with mock.patch.object(Workflow, '_execute', return_value=(out, None)):
            with self.assertLogs('condorpy', level='WARNING') as logged:
                statuses = self.workflow.node_statuses_by_cluster_id()

        self.assertEqual({100: 'Running'}, statuses)
        self.assertTrue(any('2' in line for line in logged.output))

    def test_fully_parseable_status_records_are_not_logged(self):
        self.workflow.add_node(self.resolved_node('a', cluster_id=100))
        log = logging.getLogger('condorpy')

        with mock.patch.object(Workflow, '_execute', return_value=('100;;;2+++', None)):
            with mock.patch.object(log, 'warning') as warning:
                self.workflow.node_statuses_by_cluster_id()

        warning.assert_not_called()

    def multiproc_node(self, name, num_jobs, cluster_id):
        job = Job(name)
        job.num_jobs = num_jobs
        job._cluster_id = cluster_id
        return Node(job)

    def test_disagreeing_procs_are_reported_as_various(self):
        self.workflow.add_node(self.multiproc_node('mp', 2, 500))

        with mock.patch.object(Workflow, '_execute', return_value=('500;;;2+++500;;;1+++', None)):
            statuses = self.workflow.node_statuses_by_cluster_id()

        self.assertEqual({500: 'Various'}, statuses)

    def test_agreeing_procs_are_reported_as_their_shared_status(self):
        self.workflow.add_node(self.multiproc_node('mp', 2, 500))

        with mock.patch.object(Workflow, '_execute', return_value=('500;;;2+++500;;;2+++', None)):
            statuses = self.workflow.node_statuses_by_cluster_id()

        self.assertEqual({500: 'Running'}, statuses)

    def test_multiproc_node_counts_match_the_per_node_path(self):
        self.workflow.add_node(self.multiproc_node('mp', 2, 500))

        def respond(args, **kwargs):
            if 'DAGManJobID' in args[0]:
                return '500;;;2+++500;;;1+++', None
            return '21', None

        with mock.patch.object(HTCondorObjectBase, '_execute', side_effect=respond):
            counts = self.workflow._update_statuses()

        self.assertEqual(1, counts['Unexpanded'])
        self.assertEqual(0, counts['Idle'])
        self.assertEqual(0, counts['Running'])

    def batched_and_per_node(self, batched_out, per_node_out='2'):
        def respond(args, **kwargs):
            if 'DAGManJobID' in args[0]:
                return batched_out, None
            return per_node_out, None
        return respond

    def test_statuses_come_from_one_query_when_the_batch_succeeds(self):
        for i, cluster_id in enumerate((10, 11, 12)):
            self.workflow.add_node(self.resolved_node('n%d' % i, cluster_id=cluster_id))

        respond = self.batched_and_per_node('10;;;2+++11;;;1+++12;;;4+++')
        with mock.patch.object(HTCondorObjectBase, '_execute', side_effect=respond) as ex:
            counts = self.workflow._update_statuses()

        self.assertEqual(1, ex.call_count)
        self.assertEqual(1, counts['Running'])
        self.assertEqual(1, counts['Idle'])
        self.assertEqual(1, counts['Completed'])

    def test_statuses_fall_back_to_per_node_queries_when_the_batch_fails(self):
        for i, cluster_id in enumerate((10, 11)):
            self.workflow.add_node(self.resolved_node('n%d' % i, cluster_id=cluster_id))

        def respond(args, **kwargs):
            if 'DAGManJobID' in args[0]:
                return '', 'condor_q: command not found'
            return '2', None

        with mock.patch.object(HTCondorObjectBase, '_execute', side_effect=respond) as ex:
            counts = self.workflow._update_statuses()

        self.assertEqual(3, ex.call_count)
        self.assertEqual(2, counts['Running'])

    def test_unexpanded_node_is_counted_without_a_per_node_query(self):
        self.workflow.add_node(self.resolved_node('done', cluster_id=10))
        self.workflow.add_job(Job('not_yet'))

        respond = self.batched_and_per_node('10;;;2+++')
        with mock.patch.object(HTCondorObjectBase, '_execute', side_effect=respond) as ex:
            counts = self.workflow._update_statuses()

        self.assertEqual(2, ex.call_count)
        self.assertEqual(1, counts['Running'])
        self.assertEqual(1, counts['Unexpanded'])

    def test_resolved_node_absent_from_the_batch_falls_back_to_its_own_query(self):
        self.workflow.add_node(self.resolved_node('present', cluster_id=10))
        self.workflow.add_node(self.resolved_node('absent', cluster_id=11))

        respond = self.batched_and_per_node('10;;;2+++', per_node_out='4')
        with mock.patch.object(HTCondorObjectBase, '_execute', side_effect=respond) as ex:
            counts = self.workflow._update_statuses()

        self.assertEqual(2, ex.call_count)
        self.assertEqual(1, counts['Running'])
        self.assertEqual(1, counts['Completed'])

    def test_concurrent_add_node_does_not_break_resolution(self):
        for i in range(200):
            self.workflow.add_job(Job('node_%d' % i))
        errors = []
        stop = threading.Event()

        def add_nodes():
            for i in range(2000):
                if stop.is_set():
                    return
                try:
                    self.workflow.add_job(Job('concurrent_%d' % i))
                except Exception as e:
                    errors.append(e)
                    return

        adder = threading.Thread(target=add_nodes)
        switch_interval = sys.getswitchinterval()
        sys.setswitchinterval(1e-6)
        try:
            with mock.patch.object(Workflow, '_execute', return_value=('', None)):
                adder.start()
                try:
                    for _ in range(200):
                        self.workflow.update_node_ids()
                        self.workflow._has_unresolved_nodes()
                except Exception as e:
                    errors.append(e)
                stop.set()
                adder.join()
        finally:
            sys.setswitchinterval(switch_interval)

        self.assertEqual([], errors)


if __name__ == '__main__':
    unittest.main()
