'''
Tests for private key caching, remote client reuse, and node id resolution.
'''
import os
import shutil
import tempfile
import unittest
from unittest import mock

import paramiko

from condorpy import Job, Workflow
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

    def test_node_set_resolves_ids_once(self):
        with mock.patch.object(Workflow, 'update_node_ids') as update_node_ids:
            self.workflow._node_ids_resolved = True
            self.workflow.node_set
            self.workflow.node_set
            update_node_ids.assert_not_called()

    def test_node_set_resolves_ids_when_unresolved(self):
        with mock.patch.object(Workflow, 'update_node_ids') as update_node_ids:
            self.workflow.node_set
            update_node_ids.assert_called_once()

    def test_adding_node_requires_resolution(self):
        self.workflow._node_ids_resolved = True
        self.workflow.add_job(Job('new_node'))
        self.assertFalse(self.workflow._node_ids_resolved)


if __name__ == '__main__':
    unittest.main()
