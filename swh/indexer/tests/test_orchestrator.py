# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

import celery

from swh.indexer.orchestrator import BaseOrchestratorIndexer
from swh.indexer.indexer import BaseIndexer
from swh.indexer.tests.test_utils import MockIndexerStorage, MockStorage
from swh.scheduler.tests.scheduler_testing import SchedulerTestFixture


class BaseTestIndexer(BaseIndexer):
    ADDITIONAL_CONFIG = {
            'tools': ('dict', {
                'name': 'foo',
                'version': 'bar',
                'configuration': {}
                }),
            }

    def prepare(self):
        self.idx_storage = MockIndexerStorage()
        self.storage = MockStorage()

    def check(self):
        pass

    def filter(self, ids):
        self.filtered.append(ids)
        return ids

    def run(self, ids, policy_update):
        return self.index(ids)

    def index(self, ids):
        self.indexed.append(ids)
        return [id_ + '_indexed_by_' + self.__class__.__name__
                for id_ in ids]

    def persist_index_computations(self, result, policy_update):
        self.persisted = result


class Indexer1(BaseTestIndexer):
    filtered = []
    indexed = []

    def filter(self, ids):
        return super().filter([id_ for id_ in ids if '1' in id_])


class Indexer2(BaseTestIndexer):
    filtered = []
    indexed = []

    def filter(self, ids):
        return super().filter([id_ for id_ in ids if '2' in id_])


class Indexer3(BaseTestIndexer):
    filtered = []
    indexed = []

    def filter(self, ids):
        return super().filter([id_ for id_ in ids if '3' in id_])


@celery.task
def indexer1_task(*args, **kwargs):
    return Indexer1().run(*args, **kwargs)


@celery.task
def indexer2_task(*args, **kwargs):
    return Indexer2().run(*args, **kwargs)


@celery.task
def indexer3_task(self, *args, **kwargs):
    return Indexer3().run(*args, **kwargs)


class TestOrchestrator12(BaseOrchestratorIndexer):
    TASK_NAMES = {
            'indexer1': 'swh.indexer.tests.test_orchestrator.indexer1_task',
            'indexer2': 'swh.indexer.tests.test_orchestrator.indexer2_task',
            'indexer3': 'swh.indexer.tests.test_orchestrator.indexer3_task',
            }

    INDEXER_CLASSES = {
            'indexer1': 'swh.indexer.tests.test_orchestrator.Indexer1',
            'indexer2': 'swh.indexer.tests.test_orchestrator.Indexer2',
            'indexer3': 'swh.indexer.tests.test_orchestrator.Indexer3',
            }

    def __init__(self):
        super().__init__()
        self.running_tasks = []

    def parse_config_file(self):
        return {
            'scheduler': {
                'cls': 'remote',
                'args': {
                    'url': 'http://localhost:9999',
                },
            },
            'indexers': {
                'indexer1': {
                    'batch_size': 2,
                    'check_presence': True,
                },
                'indexer2': {
                    'batch_size': 2,
                    'check_presence': True,
                },
            }
        }


class MockedTestOrchestrator12(TestOrchestrator12):
    def __init__(self):
        super().__init__()
        self.created_tasks = []

    def _create_tasks(self, celery_tasks):
        self.created_tasks.extend(celery_tasks)

    def prepare_scheduler(self):
        pass


class OrchestratorTest(SchedulerTestFixture, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.add_scheduler_task_type(
            'indexer1',
            'swh.indexer.tests.test_orchestrator.indexer1_task')
        self.add_scheduler_task_type(
            'indexer2',
            'swh.indexer.tests.test_orchestrator.indexer2_task')

    def test_orchestrator_filter(self):
        o = TestOrchestrator12()
        o.scheduler = self.scheduler
        o.run(['id12', 'id2'])
        self.assertEqual(Indexer2.indexed, [])
        self.assertEqual(Indexer1.indexed, [])
        self.run_ready_tasks()
        self.assertEqual(Indexer2.indexed, [['id12', 'id2']])
        self.assertEqual(Indexer1.indexed, [['id12']])


class MockedOrchestratorTest(unittest.TestCase):
    maxDiff = None

    def test_mocked_orchestrator_filter(self):
        o = MockedTestOrchestrator12()
        o.run(['id12', 'id2'])
        for task in o.created_tasks:
            del task['next_run']  # not worth the trouble testing it properly
        self.assertCountEqual(o.created_tasks, [
            {'type': 'indexer1',
             'arguments': {
                 'args': [],
                 'kwargs': {
                     'ids': ['id12'],
                     'policy_update': 'ignore-dups'}},
             'policy': 'oneshot'},
            {'type': 'indexer2',
             'arguments': {
                 'args': [],
                 'kwargs': {
                     'ids': ['id12', 'id2'],
                     'policy_update': 'ignore-dups'}},
             'policy': 'oneshot'},
            ])

    def test_mocked_orchestrator_batch(self):
        o = MockedTestOrchestrator12()
        o.run(['id12', 'id2a', 'id2b', 'id2c'])
        for task in o.created_tasks:
            del task['next_run']  # not worth the trouble testing it properly
        self.assertCountEqual(o.created_tasks, [
            {'type': 'indexer1',
             'arguments': {
                 'args': [],
                 'kwargs': {
                     'ids': ['id12'],
                     'policy_update': 'ignore-dups'}},
             'policy': 'oneshot'},
            {'type': 'indexer2',
             'arguments': {
                 'args': [],
                 'kwargs': {
                     'ids': ['id12', 'id2a'],
                     'policy_update': 'ignore-dups'}},
             'policy': 'oneshot'},
            {'type': 'indexer2',
             'arguments': {
                 'args': [],
                 'kwargs': {
                     'ids': ['id2b', 'id2c'],
                     'policy_update': 'ignore-dups'}},
             'policy': 'oneshot'},
            ])
