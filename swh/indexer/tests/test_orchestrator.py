# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from swh.indexer.orchestrator import BaseOrchestratorIndexer
from swh.indexer.indexer import RevisionIndexer
from swh.indexer.tests.test_utils import MockIndexerStorage
from swh.scheduler.task import Task


class BaseTestIndexer(RevisionIndexer):
    ADDITIONAL_CONFIG = {
            'tools': ('dict', {
                'name': 'foo',
                'version': 'bar',
                'configuration': {}
                }),
            }

    def prepare(self):
        self.idx_storage = MockIndexerStorage()

    def check(self):
        pass

    def filter(self, ids):
        self.filtered = ids
        return ids

    def index(self, ids):
        self.indexed = ids
        return [id_ + '_indexed_by_' + self.__class__.__name__
                for id_ in ids]

    def persist_index_computations(self, result, policy_update):
        self.persisted = result


class Indexer1(BaseTestIndexer):
    def filter(self, ids):
        return super().filter([id_ for id_ in ids if '1' in id_])


class Indexer2(BaseTestIndexer):
    def filter(self, ids):
        return super().filter([id_ for id_ in ids if '2' in id_])


class Indexer3(BaseTestIndexer):
    def filter(self, ids):
        return super().filter([id_ for id_ in ids if '3' in id_])


class Indexer1Task(Task):
    pass


class Indexer2Task(Task):
    pass


class Indexer3Task(Task):
    pass


class TestOrchestrator12(BaseOrchestratorIndexer):
    TASK_NAMES = {
            'indexer1': 'swh.indexer.tests.test_orchestrator.Indexer1Task',
            'indexer2': 'swh.indexer.tests.test_orchestrator.Indexer2Task',
            'indexer3': 'swh.indexer.tests.test_orchestrator.Indexer3Task',
            }

    INDEXER_CLASSES = {
            'indexer1': 'swh.indexer.tests.test_orchestrator.Indexer1',
            'indexer2': 'swh.indexer.tests.test_orchestrator.Indexer2',
            'indexer3': 'swh.indexer.tests.test_orchestrator.Indexer3',
            }

    def __init__(self):
        super().__init__()
        self.running_tasks = []

    def prepare(self):
        self.config = {
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
        self.prepare_tasks()

    def _run_tasks(self, celery_tasks):
        self.running_tasks.extend(celery_tasks)


class OrchestratorTest(unittest.TestCase):
    maxDiff = None

    def test_orchestrator_filter(self):
        o = TestOrchestrator12()
        o.prepare()
        o.run(['id12', 'id2'])
        self.assertCountEqual(o.running_tasks, [
                  {'args': (),
                   'chord_size': None,
                   'immutable': False,
                   'kwargs': {'ids': ['id12'],
                              'policy_update': 'ignore-dups'},
                   'options': {},
                   'subtask_type': None,
                   'task': 'swh.indexer.tests.test_orchestrator.Indexer1Task'},
                  {'args': (),
                   'chord_size': None,
                   'immutable': False,
                   'kwargs': {'ids': ['id12', 'id2'],
                              'policy_update': 'ignore-dups'},
                   'options': {},
                   'subtask_type': None,
                   'task': 'swh.indexer.tests.test_orchestrator.Indexer2Task'},
                ])

    def test_orchestrator_batch(self):
        o = TestOrchestrator12()
        o.prepare()
        o.run(['id12', 'id2a', 'id2b', 'id2c'])
        self.assertCountEqual(o.running_tasks, [
                  {'args': (),
                   'chord_size': None,
                   'immutable': False,
                   'kwargs': {'ids': ['id12'],
                              'policy_update': 'ignore-dups'},
                   'options': {},
                   'subtask_type': None,
                   'task': 'swh.indexer.tests.test_orchestrator.Indexer1Task'},
                  {'args': (),
                   'chord_size': None,
                   'immutable': False,
                   'kwargs': {'ids': ['id12', 'id2a'],
                              'policy_update': 'ignore-dups'},
                   'options': {},
                   'subtask_type': None,
                   'task': 'swh.indexer.tests.test_orchestrator.Indexer2Task'},
                  {'args': (),
                   'chord_size': None,
                   'immutable': False,
                   'kwargs': {'ids': ['id2b', 'id2c'],
                              'policy_update': 'ignore-dups'},
                   'options': {},
                   'subtask_type': None,
                   'task': 'swh.indexer.tests.test_orchestrator.Indexer2Task'},
                ])
