# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import time
import logging
import unittest
from celery import task

from swh.indexer.metadata import OriginMetadataIndexer
from swh.indexer.tests.test_utils import MockObjStorage, MockStorage
from swh.indexer.tests.test_utils import MockIndexerStorage
from swh.indexer.tests.test_origin_head import OriginHeadTestIndexer
from swh.indexer.tests.test_metadata import RevisionMetadataTestIndexer

from swh.scheduler.tests.scheduler_testing import SchedulerTestFixture

from swh.model.hashutil import hash_to_bytes


class OriginMetadataTestIndexer(OriginMetadataIndexer):
    def prepare(self):
        self.config = {
            'storage': {
                'cls': 'remote',
                'args': {
                    'url': 'http://localhost:9999',
                }
            },
            'tools': [],
        }
        self.storage = MockStorage()
        self.idx_storage = MockIndexerStorage()
        self.log = logging.getLogger('swh.indexer')
        self.objstorage = MockObjStorage()
        self.tools = self.register_tools(self.config['tools'])
        self.results = []


@task
def revision_metadata_test_task(*args, **kwargs):
    indexer = RevisionMetadataTestIndexer()
    indexer.run(*args, **kwargs)
    return indexer.results


@task
def origin_intrinsic_metadata_test_task(*args, **kwargs):
    indexer = OriginMetadataTestIndexer()
    indexer.run(*args, **kwargs)
    return indexer.results


class OriginHeadTestIndexer(OriginHeadTestIndexer):
    def prepare(self):
        super().prepare()
        self.config['tasks'] = {
            'revision_metadata': 'revision_metadata_test_task',
            'origin_intrinsic_metadata': 'origin_intrinsic_metadata_test_task',
        }


class TestOriginMetadata(SchedulerTestFixture, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.maxDiff = None
        # FIXME: Improve mock indexer storage reset behavior
        MockIndexerStorage.added_data = []
        MockIndexerStorage.revision_metadata = {}
        self.add_scheduler_task_type(
            'revision_metadata_test_task',
            'swh.indexer.tests.test_origin_metadata.'
            'revision_metadata_test_task')
        self.add_scheduler_task_type(
            'origin_intrinsic_metadata_test_task',
            'swh.indexer.tests.test_origin_metadata.'
            'origin_intrinsic_metadata_test_task')
        RevisionMetadataTestIndexer.scheduler = self.scheduler

    def tearDown(self):
        del RevisionMetadataTestIndexer.scheduler
        super().tearDown()

    def test_pipeline(self):
        indexer = OriginHeadTestIndexer()
        indexer.scheduler = self.scheduler
        indexer.run(["git+https://github.com/librariesio/yarn-parser"])

        self.run_ready_tasks()  # Run the first task
        time.sleep(0.1)  # Give it time to complete and schedule the 2nd one
        self.run_ready_tasks()  # Run the second task

        metadata = {
            '@context': 'https://doi.org/10.5063/schema/codemeta-2.0',
            'url':
                'https://github.com/librariesio/yarn-parser#readme',
            'schema:codeRepository':
                'git+https://github.com/librariesio/yarn-parser.git',
            'schema:author': 'Andrew Nesbitt',
            'license': 'AGPL-3.0',
            'version': '1.0.0',
            'description':
                'Tiny web service for parsing yarn.lock files',
            'codemeta:issueTracker':
                'https://github.com/librariesio/yarn-parser/issues',
            'name': 'yarn-parser',
            'keywords': ['yarn', 'parse', 'lock', 'dependencies'],
        }
        rev_metadata = {
            'id': hash_to_bytes('8dbb6aeb036e7fd80664eb8bfd1507881af1ba9f'),
            'translated_metadata': metadata,
            'indexer_configuration_id': 7,
        }
        origin_metadata = {
            'origin_id': 54974445,
            'from_revision': hash_to_bytes(
                '8dbb6aeb036e7fd80664eb8bfd1507881af1ba9f'),
            'metadata': metadata,
            'indexer_configuration_id': 7,
        }
        expected_results = [
                ('origin_intrinsic_metadata', True, [origin_metadata]),
                ('revision_metadata', True, [rev_metadata])]

        results = list(indexer.idx_storage.added_data)
        self.assertCountEqual(expected_results, results)
