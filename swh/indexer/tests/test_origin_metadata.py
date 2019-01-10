# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import time
import unittest

from celery import task
from swh.model.hashutil import hash_to_bytes
from swh.storage.in_memory import Storage

from swh.indexer.metadata import (
    OriginMetadataIndexer, RevisionMetadataIndexer
)

from swh.indexer.storage.in_memory import IndexerStorage
from swh.objstorage.objstorage_in_memory import InMemoryObjStorage

from swh.scheduler.tests.scheduler_testing import SchedulerTestFixture
from .test_utils import (
    BASE_TEST_CONFIG, fill_storage, fill_obj_storage
)
from .test_origin_head import OriginHeadTestIndexer
from .test_metadata import ContentMetadataTestIndexer


class RevisionMetadataTestIndexer(RevisionMetadataIndexer):
    """Specific indexer whose configuration is enough to satisfy the
       indexing tests.
    """
    ContentMetadataIndexer = ContentMetadataTestIndexer

    def parse_config_file(self, *args, **kwargs):
        return {
            **BASE_TEST_CONFIG,
            'tools': {
                'name': 'swh-metadata-detector',
                'version': '0.0.2',
                'configuration': {
                    'type': 'local',
                    'context': 'NpmMapping'
                }
            }
        }


@task
def revision_metadata_test_task(*args, **kwargs):
    indexer = RevisionMetadataTestIndexer()
    indexer.run(*args, **kwargs)
    return indexer.results


class OriginMetadataTestIndexer(OriginMetadataIndexer):
    def parse_config_file(self, *args, **kwargs):
        return {
            **BASE_TEST_CONFIG,
            'tools': []
        }


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

    @unittest.mock.patch('swh.indexer.storage.in_memory.IndexerStorage')
    @unittest.mock.patch('swh.storage.in_memory.Storage')
    def test_pipeline(self, storage_mock, idx_storage_mock):
        # Always returns the same instance of the idx storage, because
        # this function is called by each of the three indexers.
        objstorage = InMemoryObjStorage()
        storage = Storage()
        idx_storage = IndexerStorage()

        storage_mock.return_value = storage
        idx_storage_mock.return_value = idx_storage

        fill_obj_storage(objstorage)
        fill_storage(storage)

        # TODO: find a better way to share the ContentMetadataIndexer use
        # the same objstorage instance.
        import swh.objstorage
        old_inmem_objstorage = swh.objstorage._STORAGE_CLASSES['memory']
        swh.objstorage._STORAGE_CLASSES['memory'] = lambda: objstorage
        try:
            indexer = OriginHeadTestIndexer()
            indexer.scheduler = self.scheduler
            indexer.run(["git+https://github.com/librariesio/yarn-parser"])

            self.run_ready_tasks()  # Run the first task
            # Give it time to complete and schedule the 2nd one
            time.sleep(0.1)
            self.run_ready_tasks()  # Run the second task
        finally:
            swh.objstorage._STORAGE_CLASSES['memory'] = old_inmem_objstorage

        origin = storage.origin_get({
            'type': 'git',
            'url': 'https://github.com/librariesio/yarn-parser'})
        rev_id = hash_to_bytes('8dbb6aeb036e7fd80664eb8bfd1507881af1ba9f')

        metadata = {
            '@context': 'https://doi.org/10.5063/schema/codemeta-2.0',
            'url':
                'https://github.com/librariesio/yarn-parser#readme',
            'codeRepository':
                'git+git+https://github.com/librariesio/yarn-parser.git',
            'author': [{
                'type': 'Person',
                'name': 'Andrew Nesbitt'
            }],
            'license': 'https://spdx.org/licenses/AGPL-3.0',
            'version': '1.0.0',
            'description':
                'Tiny web service for parsing yarn.lock files',
            'issueTracker':
                'https://github.com/librariesio/yarn-parser/issues',
            'name': 'yarn-parser',
            'keywords': ['yarn', 'parse', 'lock', 'dependencies'],
        }
        rev_metadata = {
            'id': rev_id,
            'translated_metadata': metadata,
        }
        origin_metadata = {
            'origin_id': origin['id'],
            'from_revision': rev_id,
            'metadata': metadata,
        }

        results = list(indexer.idx_storage.revision_metadata_get([rev_id]))
        for result in results:
            del result['tool']
        self.assertEqual(results, [rev_metadata])

        results = list(indexer.idx_storage.origin_intrinsic_metadata_get([
            origin['id']]))
        for result in results:
            del result['tool']
        self.assertEqual(results, [origin_metadata])
