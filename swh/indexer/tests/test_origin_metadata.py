# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import unittest
from celery import task

from swh.indexer.metadata import OriginMetadataIndexer
from swh.indexer.tests.test_utils import MockObjStorage, MockStorage
from swh.indexer.tests.test_utils import MockIndexerStorage
from swh.indexer.tests.test_origin_head import TestOriginHeadIndexer
from swh.indexer.tests.test_metadata import TestRevisionMetadataIndexer

from swh.scheduler.tests.celery_testing import CeleryTestFixture
from swh.indexer.tests import start_worker_thread


class TestOriginMetadataIndexer(OriginMetadataIndexer):
    def prepare(self):
        self.config = {
            'storage': {
                'cls': 'remote',
                'args': {
                    'url': 'http://localhost:9999',
                }
            },
            'tools': {
                'name': 'origin-metadata',
                'version': '0.0.1',
                'configuration': {}
            }
        }
        self.storage = MockStorage()
        self.idx_storage = MockIndexerStorage()
        self.log = logging.getLogger('swh.indexer')
        self.objstorage = MockObjStorage()
        self.destination_task = None
        self.tools = self.register_tools(self.config['tools'])
        self.tool = self.tools[0]
        self.results = []


@task
def revision_metadata_test_task(*args, **kwargs):
    indexer = TestRevisionMetadataIndexer()
    indexer.run(*args, **kwargs)
    return indexer.results


@task
def origin_intrinsic_metadata_test_task(*args, **kwargs):
    indexer = TestOriginMetadataIndexer()
    indexer.run(*args, **kwargs)
    return indexer.results


class TestOriginHeadIndexer(TestOriginHeadIndexer):
    revision_metadata_task = revision_metadata_test_task
    origin_intrinsic_metadata_task = origin_intrinsic_metadata_test_task


class TestOriginMetadata(CeleryTestFixture, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.maxDiff = None
        MockIndexerStorage.added_data = []

    def test_pipeline(self):
        indexer = TestOriginHeadIndexer()
        with start_worker_thread():
            promise = indexer.run(
                    ["git+https://github.com/librariesio/yarn-parser"],
                    policy_update='update-dups',
                    parse_ids=True)
            promise.get()

        metadata = {
            'identifier': None,
            'maintainer': None,
            'url': [
                'https://github.com/librariesio/yarn-parser#readme'
            ],
            'codeRepository': [{
                'type': 'git',
                'url': 'git+https://github.com/librariesio/yarn-parser.git'
            }],
            'author': ['Andrew Nesbitt'],
            'license': ['AGPL-3.0'],
            'version': ['1.0.0'],
            'description': [
                'Tiny web service for parsing yarn.lock files'
            ],
            'relatedLink': None,
            'developmentStatus': None,
            'operatingSystem': None,
            'issueTracker': [{
                'url': 'https://github.com/librariesio/yarn-parser/issues'
            }],
            'softwareRequirements': [{
                'express': '^4.14.0',
                'yarn': '^0.21.0',
                'body-parser': '^1.15.2'
            }],
            'name': ['yarn-parser'],
            'keywords': [['yarn', 'parse', 'lock', 'dependencies']],
            'email': None
        }
        rev_metadata = {
            'id': b'8dbb6aeb036e7fd80664eb8bfd1507881af1ba9f',
            'translated_metadata': metadata,
            'indexer_configuration_id': 7,
        }
        origin_metadata = {
            'origin_id': 54974445,
            'from_revision': b'8dbb6aeb036e7fd80664eb8bfd1507881af1ba9f',
            'metadata': metadata,
            'indexer_configuration_id': 7,
        }
        expected_results = [
                ('origin_intrinsic_metadata', True, [origin_metadata]),
                ('revision_metadata', True, [rev_metadata])]

        results = list(indexer.idx_storage.added_data)
        self.assertCountEqual(expected_results, results)
