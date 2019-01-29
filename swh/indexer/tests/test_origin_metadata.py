# Copyright (C) 2018-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from unittest import mock

from swh.objstorage.objstorage_in_memory import InMemoryObjStorage
from swh.model.hashutil import hash_to_bytes
from swh.storage.in_memory import Storage

from swh.indexer.metadata import OriginMetadataIndexer
from swh.indexer.storage.in_memory import IndexerStorage

from .utils import fill_storage, fill_obj_storage, BASE_TEST_CONFIG
from .test_metadata import REVISION_METADATA_CONFIG


ORIGIN_HEAD_CONFIG = {
    **BASE_TEST_CONFIG,
    'tools': {
        'name': 'origin-metadata',
        'version': '0.0.1',
        'configuration': {},
    },
    'tasks': {
        'revision_metadata': 'revision_metadata',
        'origin_intrinsic_metadata': 'origin_intrinsic_metadata',
    }
}


@pytest.mark.db
@mock.patch('swh.indexer.metadata.RevisionMetadataIndexer.parse_config_file')
@mock.patch('swh.indexer.origin_head.OriginHeadIndexer.parse_config_file')
@mock.patch('swh.indexer.storage.in_memory.IndexerStorage')
@mock.patch('swh.storage.in_memory.Storage')
def test_full_origin_metadata_indexer(
        storage_mock, idx_storage_mock, origin_head_parse_config,
        revision_metadata_parse_config):
    # Always returns the same instance of the idx storage, because
    # this function is called by each of the three indexers.
    objstorage = InMemoryObjStorage()
    storage = Storage()
    idx_storage = IndexerStorage()

    origin_head_parse_config.return_value = ORIGIN_HEAD_CONFIG
    revision_metadata_parse_config.return_value = REVISION_METADATA_CONFIG
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
        indexer = OriginMetadataIndexer()
        indexer.storage = storage
        indexer.idx_storage = idx_storage
        indexer.run(["git+https://github.com/librariesio/yarn-parser"])
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
        'mappings': ['npm'],
    }
    origin_metadata = {
        'origin_id': origin['id'],
        'from_revision': rev_id,
        'metadata': metadata,
        'mappings': ['npm'],
    }

    results = list(indexer.idx_storage.revision_metadata_get([rev_id]))
    for result in results:
        del result['tool']
    assert results == [rev_metadata]

    results = list(indexer.idx_storage.origin_intrinsic_metadata_get([
        origin['id']]))
    for result in results:
        del result['tool']
    assert results == [origin_metadata]
