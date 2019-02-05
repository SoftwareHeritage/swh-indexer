# Copyright (C) 2018-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from unittest.mock import patch

from swh.model.hashutil import hash_to_bytes

from swh.indexer.metadata import OriginMetadataIndexer

from .utils import BASE_TEST_CONFIG, YARN_PARSER_METADATA
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


@pytest.fixture
def origin_metadata_indexer():
    prefix = 'swh.indexer.'
    suffix = '.parse_config_file'
    with patch(prefix + 'metadata.OriginMetadataIndexer' + suffix) as omi, \
            patch(prefix + 'origin_head.OriginHeadIndexer' + suffix) as ohi, \
            patch(prefix + 'metadata.RevisionMetadataIndexer' + suffix) as rmi:
        omi.return_value = BASE_TEST_CONFIG
        ohi.return_value = ORIGIN_HEAD_CONFIG
        rmi.return_value = REVISION_METADATA_CONFIG
        yield OriginMetadataIndexer()


def test_origin_metadata_indexer(
        idx_storage, storage, obj_storage, origin_metadata_indexer):

    indexer = OriginMetadataIndexer()
    indexer.run(["git+https://github.com/librariesio/yarn-parser"])

    origin = storage.origin_get({
        'type': 'git',
        'url': 'https://github.com/librariesio/yarn-parser'})
    rev_id = hash_to_bytes('8dbb6aeb036e7fd80664eb8bfd1507881af1ba9f')

    rev_metadata = {
        'id': rev_id,
        'translated_metadata': YARN_PARSER_METADATA,
        'mappings': ['npm'],
    }
    origin_metadata = {
        'origin_id': origin['id'],
        'from_revision': rev_id,
        'metadata': YARN_PARSER_METADATA,
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


def test_origin_metadata_indexer_duplicates(
        idx_storage, storage, obj_storage, origin_metadata_indexer):
    indexer = OriginMetadataIndexer()
    indexer.storage = storage
    indexer.idx_storage = idx_storage
    indexer.run(["git+https://github.com/librariesio/yarn-parser"])

    indexer.run(["git+https://github.com/librariesio/yarn-parser"]*2)

    origin = storage.origin_get({
        'type': 'git',
        'url': 'https://github.com/librariesio/yarn-parser'})
    rev_id = hash_to_bytes('8dbb6aeb036e7fd80664eb8bfd1507881af1ba9f')

    results = list(indexer.idx_storage.revision_metadata_get([rev_id]))
    assert len(results) == 1

    results = list(indexer.idx_storage.origin_intrinsic_metadata_get([
        origin['id']]))
    assert len(results) == 1
