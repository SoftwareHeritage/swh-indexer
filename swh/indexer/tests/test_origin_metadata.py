# Copyright (C) 2018-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from unittest.mock import patch

from swh.model.hashutil import hash_to_bytes

from swh.indexer.metadata import OriginMetadataIndexer

from .utils import YARN_PARSER_METADATA
from .test_metadata import REVISION_METADATA_CONFIG


def test_origin_metadata_indexer(
        idx_storage, storage, obj_storage):

    indexer = OriginMetadataIndexer(config=REVISION_METADATA_CONFIG)
    indexer.run(["https://github.com/librariesio/yarn-parser"])

    origin = storage.origin_get({
        'url': 'https://github.com/librariesio/yarn-parser'})
    rev_id = hash_to_bytes('8dbb6aeb036e7fd80664eb8bfd1507881af1ba9f')

    rev_metadata = {
        'id': rev_id,
        'metadata': YARN_PARSER_METADATA,
        'mappings': ['npm'],
    }
    origin_metadata = {
        'id': origin['id'],
        'origin_url': origin['url'],
        'from_revision': rev_id,
        'metadata': YARN_PARSER_METADATA,
        'mappings': ['npm'],
    }

    results = list(
        indexer.idx_storage.revision_intrinsic_metadata_get([rev_id]))
    for result in results:
        del result['tool']
    assert results == [rev_metadata]

    results = list(indexer.idx_storage.origin_intrinsic_metadata_get([
        origin['id']]))
    for result in results:
        del result['tool']
    assert results == [origin_metadata]


def test_origin_metadata_indexer_duplicate_origin(
        idx_storage, storage, obj_storage):
    indexer = OriginMetadataIndexer(config=REVISION_METADATA_CONFIG)
    indexer.storage = storage
    indexer.idx_storage = idx_storage
    indexer.run(["https://github.com/librariesio/yarn-parser"])

    indexer.run(["https://github.com/librariesio/yarn-parser"]*2)

    origin = storage.origin_get({
        'url': 'https://github.com/librariesio/yarn-parser'})
    rev_id = hash_to_bytes('8dbb6aeb036e7fd80664eb8bfd1507881af1ba9f')

    results = list(
        indexer.idx_storage.revision_intrinsic_metadata_get([rev_id]))
    assert len(results) == 1

    results = list(indexer.idx_storage.origin_intrinsic_metadata_get([
        origin['id']]))
    assert len(results) == 1


def test_origin_metadata_indexer_missing_head(
        idx_storage, storage, obj_storage):

    storage.origin_add([{
        'type': 'git',
        'url': 'https://example.com'
    }])

    indexer = OriginMetadataIndexer(config=REVISION_METADATA_CONFIG)
    indexer.run(["https://example.com"])

    origin = storage.origin_get({
        'url': 'https://example.com'})

    results = list(indexer.idx_storage.origin_intrinsic_metadata_get([
        origin['id']]))
    assert results == []


def test_origin_metadata_indexer_partial_missing_head(
        idx_storage, storage, obj_storage):

    storage.origin_add([{
        'type': 'git',
        'url': 'https://example.com'
    }])

    indexer = OriginMetadataIndexer(config=REVISION_METADATA_CONFIG)
    indexer.run(["https://example.com",
                 "https://github.com/librariesio/yarn-parser"])

    origin1 = storage.origin_get({
        'url': 'https://example.com'})
    origin2 = storage.origin_get({
        'url': 'https://github.com/librariesio/yarn-parser'})
    rev_id = hash_to_bytes('8dbb6aeb036e7fd80664eb8bfd1507881af1ba9f')

    rev_metadata = {
        'id': rev_id,
        'metadata': YARN_PARSER_METADATA,
        'mappings': ['npm'],
    }
    origin_metadata = {
        'id': origin2['id'],
        'origin_url': origin2['url'],
        'from_revision': rev_id,
        'metadata': YARN_PARSER_METADATA,
        'mappings': ['npm'],
    }

    results = list(
        indexer.idx_storage.revision_intrinsic_metadata_get([rev_id]))
    for result in results:
        del result['tool']
    assert results == [rev_metadata]

    results = list(indexer.idx_storage.origin_intrinsic_metadata_get([
        origin1['id'], origin2['id']]))
    for result in results:
        del result['tool']
    assert results == [origin_metadata]


def test_origin_metadata_indexer_duplicate_revision(
        idx_storage, storage, obj_storage):
    indexer = OriginMetadataIndexer(config=REVISION_METADATA_CONFIG)
    indexer.storage = storage
    indexer.idx_storage = idx_storage
    indexer.run(["https://github.com/librariesio/yarn-parser",
                 "https://github.com/librariesio/yarn-parser.git"])

    origin1 = storage.origin_get({
        'url': 'https://github.com/librariesio/yarn-parser'})
    origin2 = storage.origin_get({
        'url': 'https://github.com/librariesio/yarn-parser.git'})
    assert origin1['id'] != origin2['id']
    rev_id = hash_to_bytes('8dbb6aeb036e7fd80664eb8bfd1507881af1ba9f')

    results = list(
        indexer.idx_storage.revision_intrinsic_metadata_get([rev_id]))
    assert len(results) == 1

    results = list(indexer.idx_storage.origin_intrinsic_metadata_get([
        origin1['id'], origin2['id']]))
    assert len(results) == 2


def test_origin_metadata_indexer_no_metadata_file(
        idx_storage, storage, obj_storage):

    indexer = OriginMetadataIndexer(config=REVISION_METADATA_CONFIG)
    with patch('swh.indexer.metadata_dictionary.npm.NpmMapping.filename',
               b'foo.json'):
        indexer.run(["https://github.com/librariesio/yarn-parser"])

    origin = storage.origin_get({
        'url': 'https://github.com/librariesio/yarn-parser'})
    rev_id = hash_to_bytes('8dbb6aeb036e7fd80664eb8bfd1507881af1ba9f')

    results = list(
        indexer.idx_storage.revision_intrinsic_metadata_get([rev_id]))
    assert results == []

    results = list(indexer.idx_storage.origin_intrinsic_metadata_get([
        origin['id']]))
    assert results == []


def test_origin_metadata_indexer_no_metadata(
        idx_storage, storage, obj_storage):

    indexer = OriginMetadataIndexer(config=REVISION_METADATA_CONFIG)
    with patch('swh.indexer.metadata.RevisionMetadataIndexer'
               '.translate_revision_intrinsic_metadata',
               return_value=(['npm'], {'@context': 'foo'})):
        indexer.run(["https://github.com/librariesio/yarn-parser"])

    origin = storage.origin_get({
        'url': 'https://github.com/librariesio/yarn-parser'})
    rev_id = hash_to_bytes('8dbb6aeb036e7fd80664eb8bfd1507881af1ba9f')

    results = list(
        indexer.idx_storage.revision_intrinsic_metadata_get([rev_id]))
    assert results == []

    results = list(indexer.idx_storage.origin_intrinsic_metadata_get([
        origin['id']]))
    assert results == []


def test_origin_metadata_indexer_error(
        idx_storage, storage, obj_storage):

    indexer = OriginMetadataIndexer(config=REVISION_METADATA_CONFIG)
    with patch('swh.indexer.metadata.RevisionMetadataIndexer'
               '.translate_revision_intrinsic_metadata',
               return_value=None):
        indexer.run(["https://github.com/librariesio/yarn-parser"])

    origin = storage.origin_get({
        'url': 'https://github.com/librariesio/yarn-parser'})
    rev_id = hash_to_bytes('8dbb6aeb036e7fd80664eb8bfd1507881af1ba9f')

    results = list(
        indexer.idx_storage.revision_intrinsic_metadata_get([rev_id]))
    assert results == []

    results = list(indexer.idx_storage.origin_intrinsic_metadata_get([
        origin['id']]))
    assert results == []


def test_origin_metadata_indexer_delete_metadata(
        idx_storage, storage, obj_storage):

    indexer = OriginMetadataIndexer(config=REVISION_METADATA_CONFIG)
    indexer.run(["https://github.com/librariesio/yarn-parser"])

    origin = storage.origin_get({
        'url': 'https://github.com/librariesio/yarn-parser'})
    rev_id = hash_to_bytes('8dbb6aeb036e7fd80664eb8bfd1507881af1ba9f')

    results = list(
        indexer.idx_storage.revision_intrinsic_metadata_get([rev_id]))
    assert results != []

    results = list(indexer.idx_storage.origin_intrinsic_metadata_get([
        origin['id']]))
    assert results != []

    with patch('swh.indexer.metadata_dictionary.npm.NpmMapping.filename',
               b'foo.json'):
        indexer.run(["https://github.com/librariesio/yarn-parser"])

    results = list(
        indexer.idx_storage.revision_intrinsic_metadata_get([rev_id]))
    assert results == []

    results = list(indexer.idx_storage.origin_intrinsic_metadata_get([
        origin['id']]))
    assert results == []
