# Copyright (C) 2017-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest
import logging

from swh.indexer.mimetype import (
    ContentMimetypeIndexer, MimetypeRangeIndexer
)

from swh.indexer.tests.test_utils import MockObjStorage
from swh.model import hashutil


class _MockStorage():
    """In memory implementation to fake the content_get_range api.

    FIXME: To remove when the actual in-memory lands.

    """
    contents = []

    def __init__(self, contents):
        self.contents = contents

    def content_get_range(self, start, end, limit=1000):
        # to make input test data consilient with actual runtime the
        # other way of doing properly things would be to rewrite all
        # tests (that's another task entirely so not right now)
        if isinstance(start, bytes):
            start = hashutil.hash_to_hex(start)
        if isinstance(end, bytes):
            end = hashutil.hash_to_hex(end)
        results = []
        _next_id = None
        counter = 0
        for c in self.contents:
            _id = c['sha1']
            if start <= _id and _id <= end:
                results.append(c)
            if counter >= limit:
                break
            counter += 1

        return {
            'contents': results,
            'next': _next_id
        }


class _MockIndexerStorage():
    """Mock storage to simplify reading indexers' outputs.

    """
    state = []

    def content_mimetype_add(self, mimetypes, conflict_update=None):
        self.state = mimetypes
        self.conflict_update = conflict_update

    def content_mimetype_get_range(self, start, end, indexer_configuration_id,
                                   limit=1000):
        """Basic in-memory implementation (limit is unused).

        """
        # to make input test data consilient with actual runtime the
        # other way of doing properly things would be to rewrite all
        # tests (that's another task entirely so not right now)
        if isinstance(start, bytes):
            start = hashutil.hash_to_hex(start)
        if isinstance(end, bytes):
            end = hashutil.hash_to_hex(end)
        results = []
        _next = None
        counter = 0
        for m in self.state:
            _id = m['id']
            _tool_id = m['indexer_configuration_id']
            if (start <= _id and _id <= end and
               _tool_id == indexer_configuration_id):
                results.append(_id)
            if counter >= limit:
                break
            counter += 1

        return {
            'ids': results,
            'next': _next
        }

    def indexer_configuration_add(self, tools):
        return [{
            'id': 10,
        }]


class MimetypeTestIndexer(ContentMimetypeIndexer):
    """Specific mimetype whose configuration is enough to satisfy the
       indexing tests.

    """
    def prepare(self):
        self.config = {
            'tools': {
                'name': 'file',
                'version': '1:5.30-1+deb9u1',
                'configuration': {
                    "type": "library",
                    "debian-package": "python3-magic"
                },
            },
        }
        self.idx_storage = _MockIndexerStorage()
        self.log = logging.getLogger('swh.indexer')
        self.objstorage = MockObjStorage()
        self.tools = self.register_tools(self.config['tools'])
        self.tool = self.tools[0]


class MimetypeIndexerUnknownToolTestStorage(MimetypeTestIndexer):
    """Specific mimetype whose configuration is not enough to satisfy the
       indexing tests.

    """
    def prepare(self):
        super().prepare()
        self.tools = None


class TestMimetypeIndexerWithErrors(unittest.TestCase):
    def test_wrong_unknown_configuration_tool(self):
        """Indexer with unknown configuration tool should fail the check"""
        with self.assertRaisesRegex(ValueError, 'Tools None is unknown'):
            MimetypeIndexerUnknownToolTestStorage()


class TestMimetypeIndexer(unittest.TestCase):
    def setUp(self):
        self.indexer = MimetypeTestIndexer()

    def test_index_no_update(self):
        # given
        sha1s = [
            '01c9379dfc33803963d07c1ccc748d3fe4c96bb5',
            '688a5ef812c53907562fe379d4b3851e69c7cb15',
        ]

        # when
        self.indexer.run(sha1s, policy_update='ignore-dups')

        # then
        expected_results = [{
            'id': '01c9379dfc33803963d07c1ccc748d3fe4c96bb5',
            'indexer_configuration_id': 10,
            'mimetype': b'text/plain',
            'encoding': b'us-ascii',
        }, {
            'id': '688a5ef812c53907562fe379d4b3851e69c7cb15',
            'indexer_configuration_id': 10,
            'mimetype': b'text/plain',
            'encoding': b'us-ascii',
        }]

        self.assertFalse(self.indexer.idx_storage.conflict_update)
        self.assertEqual(expected_results, self.indexer.idx_storage.state)

    def test_index_update(self):
        # given
        sha1s = [
            '01c9379dfc33803963d07c1ccc748d3fe4c96bb5',
            '688a5ef812c53907562fe379d4b3851e69c7cb15',
            'da39a3ee5e6b4b0d3255bfef95601890afd80709',  # empty content
        ]

        # when
        self.indexer.run(sha1s, policy_update='update-dups')

        # then
        expected_results = [{
            'id': '01c9379dfc33803963d07c1ccc748d3fe4c96bb5',
            'indexer_configuration_id': 10,
            'mimetype': b'text/plain',
            'encoding': b'us-ascii',
        }, {
            'id': '688a5ef812c53907562fe379d4b3851e69c7cb15',
            'indexer_configuration_id': 10,
            'mimetype': b'text/plain',
            'encoding': b'us-ascii',
        }, {
            'id': 'da39a3ee5e6b4b0d3255bfef95601890afd80709',
            'indexer_configuration_id': 10,
            'mimetype': b'application/x-empty',
            'encoding': b'binary',
        }]

        self.assertTrue(self.indexer.idx_storage.conflict_update)
        self.assertEqual(expected_results, self.indexer.idx_storage.state)

    def test_index_one_unknown_sha1(self):
        # given
        sha1s = ['688a5ef812c53907562fe379d4b3851e69c7cb15',
                 '799a5ef812c53907562fe379d4b3851e69c7cb15',  # unknown
                 '800a5ef812c53907562fe379d4b3851e69c7cb15']  # unknown

        # when
        self.indexer.run(sha1s, policy_update='update-dups')

        # then
        expected_results = [{
            'id': '688a5ef812c53907562fe379d4b3851e69c7cb15',
            'indexer_configuration_id': 10,
            'mimetype': b'text/plain',
            'encoding': b'us-ascii',
        }]

        self.assertTrue(self.indexer.idx_storage.conflict_update)
        self.assertEqual(expected_results, self.indexer.idx_storage.state)


class MimetypeRangeIndexerTest(MimetypeRangeIndexer):
    """Specific mimetype whose configuration is enough to satisfy the
       indexing tests.

    """
    def prepare(self):
        self.config = {
            'tools': {
                'name': 'file',
                'version': '1:5.30-1+deb9u1',
                'configuration': {
                    "type": "library",
                    "debian-package": "python3-magic"
                },
            },
            'write_batch_size': 100,
        }
        self.idx_storage = _MockIndexerStorage()
        self.log = logging.getLogger('swh.indexer')
        # this hardcodes some contents, will use this to setup the storage
        self.objstorage = MockObjStorage()
        # sync objstorage and storage
        contents = [{'sha1': c_id} for c_id in self.objstorage]
        self.storage = _MockStorage(contents)
        self.tools = self.register_tools(self.config['tools'])
        self.tool = self.tools[0]


class TestMimetypeRangeIndexer(unittest.TestCase):
    def setUp(self):
        self.indexer = MimetypeRangeIndexerTest()
        # will play along with the objstorage's mocked contents for now
        self.contents = sorted(self.indexer.objstorage)
        # FIXME: leverage swh.objstorage.in_memory_storage's
        # InMemoryObjStorage, swh.storage.tests's gen_contents, and
        # hypothesis to generate data to actually run indexer on those

        self.expected_results = {
            '01c9379dfc33803963d07c1ccc748d3fe4c96bb5': {
                'encoding': b'us-ascii',
                'id': '01c9379dfc33803963d07c1ccc748d3fe4c96bb5',
                'indexer_configuration_id': 10,
                'mimetype': b'text/plain'},
            '02fb2c89e14f7fab46701478c83779c7beb7b069': {
                'encoding': b'us-ascii',
                'id': '02fb2c89e14f7fab46701478c83779c7beb7b069',
                'indexer_configuration_id': 10,
                'mimetype': b'text/x-python'},
            '103bc087db1d26afc3a0283f38663d081e9b01e6': {
                'encoding': b'us-ascii',
                'id': '103bc087db1d26afc3a0283f38663d081e9b01e6',
                'indexer_configuration_id': 10,
                'mimetype': b'text/plain'}
        }

    def assert_mimetypes_ok(self, start, end, actual_results,
                            expected_results=None):
        if expected_results is None:
            expected_results = self.expected_results

        for mimetype in actual_results:
            _id = mimetype['id']
            self.assertEqual(mimetype, expected_results[_id])
            self.assertTrue(start <= _id and _id <= end)
            _tool_id = mimetype['indexer_configuration_id']
            self.assertEqual(_tool_id, self.indexer.tool['id'])

    def test__index_contents(self):
        """Indexing contents without existing data results in indexed data

        """
        start, end = [self.contents[0], self.contents[2]]  # output hex ids
        # given
        actual_results = list(self.indexer._index_contents(
            start, end, indexed={}))

        self.assert_mimetypes_ok(start, end, actual_results)

    def test__index_contents_with_indexed_data(self):
        """Indexing contents with existing data results in less indexed data

        """
        start, end = [self.contents[0], self.contents[2]]  # output hex ids
        data_indexed = [
            '01c9379dfc33803963d07c1ccc748d3fe4c96bb5',
            '103bc087db1d26afc3a0283f38663d081e9b01e6'
        ]

        # given
        actual_results = self.indexer._index_contents(
            start, end, indexed=set(data_indexed))

        # craft the expected results
        expected_results = self.expected_results.copy()
        for already_indexed_key in data_indexed:
            expected_results.pop(already_indexed_key)

        self.assert_mimetypes_ok(
            start, end, actual_results, expected_results)

    def test_generate_content_mimetype_get(self):
        """Optimal indexing should result in indexed data

        """
        start, end = [self.contents[0], self.contents[2]]  # output hex ids
        # given
        actual_results = self.indexer.run(start, end)

        # then
        self.assertTrue(actual_results)

    def test_generate_content_mimetype_get_input_as_bytes(self):
        """Optimal indexing should result in indexed data

        Input are in bytes here.

        """
        _start, _end = [self.contents[0], self.contents[2]]  # output hex ids
        start, end = map(hashutil.hash_to_bytes, (_start, _end))

        # given
        actual_results = self.indexer.run(  # checks the bytes input this time
            start, end, skip_existing=False)  # no data so same result

        # then
        self.assertTrue(actual_results)

    def test_generate_content_mimetype_get_no_result(self):
        """No result indexed returns False"""
        start, end = ['0000000000000000000000000000000000000000',
                      '0000000000000000000000000000000000000001']
        # given
        actual_results = self.indexer.run(
            start, end, incremental=False)

        # then
        self.assertFalse(actual_results)
