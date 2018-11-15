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
        # to make input test data conciliant with actual runtime the
        # other way of doing properly things would be to rewrite all
        # tests (that's another task entirely so no)
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
        # to make input test data conciliant with actual runtime the
        # other way of doing properly things would be to rewrite all
        # tests (that's another task entirely so no)
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
            if start <= _id and _id <= end and \
               _tool_id == indexer_configuration_id:
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

    def test_generate_content_mimetype_get_range_wrong_input(self):
        """Wrong input should fail asap

        """
        with self.assertRaises(ValueError) as e:
            self.indexer.run([1, 2, 3], 'ignore-dups')

        self.assertEqual(e.exception.args, ('Range of ids expected', ))

    def test_generate_content_mimetype_get(self):
        """Optimal indexing should result in persisted computations

        """
        start, end = [self.contents[0], self.contents[2]]  # output hex ids
        # given
        actual_results = self.indexer.run(
            [start, end], policy_update='update-dups')

        # then
        expected_results = [
            {'encoding': b'us-ascii',
             'id': '01c9379dfc33803963d07c1ccc748d3fe4c96bb5',
             'indexer_configuration_id': 10,
             'mimetype': b'text/plain'},
            {'encoding': b'us-ascii',
             'id': '02fb2c89e14f7fab46701478c83779c7beb7b069',
             'indexer_configuration_id': 10,
             'mimetype': b'text/x-python'},
            {'encoding': b'us-ascii',
             'id': '103bc087db1d26afc3a0283f38663d081e9b01e6',
             'indexer_configuration_id': 10,
             'mimetype': b'text/plain'}
        ]

        self.assertEqual(expected_results, actual_results)

        for m in actual_results:
            _id = m['id']
            self.assertTrue(start <= _id and _id <= end)
            _tool_id = m['indexer_configuration_id']
            self.assertEqual(_tool_id, self.indexer.tool['id'])

    def test_generate_content_mimetype_get_input_as_bytes(self):
        """Optimal indexing should result in persisted computations

        Input are in bytes here.

        """
        start, end = [hashutil.hash_to_bytes(self.contents[0]),
                      hashutil.hash_to_bytes(self.contents[2])]
        # given
        actual_results = self.indexer.run(
            [start, end], policy_update='update-dups')

        # then
        expected_results = [
            {'encoding': b'us-ascii',
             'id': '01c9379dfc33803963d07c1ccc748d3fe4c96bb5',
             'indexer_configuration_id': 10,
             'mimetype': b'text/plain'},
            {'encoding': b'us-ascii',
             'id': '02fb2c89e14f7fab46701478c83779c7beb7b069',
             'indexer_configuration_id': 10,
             'mimetype': b'text/x-python'},
            {'encoding': b'us-ascii',
             'id': '103bc087db1d26afc3a0283f38663d081e9b01e6',
             'indexer_configuration_id': 10,
             'mimetype': b'text/plain'}
        ]

        self.assertEqual(expected_results, actual_results)

        for m in actual_results:
            _id = hashutil.hash_to_bytes(m['id'])
            self.assertTrue(start <= _id and _id <= end)
            _tool_id = m['indexer_configuration_id']
            self.assertEqual(_tool_id, self.indexer.tool['id'])
