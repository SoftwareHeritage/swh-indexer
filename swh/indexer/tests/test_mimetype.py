# Copyright (C) 2017-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest
import logging

from swh.indexer.mimetype import ContentMimetypeIndexer

from swh.indexer.tests.test_utils import MockObjStorage


class _MockIndexerStorage():
    """Mock storage to simplify reading indexers' outputs.

    """
    def content_mimetype_add(self, mimetypes, conflict_update=None):
        self.state = mimetypes
        self.conflict_update = conflict_update

    def indexer_configuration_add(self, tools):
        return [{
            'id': 10,
        }]


class TestMimetypeIndexer(ContentMimetypeIndexer):
    """Specific mimetype whose configuration is enough to satisfy the
       indexing tests.

    """
    def prepare(self):
        self.config = {
            'destination_task': None,
            'rescheduling_task': None,
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
        self.destination_task = None
        self.rescheduling_task = self.config['rescheduling_task']
        self.destination_task = self.config['destination_task']
        self.tools = self.register_tools(self.config['tools'])
        self.tool = self.tools[0]


class TestMimetypeIndexerUnknownToolStorage(TestMimetypeIndexer):
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
            TestMimetypeIndexerUnknownToolStorage()


class TestMimetypeIndexerTest(unittest.TestCase):
    def setUp(self):
        self.indexer = TestMimetypeIndexer()

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
