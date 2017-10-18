# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest
import logging
from nose.tools import istest

from swh.indexer.mimetype import ContentMimetypeIndexer

from swh.indexer.tests.test_utils import MockObjStorage


class MockStorage():
    """Mock storage to simplify reading indexers' outputs.

    """
    def content_mimetype_add(self, mimetypes, conflict_update=None):
        self.state = mimetypes
        self.conflict_update = conflict_update

    def indexer_configuration_get(self, tool):
        return {
            'id': 10,
        }


class TestMimetypeIndexer(ContentMimetypeIndexer):
    """Specific mimetype whose configuration is enough to satisfy the
       indexing tests.

    """
    def prepare(self):
        self.config = {
            'destination_queue': None,
            'rescheduling_task': None,
            'tools': {
                'name': 'file',
                'version': '5.22',
                'configuration': 'file --mime <filename>',
            },
        }
        self.storage = MockStorage()
        self.log = logging.getLogger('swh.indexer')
        self.objstorage = MockObjStorage()
        self.task_destination = None
        self.rescheduling_task = self.config['rescheduling_task']
        self.destination_queue = self.config['destination_queue']
        self.tools = self.retrieve_tools_information()


class TestMimetypeIndexerWrongStorage(TestMimetypeIndexer):
    """Specific mimetype whose configuration is not enough to satisfy the
       indexing tests.

    """
    def prepare(self):
        super().prepare()
        self.tools = None


class TestMimetypeIndexerWithErrors(unittest.TestCase):

    @istest
    def test_index_fail_because_wrong_tool(self):
        try:
            TestMimetypeIndexerWrongStorage()
        except ValueError:
            pass
        else:
            self.fail('An error should be raised about wrong tool being used.')


class TestMimetypeIndexerTest(unittest.TestCase):
    def setUp(self):
        self.indexer = TestMimetypeIndexer()

    @istest
    def test_index_no_update(self):
        # given
        sha1s = ['01c9379dfc33803963d07c1ccc748d3fe4c96bb50',
                 '688a5ef812c53907562fe379d4b3851e69c7cb15']

        # when
        self.indexer.run(sha1s, policy_update='ignore-dups')

        # then
        expected_results = [{
            'id': '01c9379dfc33803963d07c1ccc748d3fe4c96bb50',
            'indexer_configuration_id': 10,
            'mimetype': b'text/plain',
            'encoding': b'us-ascii',
        }, {
            'id': '688a5ef812c53907562fe379d4b3851e69c7cb15',
            'indexer_configuration_id': 10,
            'mimetype': b'text/plain',
            'encoding': b'us-ascii',
        }]

        self.assertFalse(self.indexer.storage.conflict_update)
        self.assertEquals(expected_results, self.indexer.storage.state)

    @istest
    def test_index_update(self):
        # given
        sha1s = ['01c9379dfc33803963d07c1ccc748d3fe4c96bb50',
                 '688a5ef812c53907562fe379d4b3851e69c7cb15']

        # when
        self.indexer.run(sha1s, policy_update='update-dups')

        # then
        expected_results = [{
            'id': '01c9379dfc33803963d07c1ccc748d3fe4c96bb50',
            'indexer_configuration_id': 10,
            'mimetype': b'text/plain',
            'encoding': b'us-ascii',
        }, {
            'id': '688a5ef812c53907562fe379d4b3851e69c7cb15',
            'indexer_configuration_id': 10,
            'mimetype': b'text/plain',
            'encoding': b'us-ascii',
        }]

        self.assertTrue(self.indexer.storage.conflict_update)
        self.assertEquals(expected_results, self.indexer.storage.state)

    @istest
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

        self.assertTrue(self.indexer.storage.conflict_update)
        self.assertEquals(expected_results, self.indexer.storage.state)
