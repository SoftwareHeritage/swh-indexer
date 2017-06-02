# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest
from nose.tools import istest

from swh.indexer.mimetype import ContentMimetypeIndexer
from swh.objstorage.exc import ObjNotFoundError


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


class MockStorageWrongConfiguration():
    def indexer_configuration_get(self, tool):
        return None


class MockObjStorage():
    """Mock objstorage with predefined contents.

    """
    def __init__(self):
        self.data = {
            '01c9379dfc33803963d07c1ccc748d3fe4c96bb50': b'this is some text',
            '688a5ef812c53907562fe379d4b3851e69c7cb15': b'another text',
            '8986af901dd2043044ce8f0d8fc039153641cf17': b'yet another text',
        }

    def get(self, sha1):
        raw_content = self.data.get(sha1)
        if not raw_content:
            raise ObjNotFoundError()
        return raw_content


class TestMimetypeIndexerNoNextStep(ContentMimetypeIndexer):
    def __init__(self, wrong_storage=False):
        super().__init__()
        self.config = {
            'destination_queue': None,
            'rescheduling_task': None,
            'tools': {
                'name': 'file',
                'version': '5.22',
                'configuration': 'file --mime <filename>',
            },
        }
        if wrong_storage:
            self.storage = MockStorageWrongConfiguration()
        else:
            self.storage = MockStorage()

        self.objstorage = MockObjStorage()
        self.task_destination = None


class TestMimetypeIndexerWithErrors(unittest.TestCase):

    @istest
    def test_index_fail_because_wrong_tool(self):
        indexer = TestMimetypeIndexerNoNextStep(wrong_storage=True)

        try:
            indexer.run(sha1s=[], policy_update='ignore-dups')
        except ValueError:
            pass
        else:
            self.fail('An error should be raised about wrong tool being used.')


class TestMimetypeIndexer(unittest.TestCase):
    def setUp(self):
        self.indexer = TestMimetypeIndexerNoNextStep()

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