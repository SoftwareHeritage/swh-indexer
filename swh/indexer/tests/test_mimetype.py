# Copyright (C) 2017-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest
import logging

from swh.indexer.mimetype import (
    ContentMimetypeIndexer, MimetypeRangeIndexer
)

from swh.indexer.tests.test_utils import (
    MockObjStorage, BasicMockStorage, BasicMockIndexerStorage,
    CommonContentIndexerTest, CommonContentIndexerRangeTest
)


class MimetypeTestIndexer(ContentMimetypeIndexer):
    """Specific mimetype indexer instance whose configuration is enough to
       satisfy the indexing tests.

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
        self.idx_storage = BasicMockIndexerStorage()
        self.log = logging.getLogger('swh.indexer')
        self.objstorage = MockObjStorage()
        self.tools = self.register_tools(self.config['tools'])
        self.tool = self.tools[0]


class MimetypeIndexerUnknownToolTestStorage(MimetypeTestIndexer):
    """Specific mimetype whose configuration is not enough to satisfy the
       indexing checks.

    """
    def prepare(self):
        super().prepare()
        self.tools = None


class TestMimetypeIndexerWithErrors(unittest.TestCase):
    def test_wrong_unknown_configuration_tool(self):
        """Indexer with unknown configuration tool should fail the check"""
        with self.assertRaisesRegex(ValueError, 'Tools None is unknown'):
            MimetypeIndexerUnknownToolTestStorage()


class TestMimetypeIndexer(CommonContentIndexerTest, unittest.TestCase):
    """Mimetype indexer test scenarios:

    - Known sha1s in the input list have their data indexed
    - Unknown sha1 in the input list are not indexed

    """
    def setUp(self):
        self.indexer = MimetypeTestIndexer()

        self.id0 = '01c9379dfc33803963d07c1ccc748d3fe4c96bb5'
        self.id1 = '688a5ef812c53907562fe379d4b3851e69c7cb15'
        self.id2 = 'da39a3ee5e6b4b0d3255bfef95601890afd80709'
        self.expected_results = {
            self.id0: {
                'id': self.id0,
                'indexer_configuration_id': 10,
                'mimetype': b'text/plain',
                'encoding': b'us-ascii',
            },
            self.id1: {
                'id': self.id1,
                'indexer_configuration_id': 10,
                'mimetype': b'text/plain',
                'encoding': b'us-ascii',
            },
            self.id2: {
                'id': self.id2,
                'indexer_configuration_id': 10,
                'mimetype': b'application/x-empty',
                'encoding': b'binary',
            }
        }


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
        self.idx_storage = BasicMockIndexerStorage()
        self.log = logging.getLogger('swh.indexer')
        # this hardcodes some contents, will use this to setup the storage
        self.objstorage = MockObjStorage()
        # sync objstorage and storage
        contents = [{'sha1': c_id} for c_id in self.objstorage]
        self.storage = BasicMockStorage(contents)
        self.tools = self.register_tools(self.config['tools'])
        self.tool = self.tools[0]


class TestMimetypeRangeIndexer(
        CommonContentIndexerRangeTest, unittest.TestCase):
    """Range Mimetype Indexer tests.

    - new data within range are indexed
    - no data outside a range are indexed
    - with filtering existing indexed data prior to compute new index
    - without filtering existing indexed data prior to compute new index

    """
    def setUp(self):
        self.indexer = MimetypeRangeIndexerTest()
        # will play along with the objstorage's mocked contents for now
        self.contents = sorted(self.indexer.objstorage)
        # FIXME: leverage swh.objstorage.in_memory_storage's
        # InMemoryObjStorage, swh.storage.tests's gen_contents, and
        # hypothesis to generate data to actually run indexer on those

        self.id0 = '01c9379dfc33803963d07c1ccc748d3fe4c96bb5'
        self.id1 = '02fb2c89e14f7fab46701478c83779c7beb7b069'
        self.id2 = '103bc087db1d26afc3a0283f38663d081e9b01e6'
        self.expected_results = {
            self.id0: {
                'encoding': b'us-ascii',
                'id': self.id0,
                'indexer_configuration_id': 10,
                'mimetype': b'text/plain'},
            self.id1: {
                'encoding': b'us-ascii',
                'id': self.id1,
                'indexer_configuration_id': 10,
                'mimetype': b'text/x-python'},
            self.id2: {
                'encoding': b'us-ascii',
                'id': self.id2,
                'indexer_configuration_id': 10,
                'mimetype': b'text/plain'}
        }
