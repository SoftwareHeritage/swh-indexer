# Copyright (C) 2017-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest
import logging

from swh.indexer.fossology_license import (
    ContentFossologyLicenseIndexer, FossologyLicenseRangeIndexer
)

from swh.indexer.tests.test_utils import (
    MockObjStorage, BasicMockStorage, BasicMockIndexerStorage,
    SHA1_TO_LICENSES, CommonContentIndexerTest, CommonContentIndexerRangeTest,
    CommonIndexerWithErrorsTest, CommonIndexerNoTool, NoDiskIndexer
)


class InjectLicenseIndexer:
    """Override license computations.

    """
    def compute_license(self, path, log=None):
        """path is the content identifier

        """
        return {
            'licenses': SHA1_TO_LICENSES.get(path)
        }


class FossologyLicenseTestIndexer(
        NoDiskIndexer, InjectLicenseIndexer, ContentFossologyLicenseIndexer):
    """Specific fossology license whose configuration is enough to satisfy
       the indexing checks.

    """
    def prepare(self):
        self.config = {
            'tools': {
                'name': 'nomos',
                'version': '3.1.0rc2-31-ga2cbb8c',
                'configuration': {
                    'command_line': 'nomossa <filepath>',
                },
            },
        }
        self.idx_storage = BasicMockIndexerStorage()
        self.log = logging.getLogger('swh.indexer')
        self.objstorage = MockObjStorage()
        self.tools = self.register_tools(self.config['tools'])
        self.tool = self.tools[0]


class TestFossologyLicenseIndexer(CommonContentIndexerTest, unittest.TestCase):
    """Language indexer test scenarios:

    - Known sha1s in the input list have their data indexed
    - Unknown sha1 in the input list are not indexed

    """
    def setUp(self):
        self.indexer = FossologyLicenseTestIndexer()

        self.id0 = '01c9379dfc33803963d07c1ccc748d3fe4c96bb5'
        self.id1 = '688a5ef812c53907562fe379d4b3851e69c7cb15'
        self.id2 = 'da39a3ee5e6b4b0d3255bfef95601890afd80709'  # empty content
        tool_id = self.indexer.tool['id']
        # then
        self.expected_results = {
            self.id0: {
                'id': self.id0,
                'indexer_configuration_id': tool_id,
                'licenses': SHA1_TO_LICENSES[self.id0],
            },
            self.id1: {
                'id': self.id1,
                'indexer_configuration_id': tool_id,
                'licenses': SHA1_TO_LICENSES[self.id1],
            },
            self.id2: {
                'id': self.id2,
                'indexer_configuration_id': tool_id,
                'licenses': SHA1_TO_LICENSES[self.id2],
            }
        }


class FossologyLicenseRangeIndexerTest(
        NoDiskIndexer, InjectLicenseIndexer, FossologyLicenseRangeIndexer):
    """Testing the range indexer on fossology license.

    """
    def prepare(self):
        self.config = {
            'tools': {
                'name': 'nomos',
                'version': '3.1.0rc2-31-ga2cbb8c',
                'configuration': {
                    'command_line': 'nomossa <filepath>',
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


class TestFossologyLicenseRangeIndexer(
        CommonContentIndexerRangeTest, unittest.TestCase):
    """Range Fossology License Indexer tests.

    - new data within range are indexed
    - no data outside a range are indexed
    - with filtering existing indexed data prior to compute new index
    - without filtering existing indexed data prior to compute new index

    """
    def setUp(self):
        self.indexer = FossologyLicenseRangeIndexerTest()
        # will play along with the objstorage's mocked contents for now
        self.contents = sorted(self.indexer.objstorage)
        # FIXME: leverage swh.objstorage.in_memory_storage's
        # InMemoryObjStorage, swh.storage.tests's gen_contents, and
        # hypothesis to generate data to actually run indexer on those

        self.id0 = '01c9379dfc33803963d07c1ccc748d3fe4c96bb5'
        self.id1 = '02fb2c89e14f7fab46701478c83779c7beb7b069'
        self.id2 = '103bc087db1d26afc3a0283f38663d081e9b01e6'
        tool_id = self.indexer.tool['id']
        self.expected_results = {
            self.id0: {
                'id': self.id0,
                'indexer_configuration_id': tool_id,
                'licenses': SHA1_TO_LICENSES[self.id0]
            },
            self.id1: {
                'id': self.id1,
                'indexer_configuration_id': tool_id,
                'licenses': SHA1_TO_LICENSES[self.id1]
            },
            self.id2: {
                'id': self.id2,
                'indexer_configuration_id': tool_id,
                'licenses': SHA1_TO_LICENSES[self.id2]
            }
        }


class FossologyLicenseIndexerUnknownToolTestStorage(
        CommonIndexerNoTool, FossologyLicenseTestIndexer):
    """Fossology license indexer with wrong configuration"""


class FossologyLicenseRangeIndexerUnknownToolTestStorage(
        CommonIndexerNoTool, FossologyLicenseRangeIndexerTest):
    """Fossology license range indexer with wrong configuration"""


class TestFossologyLicenseIndexersErrors(
        CommonIndexerWithErrorsTest, unittest.TestCase):
    """Test the indexer raise the right errors when wrongly initialized"""
    Indexer = FossologyLicenseIndexerUnknownToolTestStorage
    RangeIndexer = FossologyLicenseRangeIndexerUnknownToolTestStorage