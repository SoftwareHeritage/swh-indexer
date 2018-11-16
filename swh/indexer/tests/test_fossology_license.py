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
    SHA1_TO_LICENSES, IndexerRangeTest
)


class NoDiskIndexer:
    """Mixin to override the DiskIndexer behavior avoiding side-effects in
       tests.

    """

    def write_to_temp(self, filename, data):  # noop
        return filename

    def cleanup(self, content_path):  # noop
        return None


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
    """Specific mimetype whose configuration is enough to satisfy the
       indexing tests.

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


class FossologyLicenseIndexerUnknownToolTestStorage(
        FossologyLicenseTestIndexer):
    """Specific fossology license indexer whose configuration is not
       enough to satisfy the indexing checks

    """
    def prepare(self):
        super().prepare()
        self.tools = None


class TestFossologyLicenseIndexerWithErrors(unittest.TestCase):
    def test_wrong_unknown_configuration_tool(self):
        """Indexer with unknown configuration tool should fail the check"""
        with self.assertRaisesRegex(ValueError, 'Tools None is unknown'):
            FossologyLicenseIndexerUnknownToolTestStorage()


class TestFossologyLicenseIndexer(unittest.TestCase):
    """Fossology license tests.

    """
    def setUp(self):
        self.indexer = FossologyLicenseTestIndexer()

    def test_index_no_update(self):
        """Index sha1s results in new computed licenses

        """
        id0 = '01c9379dfc33803963d07c1ccc748d3fe4c96bb5'
        id1 = '688a5ef812c53907562fe379d4b3851e69c7cb15'
        sha1s = [id0, id1]

        # when
        self.indexer.run(sha1s, policy_update='ignore-dups')

        # then
        expected_results = [{
            'id': id0,
            'indexer_configuration_id': 10,
            'licenses': SHA1_TO_LICENSES[id0],
        }, {
            'id': id1,
            'indexer_configuration_id': 10,
            'licenses': SHA1_TO_LICENSES[id1],
        }]

        self.assertFalse(self.indexer.idx_storage.conflict_update)
        self.assertEqual(expected_results, self.indexer.idx_storage.state)

    def test_index_update(self):
        """Index sha1s results in new computed licenses

        """
        id0 = '01c9379dfc33803963d07c1ccc748d3fe4c96bb5'
        id1 = '688a5ef812c53907562fe379d4b3851e69c7cb15'
        id2 = 'da39a3ee5e6b4b0d3255bfef95601890afd80709'  # empty content
        sha1s = [id0, id1, id2]

        # when
        self.indexer.run(sha1s, policy_update='update-dups')

        # then
        expected_results = [{
            'id': id0,
            'indexer_configuration_id': 10,
            'licenses': SHA1_TO_LICENSES[id0],
        }, {
            'id': id1,
            'indexer_configuration_id': 10,
            'licenses': SHA1_TO_LICENSES[id1],
        }, {
            'id': id2,
            'indexer_configuration_id': 10,
            'licenses': SHA1_TO_LICENSES[id2],
        }]

        self.assertTrue(self.indexer.idx_storage.conflict_update)
        self.assertEqual(expected_results, self.indexer.idx_storage.state)

    def test_index_one_unknown_sha1(self):
        """Only existing contents are indexed

        """
        # given
        id0 = '688a5ef812c53907562fe379d4b3851e69c7cb15'
        sha1s = [id0,
                 '799a5ef812c53907562fe379d4b3851e69c7cb15',  # unknown
                 '800a5ef812c53907562fe379d4b3851e69c7cb15']  # unknown

        # when
        self.indexer.run(sha1s, policy_update='update-dups')

        # then
        expected_results = [{
            'id': id0,
            'indexer_configuration_id': 10,
            'licenses': SHA1_TO_LICENSES[id0],
        }]

        self.assertTrue(self.indexer.idx_storage.conflict_update)
        self.assertEqual(expected_results, self.indexer.idx_storage.state)


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


class TestFossologyLicenseRangeIndexer(IndexerRangeTest, unittest.TestCase):
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
        self.expected_results = {
            self.id0: {
                'id': self.id0,
                'indexer_configuration_id': 10,
                'licenses': SHA1_TO_LICENSES[self.id0]
            },
            self.id1: {
                'id': self.id1,
                'indexer_configuration_id': 10,
                'licenses': SHA1_TO_LICENSES[self.id1]
            },
            self.id2: {
                'id': self.id2,
                'indexer_configuration_id': 10,
                'licenses': SHA1_TO_LICENSES[self.id2]
            }
        }
