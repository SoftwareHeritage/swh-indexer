# Copyright (C) 2017-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from unittest.mock import patch

from swh.indexer.fossology_license import (
    FossologyLicenseIndexer, FossologyLicenseRangeIndexer,
    compute_license
)

from swh.indexer.tests.test_utils import (
    SHA1_TO_LICENSES, CommonContentIndexerTest, CommonContentIndexerRangeTest,
    CommonIndexerWithErrorsTest, CommonIndexerNoTool, NoDiskIndexer,
    BASE_TEST_CONFIG, fill_storage, fill_obj_storage
)


class BasicTest(unittest.TestCase):
    @patch('swh.indexer.fossology_license.subprocess')
    def test_compute_license(self, mock_subprocess):
        """Computing licenses from a raw content should return results

        """
        for path, intermediary_result, output in [
                (b'some/path', None,
                 []),
                (b'some/path/2', [],
                 []),
                (b'other/path', ' contains license(s) GPL,AGPL',
                 ['GPL', 'AGPL'])]:
            mock_subprocess.check_output.return_value = intermediary_result

            actual_result = compute_license(path, log=None)

            self.assertEqual(actual_result, {
                'licenses': output,
                'path': path,
            })


class InjectLicenseIndexer:
    """Override license computations.

    """
    def compute_license(self, path, log=None):
        """path is the content identifier

        """
        if isinstance(id, bytes):
            path = path.decode('utf-8')
        return {
            'licenses': SHA1_TO_LICENSES.get(path)
        }


class FossologyLicenseTestIndexer(
        NoDiskIndexer, InjectLicenseIndexer, FossologyLicenseIndexer):
    """Specific fossology license whose configuration is enough to satisfy
       the indexing checks.

    """
    def parse_config_file(self, *args, **kwargs):
        return {
            **BASE_TEST_CONFIG,
            'workdir': '/nowhere',
            'tools': {
                'name': 'nomos',
                'version': '3.1.0rc2-31-ga2cbb8c',
                'configuration': {
                    'command_line': 'nomossa <filepath>',
                },
            },
        }


class TestFossologyLicenseIndexer(CommonContentIndexerTest, unittest.TestCase):
    """Language indexer test scenarios:

    - Known sha1s in the input list have their data indexed
    - Unknown sha1 in the input list are not indexed

    """

    def get_indexer_results(self, ids):
        yield from self.idx_storage.content_ctags_get(ids)

    def setUp(self):
        super().setUp()
        self.indexer = FossologyLicenseTestIndexer()
        self.idx_storage = self.indexer.idx_storage

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
    def parse_config_file(self, *args, **kwargs):
        return {
            **BASE_TEST_CONFIG,
            'workdir': '/nowhere',
            'tools': {
                'name': 'nomos',
                'version': '3.1.0rc2-31-ga2cbb8c',
                'configuration': {
                    'command_line': 'nomossa <filepath>',
                },
            },
            'write_batch_size': 100,
        }


class TestFossologyLicenseRangeIndexer(
        CommonContentIndexerRangeTest, unittest.TestCase):
    """Range Fossology License Indexer tests.

    - new data within range are indexed
    - no data outside a range are indexed
    - with filtering existing indexed data prior to compute new index
    - without filtering existing indexed data prior to compute new index

    """
    def setUp(self):
        super().setUp()
        self.indexer = FossologyLicenseRangeIndexerTest()
        fill_storage(self.indexer.storage)
        fill_obj_storage(self.indexer.objstorage)

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
