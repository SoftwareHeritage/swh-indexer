# Copyright (C) 2017-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest
import pytest

from unittest.mock import patch
from typing import Any, Dict

from swh.indexer import fossology_license
from swh.indexer.fossology_license import (
    FossologyLicenseIndexer,
    FossologyLicensePartitionIndexer,
    compute_license,
)

from swh.indexer.tests.utils import (
    SHA1_TO_LICENSES,
    CommonContentIndexerTest,
    CommonContentIndexerPartitionTest,
    BASE_TEST_CONFIG,
    fill_storage,
    fill_obj_storage,
    filter_dict,
)


class BasicTest(unittest.TestCase):
    @patch("swh.indexer.fossology_license.subprocess")
    def test_compute_license(self, mock_subprocess):
        """Computing licenses from a raw content should return results

        """
        for path, intermediary_result, output in [
            (b"some/path", None, []),
            (b"some/path/2", [], []),
            (b"other/path", " contains license(s) GPL,AGPL", ["GPL", "AGPL"]),
        ]:
            mock_subprocess.check_output.return_value = intermediary_result

            actual_result = compute_license(path)

            self.assertEqual(actual_result, {"licenses": output, "path": path,})


def mock_compute_license(path):
    """path is the content identifier

    """
    if isinstance(id, bytes):
        path = path.decode("utf-8")
    # path is something like /tmp/tmpXXX/<sha1> so we keep only the sha1 part
    path = path.split("/")[-1]
    return {"licenses": SHA1_TO_LICENSES.get(path)}


CONFIG = {
    **BASE_TEST_CONFIG,
    "workdir": "/tmp",
    "tools": {
        "name": "nomos",
        "version": "3.1.0rc2-31-ga2cbb8c",
        "configuration": {"command_line": "nomossa <filepath>",},
    },
}  # type: Dict[str, Any]

RANGE_CONFIG = dict(list(CONFIG.items()) + [("write_batch_size", 100)])


class TestFossologyLicenseIndexer(CommonContentIndexerTest, unittest.TestCase):
    """Language indexer test scenarios:

    - Known sha1s in the input list have their data indexed
    - Unknown sha1 in the input list are not indexed

    """

    def get_indexer_results(self, ids):
        yield from self.idx_storage.content_fossology_license_get(ids)

    def setUp(self):
        super().setUp()
        # replace actual license computation with a mock
        self.orig_compute_license = fossology_license.compute_license
        fossology_license.compute_license = mock_compute_license

        self.indexer = FossologyLicenseIndexer(CONFIG)
        self.indexer.catch_exceptions = False
        self.idx_storage = self.indexer.idx_storage
        fill_storage(self.indexer.storage)
        fill_obj_storage(self.indexer.objstorage)

        self.id0 = "01c9379dfc33803963d07c1ccc748d3fe4c96bb5"
        self.id1 = "688a5ef812c53907562fe379d4b3851e69c7cb15"
        self.id2 = "da39a3ee5e6b4b0d3255bfef95601890afd80709"  # empty content

        tool = {k.replace("tool_", ""): v for (k, v) in self.indexer.tool.items()}
        # then
        self.expected_results = {
            self.id0: {"tool": tool, "licenses": SHA1_TO_LICENSES[self.id0],},
            self.id1: {"tool": tool, "licenses": SHA1_TO_LICENSES[self.id1],},
            self.id2: {"tool": tool, "licenses": SHA1_TO_LICENSES[self.id2],},
        }

    def tearDown(self):
        super().tearDown()
        fossology_license.compute_license = self.orig_compute_license


class TestFossologyLicensePartitionIndexer(
    CommonContentIndexerPartitionTest, unittest.TestCase
):
    """Range Fossology License Indexer tests.

    - new data within range are indexed
    - no data outside a range are indexed
    - with filtering existing indexed data prior to compute new index
    - without filtering existing indexed data prior to compute new index

    """

    def setUp(self):
        super().setUp()

        # replace actual license computation with a mock
        self.orig_compute_license = fossology_license.compute_license
        fossology_license.compute_license = mock_compute_license

        self.indexer = FossologyLicensePartitionIndexer(config=RANGE_CONFIG)
        self.indexer.catch_exceptions = False
        fill_storage(self.indexer.storage)
        fill_obj_storage(self.indexer.objstorage)

    def tearDown(self):
        super().tearDown()
        fossology_license.compute_license = self.orig_compute_license


def test_fossology_w_no_tool():
    with pytest.raises(ValueError):
        FossologyLicenseIndexer(config=filter_dict(CONFIG, "tools"))


def test_fossology_range_w_no_tool():
    with pytest.raises(ValueError):
        FossologyLicensePartitionIndexer(config=filter_dict(RANGE_CONFIG, "tools"))
