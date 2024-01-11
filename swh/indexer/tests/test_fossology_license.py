# Copyright (C) 2017-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Any, Dict
import unittest
from unittest.mock import patch

import pytest

from swh.indexer import fossology_license
from swh.indexer.fossology_license import FossologyLicenseIndexer, compute_license
from swh.indexer.storage.model import ContentLicenseRow
from swh.indexer.tests.utils import (
    BASE_TEST_CONFIG,
    RAW_CONTENT_IDS,
    SHA1_TO_LICENSES,
    CommonContentIndexerTest,
    fill_obj_storage,
    fill_storage,
    filter_dict,
    mock_compute_license,
)


class BasicTest(unittest.TestCase):
    @patch("swh.indexer.fossology_license.subprocess")
    def test_compute_license(self, mock_subprocess):
        """Computing licenses from a raw content should return results"""
        for path, intermediary_result, output in [
            (b"some/path", None, []),
            (b"some/path/2", [], []),
            (b"other/path", " contains license(s) GPL,AGPL", ["GPL", "AGPL"]),
        ]:
            mock_subprocess.check_output.return_value = intermediary_result

            actual_result = compute_license(path)

            self.assertEqual(
                actual_result,
                {
                    "licenses": output,
                    "path": path,
                },
            )


CONFIG: Dict[str, Any] = {
    **BASE_TEST_CONFIG,
    "workdir": "/tmp",
    "tools": {
        "name": "nomos",
        "version": "3.1.0rc2-31-ga2cbb8c",
        "configuration": {
            "command_line": "nomossa <filepath>",
        },
    },
}

RANGE_CONFIG = dict(list(CONFIG.items()) + [("write_batch_size", 100)])


class TestFossologyLicenseIndexer(CommonContentIndexerTest, unittest.TestCase):
    """Fossology license indexer test scenarios:

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

        self.id0, self.id1, self.id2 = RAW_CONTENT_IDS

        tool = {k.replace("tool_", ""): v for (k, v) in self.indexer.tool.items()}

        # then
        self.expected_results = [
            *[
                ContentLicenseRow(id=self.id0, tool=tool, license=license)
                for license in SHA1_TO_LICENSES[self.id0]
            ],
            *[
                ContentLicenseRow(id=self.id1, tool=tool, license=license)
                for license in SHA1_TO_LICENSES[self.id1]
            ],
            *[],  # self.id2
        ]

    def tearDown(self):
        super().tearDown()
        fossology_license.compute_license = self.orig_compute_license


def test_fossology_w_no_tool():
    with pytest.raises(ValueError):
        FossologyLicenseIndexer(config=filter_dict(CONFIG, "tools"))
