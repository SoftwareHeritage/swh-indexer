# Copyright (C) 2017-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json
import unittest
from unittest.mock import patch

import pytest

import swh.indexer.ctags
from swh.indexer.ctags import CtagsIndexer, run_ctags
from swh.indexer.storage.model import ContentCtagsRow
from swh.indexer.tests.utils import (
    BASE_TEST_CONFIG,
    OBJ_STORAGE_DATA,
    SHA1_TO_CTAGS,
    CommonContentIndexerTest,
    fill_obj_storage,
    fill_storage,
    filter_dict,
)
from swh.model.hashutil import hash_to_bytes


class BasicTest(unittest.TestCase):
    @patch("swh.indexer.ctags.subprocess")
    def test_run_ctags(self, mock_subprocess):
        """Computing licenses from a raw content should return results"""
        output0 = """
{"name":"defun","kind":"function","line":1,"language":"scheme"}
{"name":"name","kind":"symbol","line":5,"language":"else"}"""
        output1 = """
{"name":"let","kind":"var","line":10,"language":"something"}"""

        expected_result0 = [
            {"name": "defun", "kind": "function", "line": 1, "lang": "scheme"},
            {"name": "name", "kind": "symbol", "line": 5, "lang": "else"},
        ]

        expected_result1 = [
            {"name": "let", "kind": "var", "line": 10, "lang": "something"}
        ]
        for path, lang, intermediary_result, expected_result in [
            (b"some/path", "lisp", output0, expected_result0),
            (b"some/path/2", "markdown", output1, expected_result1),
        ]:
            mock_subprocess.check_output.return_value = intermediary_result
            actual_result = list(run_ctags(path, lang=lang))
            self.assertEqual(actual_result, expected_result)


class InjectCtagsIndexer:
    """Override ctags computations."""

    def compute_ctags(self, path, lang):
        """Inject fake ctags given path (sha1 identifier)."""
        return {"lang": lang, **SHA1_TO_CTAGS.get(path)}


CONFIG = {
    **BASE_TEST_CONFIG,
    "tools": {
        "name": "universal-ctags",
        "version": "~git7859817b",
        "configuration": {
            "command_line": """ctags --fields=+lnz --sort=no """
            """ --links=no <filepath>""",
            "max_content_size": 1000,
        },
    },
    "languages": {
        "python": "python",
        "haskell": "haskell",
        "bar": "bar",
    },
    "workdir": "/tmp",
}


class TestCtagsIndexer(CommonContentIndexerTest, unittest.TestCase):
    """Ctags indexer test scenarios:

    - Known sha1s in the input list have their data indexed
    - Unknown sha1 in the input list are not indexed

    """

    def get_indexer_results(self, ids):
        yield from self.idx_storage.content_ctags_get(ids)

    def setUp(self):
        super().setUp()
        self.indexer = CtagsIndexer(config=CONFIG)
        self.indexer.catch_exceptions = False
        self.idx_storage = self.indexer.idx_storage
        fill_storage(self.indexer.storage)
        fill_obj_storage(self.indexer.objstorage)

        # Prepare test input
        self.id0 = "01c9379dfc33803963d07c1ccc748d3fe4c96bb5"
        self.id1 = "d4c647f0fc257591cc9ba1722484229780d1c607"
        self.id2 = "688a5ef812c53907562fe379d4b3851e69c7cb15"

        tool = {k.replace("tool_", ""): v for (k, v) in self.indexer.tool.items()}

        self.expected_results = [
            *[
                ContentCtagsRow(
                    id=hash_to_bytes(self.id0),
                    tool=tool,
                    **kwargs,
                )
                for kwargs in SHA1_TO_CTAGS[self.id0]
            ],
            *[
                ContentCtagsRow(
                    id=hash_to_bytes(self.id1),
                    tool=tool,
                    **kwargs,
                )
                for kwargs in SHA1_TO_CTAGS[self.id1]
            ],
            *[
                ContentCtagsRow(
                    id=hash_to_bytes(self.id2),
                    tool=tool,
                    **kwargs,
                )
                for kwargs in SHA1_TO_CTAGS[self.id2]
            ],
        ]

        self._set_mocks()

    def _set_mocks(self):
        def find_ctags_for_content(raw_content):
            for (sha1, ctags) in SHA1_TO_CTAGS.items():
                if OBJ_STORAGE_DATA[sha1] == raw_content:
                    return ctags
            else:
                raise ValueError(
                    ("%r not found in objstorage, can't mock its ctags.") % raw_content
                )

        def fake_language(raw_content, *args, **kwargs):
            ctags = find_ctags_for_content(raw_content)
            return {"lang": ctags[0]["lang"]}

        self._real_compute_language = swh.indexer.ctags.compute_language
        swh.indexer.ctags.compute_language = fake_language

        def fake_check_output(cmd, *args, **kwargs):
            id_ = cmd[-1].split("/")[-1]
            return "\n".join(
                json.dumps({"language": ctag["lang"], **ctag})
                for ctag in SHA1_TO_CTAGS[id_]
            )

        self._real_check_output = swh.indexer.ctags.subprocess.check_output
        swh.indexer.ctags.subprocess.check_output = fake_check_output

    def tearDown(self):
        swh.indexer.ctags.compute_language = self._real_compute_language
        swh.indexer.ctags.subprocess.check_output = self._real_check_output
        super().tearDown()


def test_ctags_w_no_tool():
    with pytest.raises(ValueError):
        CtagsIndexer(config=filter_dict(CONFIG, "tools"))
