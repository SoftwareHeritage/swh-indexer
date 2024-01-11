# Copyright (C) 2017-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Any, Dict
import unittest

import pytest

from swh.indexer.mimetype import MimetypeIndexer, compute_mimetype_encoding
from swh.indexer.storage.model import ContentMimetypeRow
from swh.indexer.tests.utils import (
    BASE_TEST_CONFIG,
    RAW_CONTENT_IDS,
    RAW_CONTENTS,
    CommonContentIndexerTest,
    fill_obj_storage,
    fill_storage,
    filter_dict,
)


@pytest.mark.parametrize(
    "raw_text,mimetypes,encoding",
    RAW_CONTENTS.values(),
)
def test_compute_mimetype_encoding(raw_text, mimetypes, encoding):
    """Compute mimetype encoding should return results"""
    actual_result = compute_mimetype_encoding(raw_text)

    # Older libmagic versions (e.g. buster: 1:5.35-4+deb10u2, bullseye: 1:5.39-3)
    # returns different results. This allows to deal with such a case when executing
    # tests on different environments machines (e.g. ci tox, ci debian, dev machine,
    # ...)
    all_mimetypes = mimetypes if isinstance(mimetypes, tuple) else [mimetypes]

    assert actual_result in [
        {"mimetype": mimetype, "encoding": encoding} for mimetype in all_mimetypes
    ]


CONFIG: Dict[str, Any] = {
    **BASE_TEST_CONFIG,
    "tools": {
        "name": "file",
        "version": "1:5.30-1+deb9u1",
        "configuration": {"type": "library", "debian-package": "python3-magic"},
    },
}


class TestMimetypeIndexer(CommonContentIndexerTest, unittest.TestCase):
    """Mimetype indexer test scenarios:

    - Known sha1s in the input list have their data indexed
    - Unknown sha1 in the input list are not indexed

    """

    def get_indexer_results(self, ids):
        yield from self.idx_storage.content_mimetype_get(ids)

    def setUp(self):
        self.indexer = MimetypeIndexer(config=CONFIG)
        self.indexer.catch_exceptions = False
        self.idx_storage = self.indexer.idx_storage
        fill_storage(self.indexer.storage)
        fill_obj_storage(self.indexer.objstorage)

        self.id0, self.id1, self.id2 = RAW_CONTENT_IDS

        tool = {k.replace("tool_", ""): v for (k, v) in self.indexer.tool.items()}

        results = []
        for raw_content_id, (raw_content, mimetypes, encoding) in RAW_CONTENTS.items():
            # Older libmagic versions (e.g. buster: 1:5.35-4+deb10u2, bullseye:
            # 1:5.39-3) returns different results. This allows to deal with such a case
            # when executing tests on different environments machines (e.g. ci tox, ci
            # debian, dev machine, ...)
            all_mimetypes = mimetypes if isinstance(mimetypes, tuple) else [mimetypes]

            results.extend(
                [
                    ContentMimetypeRow(
                        id=raw_content_id,
                        tool=tool,
                        mimetype=mimetype,
                        encoding=encoding,
                    )
                    for mimetype in all_mimetypes
                ]
            )

        self.expected_results = results


RANGE_CONFIG = dict(list(CONFIG.items()) + [("write_batch_size", 100)])


def test_mimetype_w_no_tool():
    with pytest.raises(ValueError):
        MimetypeIndexer(config=filter_dict(CONFIG, "tools"))
