# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Any, Dict, List, Optional
from unittest.mock import Mock

import pytest

from swh.indexer.indexer import (
    ContentIndexer,
    ContentPartitionIndexer,
    OriginIndexer,
    RevisionIndexer,
)
from swh.indexer.storage import PagedResult, Sha1

from .utils import BASE_TEST_CONFIG


class _TestException(Exception):
    pass


class CrashingIndexerMixin:
    USE_TOOLS = False

    def index(
        self, id: Any, data: Optional[Any] = None, **kwargs
    ) -> List[Dict[str, Any]]:
        raise _TestException()

    def persist_index_computations(self, results) -> Dict[str, int]:
        return {}

    def indexed_contents_in_partition(
        self, partition_id: int, nb_partitions: int, page_token: Optional[str] = None
    ) -> PagedResult[Sha1]:
        raise _TestException()


class CrashingContentIndexer(CrashingIndexerMixin, ContentIndexer):
    pass


class CrashingContentPartitionIndexer(CrashingIndexerMixin, ContentPartitionIndexer):
    pass


class CrashingRevisionIndexer(CrashingIndexerMixin, RevisionIndexer):
    pass


class CrashingOriginIndexer(CrashingIndexerMixin, OriginIndexer):
    pass


def test_content_indexer_catch_exceptions():
    indexer = CrashingContentIndexer(config=BASE_TEST_CONFIG)
    indexer.objstorage = Mock()
    indexer.objstorage.get.return_value = b"content"

    assert indexer.run([b"foo"]) == {"status": "failed"}

    indexer.catch_exceptions = False

    with pytest.raises(_TestException):
        indexer.run([b"foo"])


def test_revision_indexer_catch_exceptions():
    indexer = CrashingRevisionIndexer(config=BASE_TEST_CONFIG)
    indexer.storage = Mock()
    indexer.storage.revision_get.return_value = ["rev"]

    assert indexer.run([b"foo"]) == {"status": "failed"}

    indexer.catch_exceptions = False

    with pytest.raises(_TestException):
        indexer.run([b"foo"])


def test_origin_indexer_catch_exceptions():
    indexer = CrashingOriginIndexer(config=BASE_TEST_CONFIG)

    assert indexer.run(["http://example.org"]) == {"status": "failed"}

    indexer.catch_exceptions = False

    with pytest.raises(_TestException):
        indexer.run(["http://example.org"])


def test_content_partition_indexer_catch_exceptions():
    indexer = CrashingContentPartitionIndexer(
        config={**BASE_TEST_CONFIG, "write_batch_size": 42}
    )

    assert indexer.run(0, 42) == {"status": "failed"}

    indexer.catch_exceptions = False

    with pytest.raises(_TestException):
        indexer.run(0, 42)
