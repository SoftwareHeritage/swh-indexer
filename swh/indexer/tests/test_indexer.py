# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Any, Dict, Iterable, List, Optional
from unittest.mock import Mock

import pytest

from swh.indexer.indexer import (
    ContentIndexer,
    ContentPartitionIndexer,
    DirectoryIndexer,
    OriginIndexer,
)
from swh.indexer.storage import PagedResult, Sha1
from swh.model.model import Content

from .utils import BASE_TEST_CONFIG, DIRECTORY2


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
        self, partition_id: int, nb_partitions: int
    ) -> Iterable[Sha1]:
        raise _TestException()


class CrashingContentIndexer(CrashingIndexerMixin, ContentIndexer):
    pass


class CrashingContentPartitionIndexer(CrashingIndexerMixin, ContentPartitionIndexer):
    pass


class CrashingDirectoryIndexer(CrashingIndexerMixin, DirectoryIndexer):
    pass


class CrashingOriginIndexer(CrashingIndexerMixin, OriginIndexer):
    pass


class TrivialContentPartitionIndexer(ContentPartitionIndexer[str]):
    USE_TOOLS = False

    def index(self, id: bytes, data: Optional[bytes], **kwargs) -> List[str]:
        return ["indexed " + id.decode()]

    def indexed_contents_in_partition(
        self, partition_id: int, nb_partitions: int
    ) -> Iterable[Sha1]:
        return iter([b"excluded hash", b"other excluded hash"])

    def persist_index_computations(self, results: List[str]) -> Dict[str, int]:
        self._results.append(results)  # type: ignore
        return {"nb_added": len(results)}


def test_content_indexer_catch_exceptions():
    indexer = CrashingContentIndexer(config=BASE_TEST_CONFIG)
    indexer.objstorage = Mock()
    indexer.objstorage.get.return_value = b"content"

    assert indexer.run([b"foo"]) == {"status": "failed"}

    indexer.catch_exceptions = False

    with pytest.raises(_TestException):
        indexer.run([b"foo"])


def test_directory_indexer_catch_exceptions():
    indexer = CrashingDirectoryIndexer(config=BASE_TEST_CONFIG)
    indexer.storage = Mock()
    indexer.storage.directory_get.return_value = [DIRECTORY2]

    assert indexer.run([b"foo"]) == {"status": "failed"}

    assert indexer.process_journal_objects({"directory": [DIRECTORY2.to_dict()]}) == {
        "status": "failed"
    }

    indexer.catch_exceptions = False

    with pytest.raises(_TestException):
        indexer.run([b"foo"])

    with pytest.raises(_TestException):
        indexer.process_journal_objects({"directory": [DIRECTORY2.to_dict()]})


def test_origin_indexer_catch_exceptions():
    indexer = CrashingOriginIndexer(config=BASE_TEST_CONFIG)

    assert indexer.run(["http://example.org"]) == {"status": "failed"}

    assert indexer.process_journal_objects(
        {"origin": [{"url": "http://example.org"}]}
    ) == {"status": "failed"}

    indexer.catch_exceptions = False

    with pytest.raises(_TestException):
        indexer.run(["http://example.org"])

    with pytest.raises(_TestException):
        indexer.process_journal_objects({"origin": [{"url": "http://example.org"}]})


def test_content_partition_indexer_catch_exceptions():
    indexer = CrashingContentPartitionIndexer(
        config={**BASE_TEST_CONFIG, "write_batch_size": 42}
    )

    assert indexer.run(0, 42) == {"status": "failed"}

    indexer.catch_exceptions = False

    with pytest.raises(_TestException):
        indexer.run(0, 42)


def test_content_partition_indexer():
    # TODO: simplify the mocking in this test
    indexer = TrivialContentPartitionIndexer(
        config={
            **BASE_TEST_CONFIG,
            "write_batch_size": 10,
        }  # doesn't matter
    )
    indexer.catch_exceptions = False
    indexer._results = []
    indexer.storage = Mock()
    indexer.storage.content_get_partition = lambda *args, **kwargs: PagedResult(
        results=[
            Content(sha1=c, sha1_git=c, sha256=c, blake2s256=c, length=42)
            for c in [
                b"hash1",
                b"excluded hash",
                b"hash2",
                b"other excluded hash",
                b"hash3",
            ]
        ],
        next_page_token=None,
    )
    indexer.objstorage = Mock()
    indexer.objstorage.get = lambda id: b"foo"
    nb_partitions = 1
    partition_id = 0
    indexer.run(partition_id, nb_partitions)
    assert indexer._results == [["indexed hash1", "indexed hash2", "indexed hash3"]]
