# Copyright (C) 2020-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Any, Dict, Iterable, List, Optional
from unittest.mock import Mock

import pytest
import sentry_sdk

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


def check_sentry(sentry_events, tags):
    assert len(sentry_events) == 1
    sentry_event = sentry_events.pop()
    assert sentry_event.get("tags") == tags
    assert "'_TestException'" in str(sentry_event)


def test_content_indexer_catch_exceptions(sentry_events):
    indexer = CrashingContentIndexer(config=BASE_TEST_CONFIG)
    indexer.objstorage = Mock()
    indexer.objstorage.get.return_value = b"content"
    indexer.objstorage.get_batch.return_value = [b"content"]

    sha1 = b"\x12" * 20

    # As task, catching exceptions
    assert indexer.run([sha1]) == {"status": "failed"}
    check_sentry(sentry_events, {"swh-indexer-content-sha1": sha1.hex()})

    # As journal client, catching exceptions
    assert indexer.process_journal_objects({"content": [{"sha1": sha1}]}) == {
        "status": "failed"
    }
    check_sentry(sentry_events, {"swh-indexer-content-sha1": sha1.hex()})

    indexer.catch_exceptions = False

    # As task, not catching exceptions
    with pytest.raises(_TestException):
        indexer.run([sha1])
    assert sentry_events == []

    # As journal client, not catching exceptions
    with pytest.raises(_TestException):
        indexer.process_journal_objects({"content": [{"sha1": sha1}]})
    assert sentry_events == []

    # As journal client, check the frontend will be able to get the tag when reporting
    try:
        indexer.process_journal_objects({"content": [{"sha1": sha1}]})
    except Exception:
        sentry_sdk.capture_exception()
    else:
        assert False
    check_sentry(sentry_events, {"swh-indexer-content-sha1": sha1.hex()})


def test_directory_indexer_catch_exceptions(sentry_events):
    indexer = CrashingDirectoryIndexer(config=BASE_TEST_CONFIG)
    indexer.storage = Mock()
    indexer.storage.directory_get.return_value = [DIRECTORY2]

    sha1 = DIRECTORY2.id
    swhid = str(DIRECTORY2.swhid())

    # As task, catching exceptions
    assert indexer.run([sha1]) == {"status": "failed"}
    check_sentry(sentry_events, {"swh-indexer-directory-swhid": swhid})

    # As journal client, catching exceptions
    assert indexer.process_journal_objects({"directory": [DIRECTORY2.to_dict()]}) == {
        "status": "failed"
    }
    check_sentry(sentry_events, {"swh-indexer-directory-swhid": swhid})

    indexer.catch_exceptions = False

    # As task, not catching exceptions
    with pytest.raises(_TestException):
        indexer.run([b"foo"])
    assert sentry_events == []

    # As journal client, not catching exceptions
    with pytest.raises(_TestException):
        indexer.process_journal_objects({"directory": [DIRECTORY2.to_dict()]})
    assert sentry_events == []

    # As journal client, check the frontend will be able to get the tag when reporting
    try:
        indexer.process_journal_objects({"directory": [DIRECTORY2.to_dict()]})
    except Exception:
        sentry_sdk.capture_exception()
    else:
        assert False
    check_sentry(sentry_events, {"swh-indexer-directory-swhid": swhid})


def test_origin_indexer_catch_exceptions(sentry_events):
    indexer = CrashingOriginIndexer(config=BASE_TEST_CONFIG)

    origin_url = "http://example.org"

    # As task, catching exceptions
    assert indexer.run([origin_url]) == {"status": "failed"}
    check_sentry(sentry_events, {"swh-indexer-origin-url": origin_url})

    # As journal client, catching exceptions
    assert indexer.process_journal_objects({"origin": [{"url": origin_url}]}) == {
        "status": "failed"
    }
    check_sentry(sentry_events, {"swh-indexer-origin-url": origin_url})

    indexer.catch_exceptions = False

    # As task, not catching exceptions
    with pytest.raises(_TestException):
        indexer.run([origin_url])
    assert sentry_events == []

    # As journal client, not catching exceptions
    with pytest.raises(_TestException):
        indexer.process_journal_objects({"origin": [{"url": origin_url}]})
    assert sentry_events == []

    # As journal client, check the frontend will be able to get the tag when reporting
    try:
        indexer.process_journal_objects({"origin": [{"url": origin_url}]})
    except Exception:
        sentry_sdk.capture_exception()
    else:
        assert False
    check_sentry(sentry_events, {"swh-indexer-origin-url": origin_url})


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
