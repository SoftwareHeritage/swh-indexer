# Copyright (C) 2020-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Any, Dict, Iterable, List, Optional
from unittest.mock import Mock

import pytest
import sentry_sdk

from swh.indexer.indexer import ContentIndexer, DirectoryIndexer, OriginIndexer
from swh.indexer.storage import Sha1

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


class CrashingDirectoryIndexer(CrashingIndexerMixin, DirectoryIndexer):
    pass


class CrashingOriginIndexer(CrashingIndexerMixin, OriginIndexer):
    pass


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
