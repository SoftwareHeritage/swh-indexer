# Copyright (C) 2020-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Any, Dict, Iterable, List, Optional
from unittest.mock import Mock

import pytest

from swh.indexer import get_indexer, get_indexer_names
from swh.indexer.indexer import (
    BaseIndexer,
    ContentIndexer,
    DirectoryIndexer,
    OriginIndexer,
)
from swh.indexer.storage import Sha1
from swh.model.hashutil import HashDict
from swh.objstorage.backends.in_memory import InMemoryObjStorage

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


class TestIndexer(CrashingIndexerMixin, BaseIndexer):
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
    assert indexer.run([HashDict(sha1=sha1)]) == ({"status": "failed"}, [])
    check_sentry(sentry_events, {"swh-indexer-content-sha1": sha1.hex()})

    # As task, not catching exceptions
    with pytest.raises(_TestException):
        indexer.catch_exceptions = False
        indexer.run([HashDict(sha1=sha1)])
    assert sentry_events == []


def test_directory_indexer_catch_exceptions(sentry_events):
    indexer = CrashingDirectoryIndexer(config=BASE_TEST_CONFIG)
    indexer.storage = Mock()
    indexer.storage.directory_get.return_value = [DIRECTORY2]

    swhid = str(DIRECTORY2.swhid())

    # As task, catching exceptions
    assert indexer.run([DIRECTORY2.to_dict()]) == ({"status": "failed"}, [])
    check_sentry(sentry_events, {"swh-indexer-directory-swhid": swhid})

    # As task, not catching exceptions
    with pytest.raises(_TestException):
        indexer.catch_exceptions = False
        indexer.run([DIRECTORY2.to_dict()])
    assert sentry_events == []


def test_origin_indexer_catch_exceptions(sentry_events):
    indexer = CrashingOriginIndexer(config=BASE_TEST_CONFIG)

    origin_url = "http://example.org"

    origin = {"origin": origin_url, "status": "full"}
    # As task, catching exceptions
    assert indexer.run([origin]) == ({"status": "failed"}, [])
    check_sentry(sentry_events, {"swh-indexer-origin-url": origin_url})

    # As task, not catching exceptions
    with pytest.raises(_TestException):
        indexer.catch_exceptions = False
        indexer.run([origin])
    assert sentry_events == []


def test_indexers_define_object_types(swh_config):
    """Indexer class should declare their object_types to subscribe to."""
    available_indexers = get_indexer_names()

    for indexer in available_indexers:
        indexer_class = get_indexer(indexer)()
        assert hasattr(
            indexer_class, "object_types"
        ), f"Indexer {indexer_class} should declare its class attribute `object_types`"
        object_types = getattr(indexer_class, "object_types")

        assert object_types != [], (
            f"Indexer class {indexer_class} should declare a non-empty"
            " `object_types` class attribute"
        )


def test_instantiate_objstorage_for_indexer():
    # To bullet proof against change in that test configuration (adapt accordingly this
    # test if this changes)
    assert BASE_TEST_CONFIG["objstorage"]["cls"] == "memory"
    # Basic instantiation of indexer will just instantiate objstorage
    indexer = TestIndexer(config=BASE_TEST_CONFIG)

    assert hasattr(indexer, "objstorage")
    assert isinstance(indexer.objstorage, InMemoryObjStorage)

    # Instantiation with objstorage passed along
    indexer = TestIndexer(config=BASE_TEST_CONFIG, objstorage=Mock())

    assert hasattr(indexer, "objstorage")
    assert isinstance(indexer.objstorage, Mock)
