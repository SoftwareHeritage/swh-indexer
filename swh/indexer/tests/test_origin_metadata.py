# Copyright (C) 2018-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import copy
from unittest.mock import patch

import attr
import pytest

from swh.indexer.metadata import OriginIntrinsicMetadataIndexer
from swh.indexer.storage.interface import IndexerStorageInterface
from swh.indexer.storage.model import (
    DirectoryIntrinsicMetadataRow,
    OriginIntrinsicMetadataRow,
)
from swh.model.model import Origin
from swh.storage.interface import StorageInterface

from .test_metadata import TRANSLATOR_TOOL
from .utils import DIRECTORY2, YARN_PARSER_METADATA


@pytest.fixture
def swh_indexer_config(swh_indexer_config):
    """Override the default configuration to override the tools entry"""
    cfg = copy.deepcopy(swh_indexer_config)
    cfg["tools"] = TRANSLATOR_TOOL
    cfg["batch_size"] = {
        "origin": 20,
        "release": 20,
        "revision": 20,
    }
    return cfg


def test_origin_metadata_indexer_release(
    swh_indexer_config,
    idx_storage: IndexerStorageInterface,
    storage: StorageInterface,
    obj_storage,
) -> None:
    indexer = OriginIntrinsicMetadataIndexer(
        config=swh_indexer_config, objstorage=obj_storage
    )
    origin = "https://npm.example.org/yarn-parser"
    ovs = {"origin": origin, "status": "full"}

    indexer.run([ovs])

    tool = swh_indexer_config["tools"]

    dir_id = DIRECTORY2.id
    dir_metadata = DirectoryIntrinsicMetadataRow(
        id=dir_id,
        tool=tool,
        metadata=YARN_PARSER_METADATA,
        mappings=["npm"],
    )
    origin_metadata = OriginIntrinsicMetadataRow(
        id=origin,
        tool=tool,
        from_directory=dir_id,
        metadata=YARN_PARSER_METADATA,
        mappings=["npm"],
    )

    dir_results = list(idx_storage.directory_intrinsic_metadata_get([dir_id]))
    for dir_result in dir_results:
        assert dir_result.tool
        del dir_result.tool["id"]
    assert dir_results == [dir_metadata]

    orig_results = list(idx_storage.origin_intrinsic_metadata_get([origin]))
    for orig_result in orig_results:
        assert orig_result.tool
        del orig_result.tool["id"]
    assert orig_results == [origin_metadata]


def test_origin_metadata_indexer_revision(
    swh_indexer_config,
    idx_storage: IndexerStorageInterface,
    storage: StorageInterface,
    obj_storage,
) -> None:
    indexer = OriginIntrinsicMetadataIndexer(
        config=swh_indexer_config, objstorage=obj_storage
    )

    origin = "https://github.com/librariesio/yarn-parser"
    ovs = {
        "origin": origin,
        "status": "full",
    }

    indexer.run([ovs])

    tool = swh_indexer_config["tools"]

    dir_id = DIRECTORY2.id
    dir_metadata = DirectoryIntrinsicMetadataRow(
        id=dir_id,
        tool=tool,
        metadata=YARN_PARSER_METADATA,
        mappings=["npm"],
    )
    origin_metadata = OriginIntrinsicMetadataRow(
        id=origin,
        tool=tool,
        from_directory=dir_id,
        metadata=YARN_PARSER_METADATA,
        mappings=["npm"],
    )

    dir_results = list(idx_storage.directory_intrinsic_metadata_get([dir_id]))
    for dir_result in dir_results:
        assert dir_result.tool
        del dir_result.tool["id"]
    assert dir_results == [dir_metadata]

    orig_results = list(idx_storage.origin_intrinsic_metadata_get([origin]))
    for orig_result in orig_results:
        assert orig_result.tool
        del orig_result.tool["id"]
    assert orig_results == [origin_metadata]


def test_origin_metadata_indexer_duplicate_origin(
    swh_indexer_config,
    idx_storage: IndexerStorageInterface,
    storage: StorageInterface,
    obj_storage,
) -> None:
    indexer = OriginIntrinsicMetadataIndexer(
        config=swh_indexer_config, objstorage=obj_storage
    )
    indexer.storage = storage
    indexer.idx_storage = idx_storage

    origin = "https://github.com/librariesio/yarn-parser"
    ovs = {
        "origin": origin,
        "status": "full",
    }

    indexer.run([ovs])
    indexer.run([ovs] * 2)

    dir_id = DIRECTORY2.id

    dir_results = list(indexer.idx_storage.directory_intrinsic_metadata_get([dir_id]))
    assert len(dir_results) == 1

    orig_results = list(indexer.idx_storage.origin_intrinsic_metadata_get([origin]))
    assert len(orig_results) == 1


def test_origin_metadata_indexer_missing_head(
    swh_indexer_config,
    idx_storage: IndexerStorageInterface,
    storage: StorageInterface,
    obj_storage,
) -> None:
    origin = "https://example.com"
    origin_d = {"url": origin}

    storage.origin_add([Origin.from_dict(origin_d)])

    indexer = OriginIntrinsicMetadataIndexer(
        config=swh_indexer_config, objstorage=obj_storage
    )

    ovs = {"origin": origin, "status": "full"}
    indexer.run([ovs])

    results = list(indexer.idx_storage.origin_intrinsic_metadata_get([origin]))
    assert results == []


def test_origin_metadata_indexer_partial_missing_head(
    swh_indexer_config,
    idx_storage: IndexerStorageInterface,
    storage: StorageInterface,
    obj_storage,
) -> None:
    origin1 = "https://example.com"
    origin2 = "https://github.com/librariesio/yarn-parser"

    # We are creating only one origin so the other head is missing when indexing for the
    # origin2
    origin1_d = {"url": origin1}
    storage.origin_add([Origin.from_dict(origin1_d)])

    indexer = OriginIntrinsicMetadataIndexer(
        config=swh_indexer_config, objstorage=obj_storage
    )

    ovs1 = {"origin": origin1, "status": "full"}
    ovs2 = {"origin": origin2, "status": "full"}

    indexer.run([ovs1, ovs2])

    dir_id = DIRECTORY2.id

    dir_results = list(indexer.idx_storage.directory_intrinsic_metadata_get([dir_id]))
    assert dir_results == [
        DirectoryIntrinsicMetadataRow(
            id=dir_id,
            metadata=YARN_PARSER_METADATA,
            mappings=["npm"],
            tool=dir_results[0].tool,
        )
    ]

    orig_results = list(
        indexer.idx_storage.origin_intrinsic_metadata_get([origin1, origin2])
    )
    for orig_result in orig_results:
        assert orig_results == [
            OriginIntrinsicMetadataRow(
                id=origin2,
                from_directory=dir_id,
                metadata=YARN_PARSER_METADATA,
                mappings=["npm"],
                tool=orig_results[0].tool,
            )
        ]


def test_origin_metadata_indexer_duplicate_directory(
    swh_indexer_config,
    idx_storage: IndexerStorageInterface,
    storage: StorageInterface,
    obj_storage,
) -> None:
    indexer = OriginIntrinsicMetadataIndexer(
        config=swh_indexer_config, objstorage=obj_storage
    )
    indexer.storage = storage
    indexer.idx_storage = idx_storage
    indexer.catch_exceptions = False

    origin1 = "https://github.com/librariesio/yarn-parser"
    origin2 = "https://github.com/librariesio/yarn-parser.git"

    origin1_d = {"origin": origin1, "status": "full"}
    origin2_d = {"origin": origin2, "status": "full"}

    indexer.run([origin1_d, origin2_d])

    dir_id = DIRECTORY2.id

    dir_results = list(indexer.idx_storage.directory_intrinsic_metadata_get([dir_id]))
    assert len(dir_results) == 1

    orig_results = list(
        indexer.idx_storage.origin_intrinsic_metadata_get([origin1, origin2])
    )
    assert len(orig_results) == 2


def test_origin_metadata_indexer_duplicate_directory_different_result(
    swh_indexer_config,
    idx_storage: IndexerStorageInterface,
    storage: StorageInterface,
    obj_storage,
    mocker,
) -> None:
    """Same as above, but indexing the same directory twice resulted in different
    data (because list order differs).
    """
    indexer = OriginIntrinsicMetadataIndexer(
        config=swh_indexer_config, objstorage=obj_storage
    )
    indexer.storage = storage
    indexer.idx_storage = idx_storage
    indexer.catch_exceptions = False
    origin1 = "https://github.com/librariesio/yarn-parser"
    origin2 = "https://github.com/librariesio/yarn-parser.git"

    ovs1 = {"origin": origin1, "status": "full"}
    ovs2 = {"origin": origin2, "status": "full"}

    directory_index = indexer.directory_metadata_indexer.index

    nb_calls = 0

    def side_effect(dir_id):
        nonlocal nb_calls
        if nb_calls == 0:
            keywords = ["foo", "bar"]
        elif nb_calls == 1:
            keywords = ["bar", "foo"]
        else:
            assert False, nb_calls
        nb_calls += 1
        return [
            attr.evolve(row, metadata={**row.metadata, "keywords": keywords})
            for row in directory_index(dir_id)
        ]

    mocker.patch.object(
        indexer.directory_metadata_indexer, "index", side_effect=side_effect
    )

    indexer.run([ovs1, ovs2])

    dir_id = DIRECTORY2.id

    dir_results = list(indexer.idx_storage.directory_intrinsic_metadata_get([dir_id]))
    assert len(dir_results) == 1

    orig_results = list(
        indexer.idx_storage.origin_intrinsic_metadata_get([origin1, origin2])
    )
    assert len(orig_results) == 2


def test_origin_metadata_indexer_no_metadata_file(
    swh_indexer_config,
    idx_storage: IndexerStorageInterface,
    storage: StorageInterface,
    obj_storage,
) -> None:
    indexer = OriginIntrinsicMetadataIndexer(
        config=swh_indexer_config, objstorage=obj_storage
    )
    origin = "https://github.com/librariesio/yarn-parser"
    ovs = {
        "origin": origin,
        "status": "full",
    }

    with patch("swh.indexer.metadata_mapping.npm.NpmMapping.filename", b"foo.json"):
        indexer.run([ovs])

    dir_id = DIRECTORY2.id

    dir_results = list(indexer.idx_storage.directory_intrinsic_metadata_get([dir_id]))
    assert dir_results == []

    orig_results = list(indexer.idx_storage.origin_intrinsic_metadata_get([origin]))
    assert orig_results == []


def test_origin_metadata_indexer_no_metadata(
    swh_indexer_config,
    idx_storage: IndexerStorageInterface,
    storage: StorageInterface,
    obj_storage,
) -> None:
    indexer = OriginIntrinsicMetadataIndexer(
        config=swh_indexer_config, objstorage=obj_storage
    )
    origin = "https://github.com/librariesio/yarn-parser"
    ovs = {
        "origin": origin,
        "status": "full",
    }

    with patch(
        "swh.indexer.metadata.DirectoryMetadataIndexer"
        ".translate_directory_intrinsic_metadata",
        return_value=(["npm"], {"@context": "foo"}),
    ):
        indexer.run([ovs])

    dir_id = DIRECTORY2.id

    dir_results = list(indexer.idx_storage.directory_intrinsic_metadata_get([dir_id]))
    assert dir_results == []

    orig_results = list(indexer.idx_storage.origin_intrinsic_metadata_get([origin]))
    assert orig_results == []


@pytest.mark.parametrize("catch_exceptions", [True, False])
def test_origin_metadata_indexer_directory_error(
    swh_indexer_config,
    idx_storage: IndexerStorageInterface,
    storage: StorageInterface,
    obj_storage,
    sentry_events,
    catch_exceptions,
) -> None:
    indexer = OriginIntrinsicMetadataIndexer(
        config=swh_indexer_config, objstorage=obj_storage
    )
    origin = "https://github.com/librariesio/yarn-parser"
    ovs = {
        "origin": origin,
        "status": "full",
    }

    indexer.catch_exceptions = catch_exceptions

    with patch(
        "swh.indexer.metadata.DirectoryMetadataIndexer"
        ".translate_directory_intrinsic_metadata",
        return_value=None,
    ):
        indexer.run([ovs])

    assert len(sentry_events) == 1
    sentry_event = sentry_events.pop()
    assert sentry_event.get("tags") == {
        "swh-indexer-origin-head-swhid": (
            "swh:1:rev:c926fcf9506eaa8c38b51d42a55de4aae214aed7"
        ),
        "swh-indexer-origin-url": origin,
    }
    assert "'TypeError'" in str(sentry_event)

    dir_id = DIRECTORY2.id

    dir_results = list(indexer.idx_storage.directory_intrinsic_metadata_get([dir_id]))
    assert dir_results == []

    orig_results = list(indexer.idx_storage.origin_intrinsic_metadata_get([origin]))
    assert orig_results == []


@pytest.mark.parametrize("catch_exceptions", [True, False])
def test_origin_metadata_indexer_content_exception(
    swh_indexer_config,
    idx_storage: IndexerStorageInterface,
    storage: StorageInterface,
    obj_storage,
    sentry_events,
    catch_exceptions,
) -> None:
    indexer = OriginIntrinsicMetadataIndexer(
        config=swh_indexer_config, objstorage=obj_storage
    )
    origin = "https://github.com/librariesio/yarn-parser"
    ovs = {
        "origin": origin,
        "status": "full",
    }

    indexer.catch_exceptions = catch_exceptions

    class TestException(Exception):
        pass

    with patch(
        "swh.indexer.metadata.ContentMetadataRow",
        side_effect=TestException(),
    ):
        indexer.run([ovs])

    assert len(sentry_events) == 1
    sentry_event = sentry_events.pop()
    assert sentry_event.get("tags") == {
        "swh-indexer-content-sha1": "d8f93b069a4acaf38e9e170f5314be07eaa0ae32",
        "swh-indexer-origin-head-swhid": (
            "swh:1:rev:c926fcf9506eaa8c38b51d42a55de4aae214aed7"
        ),
        "swh-indexer-origin-url": origin,
    }
    assert ".TestException'" in str(sentry_event), sentry_event

    dir_id = DIRECTORY2.id

    dir_results = list(indexer.idx_storage.directory_intrinsic_metadata_get([dir_id]))
    assert dir_results == []

    orig_results = list(indexer.idx_storage.origin_intrinsic_metadata_get([origin]))
    assert orig_results == []


def test_origin_metadata_indexer_unknown_origin(
    swh_indexer_config,
    idx_storage: IndexerStorageInterface,
    storage: StorageInterface,
    obj_storage,
) -> None:
    indexer = OriginIntrinsicMetadataIndexer(
        config=swh_indexer_config, objstorage=obj_storage
    )
    result = indexer.index_list([Origin("https://unknown.org/foo")])
    assert not result
