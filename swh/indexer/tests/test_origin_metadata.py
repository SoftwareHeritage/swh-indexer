# Copyright (C) 2018-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import copy
from unittest.mock import patch

import pytest

from swh.indexer.metadata import OriginMetadataIndexer
from swh.indexer.storage.interface import IndexerStorageInterface
from swh.indexer.storage.model import (
    OriginIntrinsicMetadataRow,
    RevisionIntrinsicMetadataRow,
)
from swh.model.model import Origin
from swh.storage.interface import StorageInterface

from .test_metadata import TRANSLATOR_TOOL
from .utils import REVISION, YARN_PARSER_METADATA


@pytest.fixture
def swh_indexer_config(swh_indexer_config):
    """Override the default configuration to override the tools entry

    """
    cfg = copy.deepcopy(swh_indexer_config)
    cfg["tools"] = TRANSLATOR_TOOL
    return cfg


def test_origin_metadata_indexer(
    swh_indexer_config,
    idx_storage: IndexerStorageInterface,
    storage: StorageInterface,
    obj_storage,
) -> None:
    indexer = OriginMetadataIndexer(config=swh_indexer_config)
    origin = "https://github.com/librariesio/yarn-parser"
    indexer.run([origin])

    tool = swh_indexer_config["tools"]

    rev_id = REVISION.id
    rev_metadata = RevisionIntrinsicMetadataRow(
        id=rev_id, tool=tool, metadata=YARN_PARSER_METADATA, mappings=["npm"],
    )
    origin_metadata = OriginIntrinsicMetadataRow(
        id=origin,
        tool=tool,
        from_revision=rev_id,
        metadata=YARN_PARSER_METADATA,
        mappings=["npm"],
    )

    rev_results = list(idx_storage.revision_intrinsic_metadata_get([rev_id]))
    for rev_result in rev_results:
        assert rev_result.tool
        del rev_result.tool["id"]
    assert rev_results == [rev_metadata]

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
    indexer = OriginMetadataIndexer(config=swh_indexer_config)
    indexer.storage = storage
    indexer.idx_storage = idx_storage
    indexer.run(["https://github.com/librariesio/yarn-parser"])
    indexer.run(["https://github.com/librariesio/yarn-parser"] * 2)

    origin = "https://github.com/librariesio/yarn-parser"
    rev_id = REVISION.id

    rev_results = list(indexer.idx_storage.revision_intrinsic_metadata_get([rev_id]))
    assert len(rev_results) == 1

    orig_results = list(indexer.idx_storage.origin_intrinsic_metadata_get([origin]))
    assert len(orig_results) == 1


def test_origin_metadata_indexer_missing_head(
    swh_indexer_config,
    idx_storage: IndexerStorageInterface,
    storage: StorageInterface,
    obj_storage,
) -> None:
    storage.origin_add([Origin(url="https://example.com")])

    indexer = OriginMetadataIndexer(config=swh_indexer_config)
    indexer.run(["https://example.com"])

    origin = "https://example.com"

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
    storage.origin_add([Origin(url=origin1)])
    indexer = OriginMetadataIndexer(config=swh_indexer_config)
    indexer.run([origin1, origin2])

    rev_id = REVISION.id

    rev_results = list(indexer.idx_storage.revision_intrinsic_metadata_get([rev_id]))
    assert rev_results == [
        RevisionIntrinsicMetadataRow(
            id=rev_id,
            metadata=YARN_PARSER_METADATA,
            mappings=["npm"],
            tool=rev_results[0].tool,
        )
    ]

    orig_results = list(
        indexer.idx_storage.origin_intrinsic_metadata_get([origin1, origin2])
    )
    for orig_result in orig_results:
        assert orig_results == [
            OriginIntrinsicMetadataRow(
                id=origin2,
                from_revision=rev_id,
                metadata=YARN_PARSER_METADATA,
                mappings=["npm"],
                tool=orig_results[0].tool,
            )
        ]


def test_origin_metadata_indexer_duplicate_revision(
    swh_indexer_config,
    idx_storage: IndexerStorageInterface,
    storage: StorageInterface,
    obj_storage,
) -> None:
    indexer = OriginMetadataIndexer(config=swh_indexer_config)
    indexer.storage = storage
    indexer.idx_storage = idx_storage
    indexer.catch_exceptions = False
    origin1 = "https://github.com/librariesio/yarn-parser"
    origin2 = "https://github.com/librariesio/yarn-parser.git"
    indexer.run([origin1, origin2])

    rev_id = REVISION.id

    rev_results = list(indexer.idx_storage.revision_intrinsic_metadata_get([rev_id]))
    assert len(rev_results) == 1

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

    indexer = OriginMetadataIndexer(config=swh_indexer_config)
    origin = "https://github.com/librariesio/yarn-parser"
    with patch("swh.indexer.metadata_dictionary.npm.NpmMapping.filename", b"foo.json"):
        indexer.run([origin])

    rev_id = REVISION.id

    rev_results = list(indexer.idx_storage.revision_intrinsic_metadata_get([rev_id]))
    assert rev_results == []

    orig_results = list(indexer.idx_storage.origin_intrinsic_metadata_get([origin]))
    assert orig_results == []


def test_origin_metadata_indexer_no_metadata(
    swh_indexer_config,
    idx_storage: IndexerStorageInterface,
    storage: StorageInterface,
    obj_storage,
) -> None:

    indexer = OriginMetadataIndexer(config=swh_indexer_config)
    origin = "https://github.com/librariesio/yarn-parser"
    with patch(
        "swh.indexer.metadata.RevisionMetadataIndexer"
        ".translate_revision_intrinsic_metadata",
        return_value=(["npm"], {"@context": "foo"}),
    ):
        indexer.run([origin])

    rev_id = REVISION.id

    rev_results = list(indexer.idx_storage.revision_intrinsic_metadata_get([rev_id]))
    assert rev_results == []

    orig_results = list(indexer.idx_storage.origin_intrinsic_metadata_get([origin]))
    assert orig_results == []


def test_origin_metadata_indexer_error(
    swh_indexer_config,
    idx_storage: IndexerStorageInterface,
    storage: StorageInterface,
    obj_storage,
) -> None:

    indexer = OriginMetadataIndexer(config=swh_indexer_config)
    origin = "https://github.com/librariesio/yarn-parser"
    with patch(
        "swh.indexer.metadata.RevisionMetadataIndexer"
        ".translate_revision_intrinsic_metadata",
        return_value=None,
    ):
        indexer.run([origin])

    rev_id = REVISION.id

    rev_results = list(indexer.idx_storage.revision_intrinsic_metadata_get([rev_id]))
    assert rev_results == []

    orig_results = list(indexer.idx_storage.origin_intrinsic_metadata_get([origin]))
    assert orig_results == []


def test_origin_metadata_indexer_unknown_origin(
    swh_indexer_config,
    idx_storage: IndexerStorageInterface,
    storage: StorageInterface,
    obj_storage,
) -> None:

    indexer = OriginMetadataIndexer(config=swh_indexer_config)
    result = indexer.index_list(["https://unknown.org/foo"])
    assert not result
