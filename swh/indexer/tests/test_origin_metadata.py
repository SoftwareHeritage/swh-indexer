# Copyright (C) 2018-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from unittest.mock import patch

from swh.indexer.metadata import OriginMetadataIndexer
from swh.indexer.storage.interface import IndexerStorageInterface
from swh.indexer.storage.model import (
    OriginIntrinsicMetadataRow,
    RevisionIntrinsicMetadataRow,
)
from swh.model.model import Origin
from swh.storage.interface import StorageInterface

from .test_metadata import REVISION_METADATA_CONFIG
from .utils import REVISION, YARN_PARSER_METADATA


def test_origin_metadata_indexer(
    idx_storage: IndexerStorageInterface, storage: StorageInterface, obj_storage
) -> None:

    indexer = OriginMetadataIndexer(config=REVISION_METADATA_CONFIG)
    origin = "https://github.com/librariesio/yarn-parser"
    indexer.run([origin])

    tool = {
        "name": "swh-metadata-translator",
        "version": "0.0.2",
        "configuration": {"context": "NpmMapping", "type": "local"},
    }

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

    rev_results = list(indexer.idx_storage.revision_intrinsic_metadata_get([rev_id]))
    for rev_result in rev_results:
        assert rev_result.tool
        del rev_result.tool["id"]
    assert rev_results == [rev_metadata]

    orig_results = list(indexer.idx_storage.origin_intrinsic_metadata_get([origin]))
    for orig_result in orig_results:
        assert orig_result.tool
        del orig_result.tool["id"]
    assert orig_results == [origin_metadata]


def test_origin_metadata_indexer_duplicate_origin(
    idx_storage: IndexerStorageInterface, storage: StorageInterface, obj_storage
) -> None:
    indexer = OriginMetadataIndexer(config=REVISION_METADATA_CONFIG)
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
    idx_storage: IndexerStorageInterface, storage: StorageInterface, obj_storage
) -> None:
    storage.origin_add([Origin(url="https://example.com")])

    indexer = OriginMetadataIndexer(config=REVISION_METADATA_CONFIG)
    indexer.run(["https://example.com"])

    origin = "https://example.com"

    results = list(indexer.idx_storage.origin_intrinsic_metadata_get([origin]))
    assert results == []


def test_origin_metadata_indexer_partial_missing_head(
    idx_storage: IndexerStorageInterface, storage: StorageInterface, obj_storage
) -> None:

    origin1 = "https://example.com"
    origin2 = "https://github.com/librariesio/yarn-parser"
    storage.origin_add([Origin(url=origin1)])
    indexer = OriginMetadataIndexer(config=REVISION_METADATA_CONFIG)
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
    idx_storage: IndexerStorageInterface, storage: StorageInterface, obj_storage
) -> None:
    indexer = OriginMetadataIndexer(config=REVISION_METADATA_CONFIG)
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
    idx_storage: IndexerStorageInterface, storage: StorageInterface, obj_storage
) -> None:

    indexer = OriginMetadataIndexer(config=REVISION_METADATA_CONFIG)
    origin = "https://github.com/librariesio/yarn-parser"
    with patch("swh.indexer.metadata_dictionary.npm.NpmMapping.filename", b"foo.json"):
        indexer.run([origin])

    rev_id = REVISION.id

    rev_results = list(indexer.idx_storage.revision_intrinsic_metadata_get([rev_id]))
    assert rev_results == []

    orig_results = list(indexer.idx_storage.origin_intrinsic_metadata_get([origin]))
    assert orig_results == []


def test_origin_metadata_indexer_no_metadata(
    idx_storage: IndexerStorageInterface, storage: StorageInterface, obj_storage
) -> None:

    indexer = OriginMetadataIndexer(config=REVISION_METADATA_CONFIG)
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
    idx_storage: IndexerStorageInterface, storage: StorageInterface, obj_storage
) -> None:

    indexer = OriginMetadataIndexer(config=REVISION_METADATA_CONFIG)
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


def test_origin_metadata_indexer_delete_metadata(
    idx_storage: IndexerStorageInterface, storage: StorageInterface, obj_storage
) -> None:

    indexer = OriginMetadataIndexer(config=REVISION_METADATA_CONFIG)
    origin = "https://github.com/librariesio/yarn-parser"
    indexer.run([origin])

    rev_id = REVISION.id

    rev_results = list(indexer.idx_storage.revision_intrinsic_metadata_get([rev_id]))
    assert rev_results != []

    orig_results = list(indexer.idx_storage.origin_intrinsic_metadata_get([origin]))
    assert orig_results != []

    with patch("swh.indexer.metadata_dictionary.npm.NpmMapping.filename", b"foo.json"):
        indexer.run([origin])

    rev_results = list(indexer.idx_storage.revision_intrinsic_metadata_get([rev_id]))
    assert rev_results == []

    orig_results = list(indexer.idx_storage.origin_intrinsic_metadata_get([origin]))
    assert orig_results == []


def test_origin_metadata_indexer_unknown_origin(
    idx_storage: IndexerStorageInterface, storage: StorageInterface, obj_storage
) -> None:

    indexer = OriginMetadataIndexer(config=REVISION_METADATA_CONFIG)
    result = indexer.index_list(["https://unknown.org/foo"])
    assert not result
