# Copyright (C) 2017-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
from unittest.mock import call

import attr

from swh.indexer.metadata import (
    ContentMetadataIndexer,
    DirectoryMetadataIndexer,
    ExtrinsicMetadataIndexer,
)
from swh.indexer.storage.model import (
    ContentMetadataRow,
    DirectoryIntrinsicMetadataRow,
    OriginExtrinsicMetadataRow,
)
from swh.indexer.tests.utils import DIRECTORY2
from swh.model.model import (
    Directory,
    DirectoryEntry,
    MetadataAuthority,
    MetadataAuthorityType,
    MetadataFetcher,
    RawExtrinsicMetadata,
)
from swh.model.swhids import ExtendedObjectType, ExtendedSWHID

from .utils import (
    BASE_TEST_CONFIG,
    YARN_PARSER_METADATA,
    fill_obj_storage,
    fill_storage,
)

TRANSLATOR_TOOL = {
    "name": "swh-metadata-translator",
    "version": "0.0.2",
    "configuration": {"type": "local", "context": "NpmMapping"},
}


class ContentMetadataTestIndexer(ContentMetadataIndexer):
    """Specific Metadata whose configuration is enough to satisfy the
    indexing tests.
    """

    def parse_config_file(self, *args, **kwargs):
        assert False, "should not be called; the dir indexer configures it."


DIRECTORY_METADATA_CONFIG = {
    **BASE_TEST_CONFIG,
    "tools": TRANSLATOR_TOOL,
}

REMD = RawExtrinsicMetadata(
    target=ExtendedSWHID(
        object_type=ExtendedObjectType.ORIGIN,
        object_id=b"\x01" * 20,
    ),
    discovery_date=datetime.datetime.now(tz=datetime.timezone.utc),
    authority=MetadataAuthority(
        type=MetadataAuthorityType.FORGE,
        url="https://example.org/",
    ),
    fetcher=MetadataFetcher(
        name="example-fetcher",
        version="1.0.0",
    ),
    format="application/vnd.github.v3+json",
    metadata=b'{"full_name": "test software"}',
)


class TestMetadata:
    """
    Tests metadata_mock_tool tool for Metadata detection
    """

    def test_directory_metadata_indexer(self):
        metadata_indexer = DirectoryMetadataIndexer(config=DIRECTORY_METADATA_CONFIG)
        fill_obj_storage(metadata_indexer.objstorage)
        fill_storage(metadata_indexer.storage)

        tool = metadata_indexer.idx_storage.indexer_configuration_get(
            {f"tool_{k}": v for (k, v) in TRANSLATOR_TOOL.items()}
        )
        assert tool is not None
        dir_ = DIRECTORY2

        metadata_indexer.idx_storage.content_metadata_add(
            [
                ContentMetadataRow(
                    id=DIRECTORY2.entries[0].target,
                    indexer_configuration_id=tool["id"],
                    metadata=YARN_PARSER_METADATA,
                )
            ]
        )

        metadata_indexer.run([dir_.id])

        results = list(
            metadata_indexer.idx_storage.directory_intrinsic_metadata_get(
                [DIRECTORY2.id]
            )
        )

        expected_results = [
            DirectoryIntrinsicMetadataRow(
                id=dir_.id,
                tool=TRANSLATOR_TOOL,
                metadata=YARN_PARSER_METADATA,
                mappings=["npm"],
            )
        ]

        for result in results:
            del result.tool["id"]

        assert results == expected_results

    def test_directory_metadata_indexer_single_root_dir(self):
        metadata_indexer = DirectoryMetadataIndexer(config=DIRECTORY_METADATA_CONFIG)
        fill_obj_storage(metadata_indexer.objstorage)
        fill_storage(metadata_indexer.storage)

        # Add a parent directory, that is the only directory at the root
        # of the directory
        dir_ = DIRECTORY2

        new_dir = Directory(
            entries=(
                DirectoryEntry(
                    name=b"foobar-1.0.0",
                    type="dir",
                    target=dir_.id,
                    perms=16384,
                ),
            ),
        )
        assert new_dir.id is not None
        metadata_indexer.storage.directory_add([new_dir])

        tool = metadata_indexer.idx_storage.indexer_configuration_get(
            {f"tool_{k}": v for (k, v) in TRANSLATOR_TOOL.items()}
        )
        assert tool is not None

        metadata_indexer.idx_storage.content_metadata_add(
            [
                ContentMetadataRow(
                    id=DIRECTORY2.entries[0].target,
                    indexer_configuration_id=tool["id"],
                    metadata=YARN_PARSER_METADATA,
                )
            ]
        )

        metadata_indexer.run([new_dir.id])

        results = list(
            metadata_indexer.idx_storage.directory_intrinsic_metadata_get([new_dir.id])
        )

        expected_results = [
            DirectoryIntrinsicMetadataRow(
                id=new_dir.id,
                tool=TRANSLATOR_TOOL,
                metadata=YARN_PARSER_METADATA,
                mappings=["npm"],
            )
        ]

        for result in results:
            del result.tool["id"]

        assert results == expected_results

    def test_extrinsic_metadata_indexer_unknown_format(self, mocker):
        """Should be ignored when unknown format"""
        metadata_indexer = ExtrinsicMetadataIndexer(config=DIRECTORY_METADATA_CONFIG)
        metadata_indexer.storage = mocker.patch.object(metadata_indexer, "storage")

        remd = attr.evolve(REMD, format="unknown format")

        results = metadata_indexer.index(remd.id, data=remd)

        assert metadata_indexer.storage.method_calls == []
        assert results == []

    def test_extrinsic_metadata_indexer_github(self, mocker):
        """Nominal case, calling the mapping and storing the result"""
        origin = "https://example.org/jdoe/myrepo"

        metadata_indexer = ExtrinsicMetadataIndexer(config=DIRECTORY_METADATA_CONFIG)
        metadata_indexer.catch_exceptions = False
        metadata_indexer.storage = mocker.patch.object(metadata_indexer, "storage")
        metadata_indexer.storage.origin_get_by_sha1.return_value = [{"url": origin}]

        tool = metadata_indexer.idx_storage.indexer_configuration_get(
            {f"tool_{k}": v for (k, v) in TRANSLATOR_TOOL.items()}
        )
        assert tool is not None

        assert metadata_indexer.process_journal_objects(
            {"raw_extrinsic_metadata": [REMD.to_dict()]}
        ) == {"status": "eventful", "origin_extrinsic_metadata:add": 1}

        assert metadata_indexer.storage.method_calls == [
            call.origin_get_by_sha1([b"\x01" * 20])
        ]

        results = list(
            metadata_indexer.idx_storage.origin_extrinsic_metadata_get([origin])
        )
        assert results == [
            OriginExtrinsicMetadataRow(
                id="https://example.org/jdoe/myrepo",
                tool={"id": tool["id"], **TRANSLATOR_TOOL},
                metadata={
                    "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
                    "type": "https://forgefed.org/ns#Repository",
                    "name": "test software",
                },
                from_remd_id=REMD.id,
                mappings=["GitHubMapping"],
            )
        ]

    def test_extrinsic_metadata_indexer_nonforge_authority(self, mocker):
        """Early abort on non-forge authorities"""
        metadata_indexer = ExtrinsicMetadataIndexer(config=DIRECTORY_METADATA_CONFIG)
        metadata_indexer.storage = mocker.patch.object(metadata_indexer, "storage")

        remd = attr.evolve(
            REMD,
            authority=attr.evolve(REMD.authority, type=MetadataAuthorityType.REGISTRY),
        )

        results = metadata_indexer.index(remd.id, data=remd)

        assert metadata_indexer.storage.method_calls == []
        assert results == []

    def test_extrinsic_metadata_indexer_thirdparty_authority(self, mocker):
        """Should be ignored when authority URL does not match the origin"""

        origin = "https://different-domain.example.org/jdoe/myrepo"

        metadata_indexer = ExtrinsicMetadataIndexer(config=DIRECTORY_METADATA_CONFIG)
        metadata_indexer.catch_exceptions = False
        metadata_indexer.storage = mocker.patch.object(metadata_indexer, "storage")
        metadata_indexer.storage.origin_get_by_sha1.return_value = [{"url": origin}]

        tool = metadata_indexer.idx_storage.indexer_configuration_get(
            {f"tool_{k}": v for (k, v) in TRANSLATOR_TOOL.items()}
        )
        assert tool is not None

        results = metadata_indexer.index(REMD.id, data=REMD)

        assert metadata_indexer.storage.method_calls == [
            call.origin_get_by_sha1([b"\x01" * 20])
        ]
        assert results == []
