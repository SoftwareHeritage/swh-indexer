# Copyright (C) 2017-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import hashlib
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
    MAPPING_DESCRIPTION_CONTENT_SHA1,
    MAPPING_DESCRIPTION_CONTENT_SHA1GIT,
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

DEPOSIT_REMD = RawExtrinsicMetadata(
    target=ExtendedSWHID(
        object_type=ExtendedObjectType.DIRECTORY,
        object_id=b"\x02" * 20,
    ),
    discovery_date=datetime.datetime.now(tz=datetime.timezone.utc),
    authority=MetadataAuthority(
        type=MetadataAuthorityType.DEPOSIT_CLIENT,
        url="https://example.org/",
    ),
    fetcher=MetadataFetcher(
        name="example-fetcher",
        version="1.0.0",
    ),
    format="sword-v2-atom-codemeta-v2",
    metadata="""<?xml version="1.0"?>
        <atom:entry xmlns:atom="http://www.w3.org/2005/Atom"
                    xmlns="https://doi.org/10.5063/schema/codemeta-2.0">
          <name>My Software</name>
          <author>
            <name>Author 1</name>
            <email>foo@example.org</email>
          </author>
          <author>
            <name>Author 2</name>
          </author>
        </atom:entry>
    """.encode(),
    origin="https://example.org/jdoe/myrepo",
)

GITHUB_REMD = RawExtrinsicMetadata(
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
    metadata=b'{"full_name": "test software", "html_url": "http://example.org/"}',
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

        assert (
            dir_.entries[0].target
            == MAPPING_DESCRIPTION_CONTENT_SHA1GIT["json:yarn-parser-package.json"]
        )

        metadata_indexer.idx_storage.content_metadata_add(
            [
                ContentMetadataRow(
                    id=MAPPING_DESCRIPTION_CONTENT_SHA1[
                        "json:yarn-parser-package.json"
                    ],
                    indexer_configuration_id=tool["id"],
                    metadata=YARN_PARSER_METADATA,
                )
            ]
        )

        metadata_indexer.run([dir_.id])

        results = list(
            metadata_indexer.idx_storage.directory_intrinsic_metadata_get([dir_.id])
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
        assert (
            dir_.entries[0].target
            == MAPPING_DESCRIPTION_CONTENT_SHA1GIT["json:yarn-parser-package.json"]
        )

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
                    id=MAPPING_DESCRIPTION_CONTENT_SHA1[
                        "json:yarn-parser-package.json"
                    ],
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

        remd = attr.evolve(GITHUB_REMD, format="unknown format")

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
            {"raw_extrinsic_metadata": [GITHUB_REMD.to_dict()]}
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
                    "id": "http://example.org/",
                    "type": "https://forgefed.org/ns#Repository",
                    "name": "test software",
                },
                from_remd_id=GITHUB_REMD.id,
                mappings=["github"],
            )
        ]

    def test_extrinsic_metadata_indexer_firstparty_deposit(self, mocker):
        """Also nominal case, calling the mapping and storing the result"""
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
            {"raw_extrinsic_metadata": [DEPOSIT_REMD.to_dict()]}
        ) == {"status": "eventful", "origin_extrinsic_metadata:add": 1}

        assert metadata_indexer.storage.method_calls == [
            call.origin_get_by_sha1(
                [b"\xb1\x0c\\\xd2w\x1b\xdd\xac\x07\xdb\xdf>\x93O1\xd0\xc9L\x0c\xcf"]
            )
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
                    "author": [
                        {"email": "foo@example.org", "name": "Author 1"},
                        {"name": "Author 2"},
                    ],
                    "name": "My Software",
                },
                from_remd_id=DEPOSIT_REMD.id,
                mappings=["sword-codemeta"],
            )
        ]

    def test_extrinsic_metadata_indexer_thirdparty_deposit(self, mocker):
        """Metadata-only deposit: currently ignored"""
        origin = "https://not-from-example.org/jdoe/myrepo"

        metadata_indexer = ExtrinsicMetadataIndexer(config=DIRECTORY_METADATA_CONFIG)
        metadata_indexer.catch_exceptions = False
        metadata_indexer.storage = mocker.patch.object(metadata_indexer, "storage")
        metadata_indexer.storage.origin_get_by_sha1.return_value = [{"url": origin}]

        tool = metadata_indexer.idx_storage.indexer_configuration_get(
            {f"tool_{k}": v for (k, v) in TRANSLATOR_TOOL.items()}
        )
        assert tool is not None

        assert metadata_indexer.process_journal_objects(
            {"raw_extrinsic_metadata": [DEPOSIT_REMD.to_dict()]}
        ) == {"status": "uneventful", "origin_extrinsic_metadata:add": 0}

        assert metadata_indexer.storage.method_calls == [
            call.origin_get_by_sha1(
                [b"\xb1\x0c\\\xd2w\x1b\xdd\xac\x07\xdb\xdf>\x93O1\xd0\xc9L\x0c\xcf"]
            )
        ]

        results = list(
            metadata_indexer.idx_storage.origin_extrinsic_metadata_get([origin])
        )
        assert results == []

    def test_extrinsic_metadata_indexer_thirdparty_deposit_unescaped_origin(
        self, mocker
    ):
        """Tests the workaround for REMD objects created from `incorrectly parsing SWHID
        <https://gitlab.softwareheritage.org/swh/devel/swh-model/-/merge_requests/348>`_
        """
        origin = "https://cran.r-project.org/package=airGR"

        metadata_indexer = ExtrinsicMetadataIndexer(config=DIRECTORY_METADATA_CONFIG)
        metadata_indexer.catch_exceptions = False
        metadata_indexer.storage = mocker.patch.object(metadata_indexer, "storage")
        metadata_indexer.storage.origin_get_by_sha1.return_value = [{"url": origin}]

        tool = metadata_indexer.idx_storage.indexer_configuration_get(
            {f"tool_{k}": v for (k, v) in TRANSLATOR_TOOL.items()}
        )
        assert tool is not None

        assert metadata_indexer.process_journal_objects(
            {
                "raw_extrinsic_metadata": [
                    attr.evolve(
                        DEPOSIT_REMD,
                        fetcher=attr.evolve(
                            DEPOSIT_REMD.fetcher,
                            name="swh-deposit",
                        ),
                        origin="https://cran.r-project.org/package%3DairGR",
                        discovery_date=datetime.datetime(
                            2024, 5, 13, 8, 4, 8, tzinfo=datetime.timezone.utc
                        ),
                    ).to_dict(),
                ]
            }
        ) == {"status": "uneventful", "origin_extrinsic_metadata:add": 0}

        assert metadata_indexer.storage.method_calls == [
            call.origin_get_by_sha1([hashlib.sha1(origin.encode()).digest()])
        ]

        results = list(
            metadata_indexer.idx_storage.origin_extrinsic_metadata_get([origin])
        )
        assert results == []

    def test_extrinsic_metadata_indexer_nonforge_authority(self, mocker):
        """Early abort on non-forge authorities"""
        metadata_indexer = ExtrinsicMetadataIndexer(config=DIRECTORY_METADATA_CONFIG)
        metadata_indexer.storage = mocker.patch.object(metadata_indexer, "storage")

        remd = attr.evolve(
            GITHUB_REMD,
            authority=attr.evolve(
                GITHUB_REMD.authority, type=MetadataAuthorityType.REGISTRY
            ),
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

        results = metadata_indexer.index(GITHUB_REMD.id, data=GITHUB_REMD)

        assert metadata_indexer.storage.method_calls == [
            call.origin_get_by_sha1([b"\x01" * 20])
        ]
        assert results == []

    def test_extrinsic_metadata_indexer_duplicate_origin(self, mocker):
        """Two metadata objects with the same origin target"""
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
            {
                "raw_extrinsic_metadata": [
                    GITHUB_REMD.to_dict(),
                    {**GITHUB_REMD.to_dict(), "id": b"\x00" * 20},
                ]
            }
        ) == {"status": "eventful", "origin_extrinsic_metadata:add": 1}

        results = list(
            metadata_indexer.idx_storage.origin_extrinsic_metadata_get([origin])
        )
        assert len(results) == 1, results
        assert results[0].from_remd_id == b"\x00" * 20

    def test_extrinsic_directory_metadata_indexer_duplicate_origin(self, mocker):
        """Two metadata objects on directories, but with an origin context"""
        origin = DEPOSIT_REMD.origin

        metadata_indexer = ExtrinsicMetadataIndexer(config=DIRECTORY_METADATA_CONFIG)
        metadata_indexer.catch_exceptions = False
        metadata_indexer.storage = mocker.patch.object(metadata_indexer, "storage")
        metadata_indexer.storage.origin_get_by_sha1.return_value = [{"url": origin}]

        tool = metadata_indexer.idx_storage.indexer_configuration_get(
            {f"tool_{k}": v for (k, v) in TRANSLATOR_TOOL.items()}
        )
        assert tool is not None

        assert metadata_indexer.process_journal_objects(
            {
                "raw_extrinsic_metadata": [
                    DEPOSIT_REMD.to_dict(),
                    {
                        **DEPOSIT_REMD.to_dict(),
                        "id": b"\x00" * 20,
                        "target": "swh:1:dir:" + "01" * 20,
                    },
                ]
            }
        ) == {"status": "eventful", "origin_extrinsic_metadata:add": 1}

        results = list(
            metadata_indexer.idx_storage.origin_extrinsic_metadata_get([origin])
        )
        assert len(results) == 1, results
        assert results[0].from_remd_id == b"\x00" * 20
