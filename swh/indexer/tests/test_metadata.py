# Copyright (C) 2017-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.indexer.metadata import ContentMetadataIndexer, DirectoryMetadataIndexer
from swh.indexer.storage.model import ContentMetadataRow, DirectoryIntrinsicMetadataRow
from swh.indexer.tests.utils import DIRECTORY2
from swh.model.model import Directory, DirectoryEntry

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
