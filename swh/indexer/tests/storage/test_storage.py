# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import math
import threading
from typing import Any, Dict, List, Tuple, Type

import attr
import pytest

from swh.indexer.storage.exc import DuplicateId, IndexerStorageArgumentException
from swh.indexer.storage.interface import IndexerStorageInterface, PagedResult
from swh.indexer.storage.model import (
    BaseRow,
    ContentCtagsRow,
    ContentLanguageRow,
    ContentLicenseRow,
    ContentMetadataRow,
    ContentMimetypeRow,
    DirectoryIntrinsicMetadataRow,
    OriginExtrinsicMetadataRow,
    OriginIntrinsicMetadataRow,
)
from swh.model.hashutil import hash_to_bytes


def prepare_mimetypes_from_licenses(
    fossology_licenses: List[ContentLicenseRow],
) -> List[ContentMimetypeRow]:
    """Fossology license needs some consistent data in db to run."""
    mimetypes = []
    for c in fossology_licenses:
        mimetypes.append(
            ContentMimetypeRow(
                id=c.id,
                mimetype="text/plain",  # for filtering on textual data to work
                encoding="utf-8",
                indexer_configuration_id=c.indexer_configuration_id,
            )
        )
    return mimetypes


def endpoint_name(etype: str, ename: str) -> str:
    """Compute the storage's endpoint's name

    >>> endpoint_name('content_mimetype', 'add')
    'content_mimetype_add'
    >>> endpoint_name('content_fosso_license', 'delete')
    'content_fosso_license_delete'

    """
    return f"{etype}_{ename}"


def endpoint(storage, etype: str, ename: str):
    return getattr(storage, endpoint_name(etype, ename))


def expected_summary(count: int, etype: str, ename: str = "add") -> Dict[str, int]:
    """Compute the expected summary

    The key is determine according to etype and ename

        >>> expected_summary(10, 'content_mimetype', 'add')
        {'content_mimetype:add': 10}
        >>> expected_summary(9, 'origin_intrinsic_metadata', 'delete')
        {'origin_intrinsic_metadata:del': 9}

    """
    pattern = ename[0:3]
    key = endpoint_name(etype, ename).replace(f"_{ename}", f":{pattern}")
    return {key: count}


def test_check_config(swh_indexer_storage) -> None:
    assert swh_indexer_storage.check_config(check_write=True)
    assert swh_indexer_storage.check_config(check_write=False)


class StorageETypeTester:
    """Base class for testing a series of common behaviour between a bunch of
    endpoint types supported by an IndexerStorage.

    This is supposed to be inherited with the following class attributes:
    - endpoint_type
    - tool_name
    - example_data

    See below for example usage.
    """

    endpoint_type: str
    tool_name: str
    example_data: List[Dict]
    row_class: Type[BaseRow]

    def test_missing(
        self, swh_indexer_storage_with_data: Tuple[IndexerStorageInterface, Any]
    ) -> None:
        storage, data = swh_indexer_storage_with_data
        etype = self.endpoint_type
        tool_id = data.tools[self.tool_name]["id"]

        # given 2 (hopefully) unknown objects
        query = [
            {
                "id": data.sha1_1,
                "indexer_configuration_id": tool_id,
            },
            {
                "id": data.sha1_2,
                "indexer_configuration_id": tool_id,
            },
        ]

        # we expect these are both returned by the xxx_missing endpoint
        actual_missing = endpoint(storage, etype, "missing")(query)
        assert list(actual_missing) == [
            data.sha1_1,
            data.sha1_2,
        ]

        # now, when we add one of them
        summary = endpoint(storage, etype, "add")(
            [
                self.row_class.from_dict(
                    {
                        "id": data.sha1_2,
                        **self.example_data[0],
                        "indexer_configuration_id": tool_id,
                    }
                )
            ]
        )

        assert summary == expected_summary(1, etype)

        # we expect only the other one returned
        actual_missing = endpoint(storage, etype, "missing")(query)
        assert list(actual_missing) == [data.sha1_1]

    def test_add__update_in_place_duplicate(
        self, swh_indexer_storage_with_data: Tuple[IndexerStorageInterface, Any]
    ) -> None:
        storage, data = swh_indexer_storage_with_data
        etype = self.endpoint_type
        tool = data.tools[self.tool_name]

        data_v1 = {
            "id": data.sha1_2,
            **self.example_data[0],
            "indexer_configuration_id": tool["id"],
        }

        # given
        summary = endpoint(storage, etype, "add")([self.row_class.from_dict(data_v1)])
        assert summary == expected_summary(1, etype)  # not added

        # when
        actual_data = list(endpoint(storage, etype, "get")([data.sha1_2]))

        expected_data_v1 = [
            self.row_class.from_dict(
                {"id": data.sha1_2, **self.example_data[0], "tool": tool}
            )
        ]

        # then
        assert actual_data == expected_data_v1

        # given
        data_v2 = data_v1.copy()
        data_v2.update(self.example_data[1])

        endpoint(storage, etype, "add")([self.row_class.from_dict(data_v2)])
        assert summary == expected_summary(1, etype)  # modified so counted

        actual_data = list(endpoint(storage, etype, "get")([data.sha1_2]))

        expected_data_v2 = [
            self.row_class.from_dict(
                {
                    "id": data.sha1_2,
                    **self.example_data[1],
                    "tool": tool,
                }
            )
        ]

        # data did change as the v2 was used to overwrite v1
        assert actual_data == expected_data_v2

    def test_add_deadlock(
        self, swh_indexer_storage_with_data: Tuple[IndexerStorageInterface, Any]
    ) -> None:
        storage, data = swh_indexer_storage_with_data
        etype = self.endpoint_type
        tool = data.tools[self.tool_name]

        hashes = [
            hash_to_bytes("34973274ccef6ab4dfaaf86599792fa9c3fe4{:03d}".format(i))
            for i in range(1000)
        ]

        data_v1 = [
            self.row_class.from_dict(
                {
                    "id": hash_,
                    **self.example_data[0],
                    "indexer_configuration_id": tool["id"],
                }
            )
            for hash_ in hashes
        ]
        data_v2 = [
            self.row_class.from_dict(
                {
                    "id": hash_,
                    **self.example_data[1],
                    "indexer_configuration_id": tool["id"],
                }
            )
            for hash_ in hashes
        ]

        # Remove one item from each, so that both queries have to succeed for
        # all items to be in the DB.
        data_v2a = data_v2[1:]
        data_v2b = list(reversed(data_v2[0:-1]))

        # given
        endpoint(storage, etype, "add")(data_v1)

        # when
        actual_data = sorted(
            endpoint(storage, etype, "get")(hashes),
            key=lambda x: x.id,
        )

        expected_data_v1 = [
            self.row_class.from_dict(
                {"id": hash_, **self.example_data[0], "tool": tool}
            )
            for hash_ in hashes
        ]

        # then
        assert actual_data == expected_data_v1

        # given
        def f1() -> None:
            endpoint(storage, etype, "add")(data_v2a)

        def f2() -> None:
            endpoint(storage, etype, "add")(data_v2b)

        t1 = threading.Thread(target=f1)
        t2 = threading.Thread(target=f2)
        t2.start()
        t1.start()

        t1.join()
        t2.join()

        actual_data = sorted(
            endpoint(storage, etype, "get")(hashes),
            key=lambda x: x.id,
        )

        expected_data_v2 = [
            self.row_class.from_dict(
                {"id": hash_, **self.example_data[1], "tool": tool}
            )
            for hash_ in hashes
        ]

        assert len(actual_data) == len(expected_data_v1) == len(expected_data_v2)
        for (item, expected_item_v1, expected_item_v2) in zip(
            actual_data, expected_data_v1, expected_data_v2
        ):
            assert item in (expected_item_v1, expected_item_v2)

    def test_add__duplicate_twice(
        self, swh_indexer_storage_with_data: Tuple[IndexerStorageInterface, Any]
    ) -> None:
        storage, data = swh_indexer_storage_with_data
        etype = self.endpoint_type
        tool = data.tools[self.tool_name]

        data_dir1 = self.row_class.from_dict(
            {
                "id": data.directory_id_2,
                **self.example_data[0],
                "indexer_configuration_id": tool["id"],
            }
        )

        data_dir2 = self.row_class.from_dict(
            {
                "id": data.directory_id_2,
                **self.example_data[1],
                "indexer_configuration_id": tool["id"],
            }
        )

        # when
        summary = endpoint(storage, etype, "add")([data_dir1])
        assert summary == expected_summary(1, etype)

        with pytest.raises(DuplicateId):
            endpoint(storage, etype, "add")([data_dir2, data_dir2])

        # then
        actual_data = list(
            endpoint(storage, etype, "get")([data.directory_id_2, data.directory_id_1])
        )

        expected_data = [
            self.row_class.from_dict(
                {"id": data.directory_id_2, **self.example_data[0], "tool": tool}
            )
        ]
        assert actual_data == expected_data

    def test_add(
        self, swh_indexer_storage_with_data: Tuple[IndexerStorageInterface, Any]
    ) -> None:
        storage, data = swh_indexer_storage_with_data
        etype = self.endpoint_type
        tool = data.tools[self.tool_name]

        # conftest fills it with mimetypes
        storage.journal_writer.journal.objects = []  # type: ignore

        query = [data.sha1_2, data.sha1_1]
        data1 = self.row_class.from_dict(
            {
                "id": data.sha1_2,
                **self.example_data[0],
                "indexer_configuration_id": tool["id"],
            }
        )

        # when
        summary = endpoint(storage, etype, "add")([data1])
        assert summary == expected_summary(1, etype)

        # then
        actual_data = list(endpoint(storage, etype, "get")(query))

        # then
        expected_data = [
            self.row_class.from_dict(
                {"id": data.sha1_2, **self.example_data[0], "tool": tool}
            )
        ]

        assert actual_data == expected_data

        journal_objects = storage.journal_writer.journal.objects  # type: ignore
        actual_journal_data = [
            obj for (obj_type, obj) in journal_objects if obj_type == self.endpoint_type
        ]
        assert list(sorted(actual_journal_data)) == list(sorted(expected_data))


class TestIndexerStorageContentMimetypes(StorageETypeTester):
    """Test Indexer Storage content_mimetype related methods"""

    endpoint_type = "content_mimetype"
    tool_name = "file"
    example_data = [
        {
            "mimetype": "text/plain",
            "encoding": "utf-8",
        },
        {
            "mimetype": "text/html",
            "encoding": "us-ascii",
        },
    ]
    row_class = ContentMimetypeRow

    def test_generate_content_mimetype_get_partition_failure(
        self, swh_indexer_storage: IndexerStorageInterface
    ) -> None:
        """get_partition call with wrong limit input should fail"""
        storage = swh_indexer_storage
        indexer_configuration_id = 42
        with pytest.raises(
            IndexerStorageArgumentException, match="limit should not be None"
        ):
            storage.content_mimetype_get_partition(
                indexer_configuration_id, 0, 3, limit=None  # type: ignore
            )

    def test_generate_content_mimetype_get_partition_no_limit(
        self, swh_indexer_storage_with_data: Tuple[IndexerStorageInterface, Any]
    ) -> None:
        """get_partition should return result"""
        storage, data = swh_indexer_storage_with_data
        mimetypes = data.mimetypes

        expected_ids = set([c.id for c in mimetypes])
        indexer_configuration_id = mimetypes[0].indexer_configuration_id

        assert len(mimetypes) == 16
        nb_partitions = 16

        actual_ids = []
        for partition_id in range(nb_partitions):
            actual_result = storage.content_mimetype_get_partition(
                indexer_configuration_id, partition_id, nb_partitions
            )
            assert actual_result.next_page_token is None
            actual_ids.extend(actual_result.results)

        assert len(actual_ids) == len(expected_ids)
        for actual_id in actual_ids:
            assert actual_id in expected_ids

    def test_generate_content_mimetype_get_partition_full(
        self, swh_indexer_storage_with_data: Tuple[IndexerStorageInterface, Any]
    ) -> None:
        """get_partition for a single partition should return available ids"""
        storage, data = swh_indexer_storage_with_data
        mimetypes = data.mimetypes
        expected_ids = set([c.id for c in mimetypes])
        indexer_configuration_id = mimetypes[0].indexer_configuration_id

        actual_result = storage.content_mimetype_get_partition(
            indexer_configuration_id, 0, 1
        )
        assert actual_result.next_page_token is None
        actual_ids = actual_result.results
        assert len(actual_ids) == len(expected_ids)
        for actual_id in actual_ids:
            assert actual_id in expected_ids

    def test_generate_content_mimetype_get_partition_empty(
        self, swh_indexer_storage_with_data: Tuple[IndexerStorageInterface, Any]
    ) -> None:
        """get_partition when at least one of the partitions is empty"""
        storage, data = swh_indexer_storage_with_data
        mimetypes = data.mimetypes
        expected_ids = set([c.id for c in mimetypes])
        indexer_configuration_id = mimetypes[0].indexer_configuration_id

        # nb_partitions = smallest power of 2 such that at least one of
        # the partitions is empty
        nb_mimetypes = len(mimetypes)
        nb_partitions = 1 << math.floor(math.log2(nb_mimetypes) + 1)

        seen_ids = []

        for partition_id in range(nb_partitions):
            actual_result = storage.content_mimetype_get_partition(
                indexer_configuration_id,
                partition_id,
                nb_partitions,
                limit=nb_mimetypes + 1,
            )

            for actual_id in actual_result.results:
                seen_ids.append(actual_id)

            # Limit is higher than the max number of results
            assert actual_result.next_page_token is None

        assert set(seen_ids) == expected_ids

    def test_generate_content_mimetype_get_partition_with_pagination(
        self, swh_indexer_storage_with_data: Tuple[IndexerStorageInterface, Any]
    ) -> None:
        """get_partition should return ids provided with pagination"""
        storage, data = swh_indexer_storage_with_data
        mimetypes = data.mimetypes
        expected_ids = set([c.id for c in mimetypes])
        indexer_configuration_id = mimetypes[0].indexer_configuration_id

        nb_partitions = 4

        actual_ids = []
        for partition_id in range(nb_partitions):
            next_page_token = None
            while True:
                actual_result = storage.content_mimetype_get_partition(
                    indexer_configuration_id,
                    partition_id,
                    nb_partitions,
                    limit=2,
                    page_token=next_page_token,
                )
                actual_ids.extend(actual_result.results)
                next_page_token = actual_result.next_page_token
                if next_page_token is None:
                    break

        assert len(set(actual_ids)) == len(set(expected_ids))
        for actual_id in actual_ids:
            assert actual_id in expected_ids


class TestIndexerStorageContentLanguage(StorageETypeTester):
    """Test Indexer Storage content_language related methods"""

    endpoint_type = "content_language"
    tool_name = "pygments"
    example_data = [
        {
            "lang": "haskell",
        },
        {
            "lang": "common-lisp",
        },
    ]
    row_class = ContentLanguageRow


class TestIndexerStorageContentCTags(StorageETypeTester):
    """Test Indexer Storage content_ctags related methods"""

    endpoint_type = "content_ctags"
    tool_name = "universal-ctags"
    example_data = [
        {
            "name": "done",
            "kind": "variable",
            "line": 119,
            "lang": "OCaml",
        },
        {
            "name": "done",
            "kind": "variable",
            "line": 100,
            "lang": "Python",
        },
        {
            "name": "main",
            "kind": "function",
            "line": 119,
            "lang": "Python",
        },
    ]
    row_class = ContentCtagsRow

    # the following tests are disabled because CTAGS behaves differently
    @pytest.mark.skip
    def test_add__update_in_place_duplicate(self):
        pass

    @pytest.mark.skip
    def test_add_deadlock(self):
        pass

    def test_content_ctags_search(
        self, swh_indexer_storage_with_data: Tuple[IndexerStorageInterface, Any]
    ) -> None:
        storage, data = swh_indexer_storage_with_data
        # 1. given
        tool = data.tools["universal-ctags"]
        tool_id = tool["id"]

        ctags1 = [
            ContentCtagsRow(
                id=data.sha1_1,
                indexer_configuration_id=tool_id,
                **kwargs,  # type: ignore
            )
            for kwargs in [
                {
                    "name": "hello",
                    "kind": "function",
                    "line": 133,
                    "lang": "Python",
                },
                {
                    "name": "counter",
                    "kind": "variable",
                    "line": 119,
                    "lang": "Python",
                },
                {
                    "name": "hello",
                    "kind": "variable",
                    "line": 210,
                    "lang": "Python",
                },
            ]
        ]
        ctags1_with_tool = [
            attr.evolve(ctag, indexer_configuration_id=None, tool=tool)
            for ctag in ctags1
        ]

        ctags2 = [
            ContentCtagsRow(
                id=data.sha1_2,
                indexer_configuration_id=tool_id,
                **kwargs,  # type: ignore
            )
            for kwargs in [
                {
                    "name": "hello",
                    "kind": "variable",
                    "line": 100,
                    "lang": "C",
                },
                {
                    "name": "result",
                    "kind": "variable",
                    "line": 120,
                    "lang": "C",
                },
            ]
        ]
        ctags2_with_tool = [
            attr.evolve(ctag, indexer_configuration_id=None, tool=tool)
            for ctag in ctags2
        ]

        storage.content_ctags_add(ctags1 + ctags2)

        # 1. when
        actual_ctags = list(storage.content_ctags_search("hello", limit=1))

        # 1. then
        assert actual_ctags == [ctags1_with_tool[0]]

        # 2. when
        actual_ctags = list(
            storage.content_ctags_search("hello", limit=1, last_sha1=data.sha1_1)
        )

        # 2. then
        assert actual_ctags == [ctags2_with_tool[0]]

        # 3. when
        actual_ctags = list(storage.content_ctags_search("hello"))

        # 3. then
        assert actual_ctags == [
            ctags1_with_tool[0],
            ctags1_with_tool[2],
            ctags2_with_tool[0],
        ]

        # 4. when
        actual_ctags = list(storage.content_ctags_search("counter"))

        # then
        assert actual_ctags == [ctags1_with_tool[1]]

        # 5. when
        actual_ctags = list(storage.content_ctags_search("result", limit=1))

        # then
        assert actual_ctags == [ctags2_with_tool[1]]

    def test_content_ctags_search_no_result(
        self, swh_indexer_storage: IndexerStorageInterface
    ) -> None:
        storage = swh_indexer_storage
        actual_ctags = list(storage.content_ctags_search("counter"))

        assert not actual_ctags

    def test_content_ctags_add__add_new_ctags_added(
        self, swh_indexer_storage_with_data: Tuple[IndexerStorageInterface, Any]
    ) -> None:
        storage, data = swh_indexer_storage_with_data

        # given
        tool = data.tools["universal-ctags"]
        tool_id = tool["id"]

        ctag1 = ContentCtagsRow(
            id=data.sha1_2,
            indexer_configuration_id=tool_id,
            name="done",
            kind="variable",
            line=100,
            lang="Scheme",
        )
        ctag1_with_tool = attr.evolve(ctag1, indexer_configuration_id=None, tool=tool)

        # given
        storage.content_ctags_add([ctag1])
        storage.content_ctags_add([ctag1])  # conflict does nothing

        # when
        actual_ctags = list(storage.content_ctags_get([data.sha1_2]))

        # then
        assert actual_ctags == [ctag1_with_tool]

        # given
        ctag2 = ContentCtagsRow(
            id=data.sha1_2,
            indexer_configuration_id=tool_id,
            name="defn",
            kind="function",
            line=120,
            lang="Scheme",
        )
        ctag2_with_tool = attr.evolve(ctag2, indexer_configuration_id=None, tool=tool)

        storage.content_ctags_add([ctag2])

        actual_ctags = list(storage.content_ctags_get([data.sha1_2]))

        assert actual_ctags == [ctag1_with_tool, ctag2_with_tool]

    def test_content_ctags_add__update_in_place(
        self, swh_indexer_storage_with_data: Tuple[IndexerStorageInterface, Any]
    ) -> None:
        storage, data = swh_indexer_storage_with_data
        # given
        tool = data.tools["universal-ctags"]
        tool_id = tool["id"]

        ctag1 = ContentCtagsRow(
            id=data.sha1_2,
            indexer_configuration_id=tool_id,
            name="done",
            kind="variable",
            line=100,
            lang="Scheme",
        )
        ctag1_with_tool = attr.evolve(ctag1, indexer_configuration_id=None, tool=tool)

        # given
        storage.content_ctags_add([ctag1])

        # when
        actual_ctags = list(storage.content_ctags_get([data.sha1_2]))

        # then
        assert actual_ctags == [ctag1_with_tool]

        # given
        ctag2 = ContentCtagsRow(
            id=data.sha1_2,
            indexer_configuration_id=tool_id,
            name="defn",
            kind="function",
            line=120,
            lang="Scheme",
        )
        ctag2_with_tool = attr.evolve(ctag2, indexer_configuration_id=None, tool=tool)

        storage.content_ctags_add([ctag1, ctag2])

        actual_ctags = list(storage.content_ctags_get([data.sha1_2]))

        assert actual_ctags == [ctag1_with_tool, ctag2_with_tool]

    def test_add_empty(
        self, swh_indexer_storage_with_data: Tuple[IndexerStorageInterface, Any]
    ) -> None:
        (storage, data) = swh_indexer_storage_with_data
        etype = self.endpoint_type

        summary = endpoint(storage, etype, "add")([])
        assert summary == {"content_ctags:add": 0}

        actual_ctags = list(endpoint(storage, etype, "get")([data.sha1_2]))

        assert actual_ctags == []

    def test_get_unknown(
        self, swh_indexer_storage_with_data: Tuple[IndexerStorageInterface, Any]
    ) -> None:
        (storage, data) = swh_indexer_storage_with_data
        etype = self.endpoint_type

        actual_ctags = list(endpoint(storage, etype, "get")([data.sha1_2]))

        assert actual_ctags == []


class TestIndexerStorageContentMetadata(StorageETypeTester):
    """Test Indexer Storage content_metadata related methods"""

    tool_name = "swh-metadata-detector"
    endpoint_type = "content_metadata"
    example_data = [
        {
            "metadata": {
                "other": {},
                "codeRepository": {
                    "type": "git",
                    "url": "https://github.com/moranegg/metadata_test",
                },
                "description": "Simple package.json test for indexer",
                "name": "test_metadata",
                "version": "0.0.1",
            },
        },
        {
            "metadata": {"other": {}, "name": "test_metadata", "version": "0.0.1"},
        },
    ]
    row_class = ContentMetadataRow


class TestIndexerStorageDirectoryIntrinsicMetadata(StorageETypeTester):
    """Test Indexer Storage directory_intrinsic_metadata related methods"""

    tool_name = "swh-metadata-detector"
    endpoint_type = "directory_intrinsic_metadata"
    example_data = [
        {
            "metadata": {
                "other": {},
                "codeRepository": {
                    "type": "git",
                    "url": "https://github.com/moranegg/metadata_test",
                },
                "description": "Simple package.json test for indexer",
                "name": "test_metadata",
                "version": "0.0.1",
            },
            "mappings": ["mapping1"],
        },
        {
            "metadata": {"other": {}, "name": "test_metadata", "version": "0.0.1"},
            "mappings": ["mapping2"],
        },
    ]
    row_class = DirectoryIntrinsicMetadataRow


class TestIndexerStorageContentFossologyLicense(StorageETypeTester):
    endpoint_type = "content_fossology_license"
    tool_name = "nomos"
    example_data = [
        {"license": "Apache-2.0"},
        {"license": "BSD-2-Clause"},
    ]

    row_class = ContentLicenseRow

    # the following tests are disabled because licenses behaves differently
    @pytest.mark.skip
    def test_add__update_in_place_duplicate(self):
        pass

    @pytest.mark.skip
    def test_add_deadlock(self):
        pass

    # content_fossology_license_missing does not exist
    @pytest.mark.skip
    def test_missing(self):
        pass

    def test_content_fossology_license_add__new_license_added(
        self, swh_indexer_storage_with_data: Tuple[IndexerStorageInterface, Any]
    ) -> None:
        storage, data = swh_indexer_storage_with_data
        # given
        tool = data.tools["nomos"]
        tool_id = tool["id"]

        license1 = ContentLicenseRow(
            id=data.sha1_1,
            license="Apache-2.0",
            indexer_configuration_id=tool_id,
        )

        # given
        storage.content_fossology_license_add([license1])
        # conflict does nothing
        storage.content_fossology_license_add([license1])

        # when
        actual_licenses = list(storage.content_fossology_license_get([data.sha1_1]))

        # then
        expected_licenses = [
            ContentLicenseRow(
                id=data.sha1_1,
                license="Apache-2.0",
                tool=tool,
            )
        ]
        assert actual_licenses == expected_licenses

        # given
        license2 = ContentLicenseRow(
            id=data.sha1_1,
            license="BSD-2-Clause",
            indexer_configuration_id=tool_id,
        )

        storage.content_fossology_license_add([license2])

        actual_licenses = list(storage.content_fossology_license_get([data.sha1_1]))

        expected_licenses.append(
            ContentLicenseRow(
                id=data.sha1_1,
                license="BSD-2-Clause",
                tool=tool,
            )
        )

        # first license was not removed when the second one was added
        assert sorted(actual_licenses) == sorted(expected_licenses)

    def test_generate_content_fossology_license_get_partition_failure(
        self, swh_indexer_storage_with_data: Tuple[IndexerStorageInterface, Any]
    ) -> None:
        """get_partition call with wrong limit input should fail"""
        storage, data = swh_indexer_storage_with_data
        indexer_configuration_id = 42
        with pytest.raises(
            IndexerStorageArgumentException, match="limit should not be None"
        ):
            storage.content_fossology_license_get_partition(
                indexer_configuration_id,
                0,
                3,
                limit=None,  # type: ignore
            )

    def test_generate_content_fossology_license_get_partition_no_limit(
        self, swh_indexer_storage_with_data: Tuple[IndexerStorageInterface, Any]
    ) -> None:
        """get_partition should return results"""
        storage, data = swh_indexer_storage_with_data
        # craft some consistent mimetypes
        fossology_licenses = data.fossology_licenses
        mimetypes = prepare_mimetypes_from_licenses(fossology_licenses)
        indexer_configuration_id = fossology_licenses[0].indexer_configuration_id

        storage.content_mimetype_add(mimetypes)
        # add fossology_licenses to storage
        storage.content_fossology_license_add(fossology_licenses)

        # All ids from the db
        expected_ids = set([c.id for c in fossology_licenses])

        assert len(fossology_licenses) == 10
        assert len(mimetypes) == 10
        nb_partitions = 4

        actual_ids = []
        for partition_id in range(nb_partitions):

            actual_result = storage.content_fossology_license_get_partition(
                indexer_configuration_id, partition_id, nb_partitions
            )
            assert actual_result.next_page_token is None
            actual_ids.extend(actual_result.results)

        assert len(set(actual_ids)) == len(expected_ids)
        for actual_id in actual_ids:
            assert actual_id in expected_ids

    def test_generate_content_fossology_license_get_partition_full(
        self, swh_indexer_storage_with_data: Tuple[IndexerStorageInterface, Any]
    ) -> None:
        """get_partition for a single partition should return available ids"""
        storage, data = swh_indexer_storage_with_data
        # craft some consistent mimetypes
        fossology_licenses = data.fossology_licenses
        mimetypes = prepare_mimetypes_from_licenses(fossology_licenses)
        indexer_configuration_id = fossology_licenses[0].indexer_configuration_id

        storage.content_mimetype_add(mimetypes)
        # add fossology_licenses to storage
        storage.content_fossology_license_add(fossology_licenses)

        # All ids from the db
        expected_ids = set([c.id for c in fossology_licenses])

        actual_result = storage.content_fossology_license_get_partition(
            indexer_configuration_id, 0, 1
        )
        assert actual_result.next_page_token is None
        actual_ids = actual_result.results
        assert len(set(actual_ids)) == len(expected_ids)
        for actual_id in actual_ids:
            assert actual_id in expected_ids

    def test_generate_content_fossology_license_get_partition_empty(
        self, swh_indexer_storage_with_data: Tuple[IndexerStorageInterface, Any]
    ) -> None:
        """get_partition when at least one of the partitions is empty"""
        storage, data = swh_indexer_storage_with_data
        # craft some consistent mimetypes
        fossology_licenses = data.fossology_licenses
        mimetypes = prepare_mimetypes_from_licenses(fossology_licenses)
        indexer_configuration_id = fossology_licenses[0].indexer_configuration_id

        storage.content_mimetype_add(mimetypes)
        # add fossology_licenses to storage
        storage.content_fossology_license_add(fossology_licenses)

        # All ids from the db
        expected_ids = set([c.id for c in fossology_licenses])

        # nb_partitions = smallest power of 2 such that at least one of
        # the partitions is empty
        nb_licenses = len(fossology_licenses)
        nb_partitions = 1 << math.floor(math.log2(nb_licenses) + 1)

        seen_ids = []

        for partition_id in range(nb_partitions):
            actual_result = storage.content_fossology_license_get_partition(
                indexer_configuration_id,
                partition_id,
                nb_partitions,
                limit=nb_licenses + 1,
            )

            for actual_id in actual_result.results:
                seen_ids.append(actual_id)

            # Limit is higher than the max number of results
            assert actual_result.next_page_token is None

        assert set(seen_ids) == expected_ids

    def test_generate_content_fossology_license_get_partition_with_pagination(
        self, swh_indexer_storage_with_data: Tuple[IndexerStorageInterface, Any]
    ) -> None:
        """get_partition should return ids provided with paginationv"""
        storage, data = swh_indexer_storage_with_data
        # craft some consistent mimetypes
        fossology_licenses = data.fossology_licenses
        mimetypes = prepare_mimetypes_from_licenses(fossology_licenses)
        indexer_configuration_id = fossology_licenses[0].indexer_configuration_id

        storage.content_mimetype_add(mimetypes)
        # add fossology_licenses to storage
        storage.content_fossology_license_add(fossology_licenses)

        # All ids from the db
        expected_ids = [c.id for c in fossology_licenses]

        nb_partitions = 4

        actual_ids = []
        for partition_id in range(nb_partitions):
            next_page_token = None
            while True:
                actual_result = storage.content_fossology_license_get_partition(
                    indexer_configuration_id,
                    partition_id,
                    nb_partitions,
                    limit=2,
                    page_token=next_page_token,
                )
                actual_ids.extend(actual_result.results)
                next_page_token = actual_result.next_page_token
                if next_page_token is None:
                    break

        assert len(set(actual_ids)) == len(set(expected_ids))
        for actual_id in actual_ids:
            assert actual_id in expected_ids

    def test_add_empty(
        self, swh_indexer_storage_with_data: Tuple[IndexerStorageInterface, Any]
    ) -> None:
        (storage, data) = swh_indexer_storage_with_data
        etype = self.endpoint_type

        summary = endpoint(storage, etype, "add")([])
        assert summary == {"content_fossology_license:add": 0}

        actual_license = list(endpoint(storage, etype, "get")([data.sha1_2]))

        assert actual_license == []

    def test_get_unknown(
        self, swh_indexer_storage_with_data: Tuple[IndexerStorageInterface, Any]
    ) -> None:
        (storage, data) = swh_indexer_storage_with_data
        etype = self.endpoint_type

        actual_license = list(endpoint(storage, etype, "get")([data.sha1_2]))

        assert actual_license == []


class TestIndexerStorageOriginIntrinsicMetadata:
    def test_origin_intrinsic_metadata_add(
        self, swh_indexer_storage_with_data: Tuple[IndexerStorageInterface, Any]
    ) -> None:
        storage, data = swh_indexer_storage_with_data
        # given
        tool_id = data.tools["swh-metadata-detector"]["id"]

        metadata = {
            "version": None,
            "name": None,
        }
        metadata_dir = DirectoryIntrinsicMetadataRow(
            id=data.directory_id_2,
            metadata=metadata,
            mappings=["mapping1"],
            indexer_configuration_id=tool_id,
        )
        metadata_origin = OriginIntrinsicMetadataRow(
            id=data.origin_url_1,
            metadata=metadata,
            indexer_configuration_id=tool_id,
            mappings=["mapping1"],
            from_directory=data.directory_id_2,
        )

        # when
        storage.directory_intrinsic_metadata_add([metadata_dir])
        storage.origin_intrinsic_metadata_add([metadata_origin])

        # then
        actual_metadata = list(
            storage.origin_intrinsic_metadata_get([data.origin_url_1, "no://where"])
        )

        expected_metadata = [
            OriginIntrinsicMetadataRow(
                id=data.origin_url_1,
                metadata=metadata,
                tool=data.tools["swh-metadata-detector"],
                from_directory=data.directory_id_2,
                mappings=["mapping1"],
            )
        ]

        assert actual_metadata == expected_metadata

        journal_objects = storage.journal_writer.journal.objects  # type: ignore
        actual_journal_metadata = [
            obj
            for (obj_type, obj) in journal_objects
            if obj_type == "origin_intrinsic_metadata"
        ]
        assert list(sorted(actual_journal_metadata)) == list(sorted(expected_metadata))

    def test_origin_intrinsic_metadata_add_update_in_place_duplicate(
        self, swh_indexer_storage_with_data: Tuple[IndexerStorageInterface, Any]
    ) -> None:
        storage, data = swh_indexer_storage_with_data
        # given
        tool_id = data.tools["swh-metadata-detector"]["id"]

        metadata_v1: Dict[str, Any] = {
            "version": None,
            "name": None,
        }
        metadata_dir_v1 = DirectoryIntrinsicMetadataRow(
            id=data.directory_id_2,
            metadata=metadata_v1,
            mappings=[],
            indexer_configuration_id=tool_id,
        )
        metadata_origin_v1 = OriginIntrinsicMetadataRow(
            id=data.origin_url_1,
            metadata=metadata_v1.copy(),
            indexer_configuration_id=tool_id,
            mappings=[],
            from_directory=data.directory_id_2,
        )

        # given
        storage.directory_intrinsic_metadata_add([metadata_dir_v1])
        storage.origin_intrinsic_metadata_add([metadata_origin_v1])

        # when
        actual_metadata = list(
            storage.origin_intrinsic_metadata_get([data.origin_url_1])
        )

        # then
        expected_metadata_v1 = [
            OriginIntrinsicMetadataRow(
                id=data.origin_url_1,
                metadata=metadata_v1,
                tool=data.tools["swh-metadata-detector"],
                from_directory=data.directory_id_2,
                mappings=[],
            )
        ]
        assert actual_metadata == expected_metadata_v1

        # given
        metadata_v2 = metadata_v1.copy()
        metadata_v2.update(
            {
                "name": "test_update_duplicated_metadata",
                "author": "MG",
            }
        )
        metadata_dir_v2 = attr.evolve(metadata_dir_v1, metadata=metadata_v2)
        metadata_origin_v2 = OriginIntrinsicMetadataRow(
            id=data.origin_url_1,
            metadata=metadata_v2.copy(),
            indexer_configuration_id=tool_id,
            mappings=["npm"],
            from_directory=data.directory_id_1,
        )

        storage.directory_intrinsic_metadata_add([metadata_dir_v2])
        storage.origin_intrinsic_metadata_add([metadata_origin_v2])

        actual_metadata = list(
            storage.origin_intrinsic_metadata_get([data.origin_url_1])
        )

        expected_metadata_v2 = [
            OriginIntrinsicMetadataRow(
                id=data.origin_url_1,
                metadata=metadata_v2,
                tool=data.tools["swh-metadata-detector"],
                from_directory=data.directory_id_1,
                mappings=["npm"],
            )
        ]

        # metadata did change as the v2 was used to overwrite v1
        assert actual_metadata == expected_metadata_v2

    def test_origin_intrinsic_metadata_add__deadlock(
        self, swh_indexer_storage_with_data: Tuple[IndexerStorageInterface, Any]
    ) -> None:
        storage, data = swh_indexer_storage_with_data
        # given
        tool_id = data.tools["swh-metadata-detector"]["id"]

        origins = ["file:///tmp/origin{:02d}".format(i) for i in range(100)]

        example_data1: Dict[str, Any] = {
            "metadata": {
                "version": None,
                "name": None,
            },
            "mappings": [],
        }
        example_data2: Dict[str, Any] = {
            "metadata": {
                "version": "v1.1.1",
                "name": "foo",
            },
            "mappings": [],
        }

        metadata_dir_v1 = DirectoryIntrinsicMetadataRow(
            id=data.directory_id_2,
            metadata={
                "version": None,
                "name": None,
            },
            mappings=[],
            indexer_configuration_id=tool_id,
        )

        data_v1 = [
            OriginIntrinsicMetadataRow(
                id=origin,
                from_directory=data.directory_id_2,
                indexer_configuration_id=tool_id,
                **example_data1,
            )
            for origin in origins
        ]
        data_v2 = [
            OriginIntrinsicMetadataRow(
                id=origin,
                from_directory=data.directory_id_2,
                indexer_configuration_id=tool_id,
                **example_data2,
            )
            for origin in origins
        ]

        # Remove one item from each, so that both queries have to succeed for
        # all items to be in the DB.
        data_v2a = data_v2[1:]
        data_v2b = list(reversed(data_v2[0:-1]))

        # given
        storage.directory_intrinsic_metadata_add([metadata_dir_v1])
        storage.origin_intrinsic_metadata_add(data_v1)

        # when
        actual_data = list(storage.origin_intrinsic_metadata_get(origins))

        expected_data_v1 = [
            OriginIntrinsicMetadataRow(
                id=origin,
                from_directory=data.directory_id_2,
                tool=data.tools["swh-metadata-detector"],
                **example_data1,
            )
            for origin in origins
        ]

        # then
        assert actual_data == expected_data_v1

        # given
        def f1() -> None:
            storage.origin_intrinsic_metadata_add(data_v2a)

        def f2() -> None:
            storage.origin_intrinsic_metadata_add(data_v2b)

        t1 = threading.Thread(target=f1)
        t2 = threading.Thread(target=f2)
        t2.start()
        t1.start()

        t1.join()
        t2.join()

        actual_data = list(storage.origin_intrinsic_metadata_get(origins))

        expected_data_v2 = [
            OriginIntrinsicMetadataRow(
                id=origin,
                from_directory=data.directory_id_2,
                tool=data.tools["swh-metadata-detector"],
                **example_data2,
            )
            for origin in origins
        ]

        actual_data.sort(key=lambda item: item.id)
        assert len(actual_data) == len(expected_data_v1) == len(expected_data_v2)
        for (item, expected_item_v1, expected_item_v2) in zip(
            actual_data, expected_data_v1, expected_data_v2
        ):
            assert item in (expected_item_v1, expected_item_v2)

    def test_origin_intrinsic_metadata_add__duplicate_twice(
        self, swh_indexer_storage_with_data: Tuple[IndexerStorageInterface, Any]
    ) -> None:
        storage, data = swh_indexer_storage_with_data
        # given
        tool_id = data.tools["swh-metadata-detector"]["id"]

        metadata = {
            "developmentStatus": None,
            "name": None,
        }
        metadata_dir = DirectoryIntrinsicMetadataRow(
            id=data.directory_id_2,
            metadata=metadata,
            mappings=["mapping1"],
            indexer_configuration_id=tool_id,
        )
        metadata_origin = OriginIntrinsicMetadataRow(
            id=data.origin_url_1,
            metadata=metadata,
            indexer_configuration_id=tool_id,
            mappings=["mapping1"],
            from_directory=data.directory_id_2,
        )

        # when
        storage.directory_intrinsic_metadata_add([metadata_dir])

        with pytest.raises(DuplicateId):
            storage.origin_intrinsic_metadata_add([metadata_origin, metadata_origin])

    def test_origin_intrinsic_metadata_search_fulltext(
        self, swh_indexer_storage_with_data: Tuple[IndexerStorageInterface, Any]
    ) -> None:
        storage, data = swh_indexer_storage_with_data
        # given
        tool_id = data.tools["swh-metadata-detector"]["id"]

        metadata1 = {
            "author": "John Doe",
        }
        metadata1_dir = DirectoryIntrinsicMetadataRow(
            id=data.directory_id_1,
            metadata=metadata1,
            mappings=[],
            indexer_configuration_id=tool_id,
        )
        metadata1_origin = OriginIntrinsicMetadataRow(
            id=data.origin_url_1,
            metadata=metadata1,
            mappings=[],
            indexer_configuration_id=tool_id,
            from_directory=data.directory_id_1,
        )
        metadata2 = {
            "author": "Jane Doe",
        }
        metadata2_dir = DirectoryIntrinsicMetadataRow(
            id=data.directory_id_2,
            metadata=metadata2,
            mappings=[],
            indexer_configuration_id=tool_id,
        )
        metadata2_origin = OriginIntrinsicMetadataRow(
            id=data.origin_url_2,
            metadata=metadata2,
            mappings=[],
            indexer_configuration_id=tool_id,
            from_directory=data.directory_id_2,
        )

        # when
        storage.directory_intrinsic_metadata_add([metadata1_dir])
        storage.origin_intrinsic_metadata_add([metadata1_origin])
        storage.directory_intrinsic_metadata_add([metadata2_dir])
        storage.origin_intrinsic_metadata_add([metadata2_origin])

        # then
        search = storage.origin_intrinsic_metadata_search_fulltext
        assert set([res.id for res in search(["Doe"])]) == set(
            [data.origin_url_1, data.origin_url_2]
        )
        assert [res.id for res in search(["John", "Doe"])] == [data.origin_url_1]
        assert [res.id for res in search(["John"])] == [data.origin_url_1]
        assert not list(search(["John", "Jane"]))

    def test_origin_intrinsic_metadata_search_fulltext_rank(
        self, swh_indexer_storage_with_data: Tuple[IndexerStorageInterface, Any]
    ) -> None:
        storage, data = swh_indexer_storage_with_data
        # given
        tool_id = data.tools["swh-metadata-detector"]["id"]

        # The following authors have "Random Person" to add some more content
        # to the JSON data, to work around normalization quirks when there
        # are few words (rank/(1+ln(nb_words)) is very sensitive to nb_words
        # for small values of nb_words).
        metadata1 = {
            "author": [
                "Random Person",
                "John Doe",
                "Jane Doe",
            ]
        }
        metadata1_dir = DirectoryIntrinsicMetadataRow(
            id=data.directory_id_1,
            metadata=metadata1,
            mappings=[],
            indexer_configuration_id=tool_id,
        )
        metadata1_origin = OriginIntrinsicMetadataRow(
            id=data.origin_url_1,
            metadata=metadata1,
            mappings=[],
            indexer_configuration_id=tool_id,
            from_directory=data.directory_id_1,
        )
        metadata2 = {
            "author": [
                "Random Person",
                "Jane Doe",
            ]
        }
        metadata2_dir = DirectoryIntrinsicMetadataRow(
            id=data.directory_id_2,
            metadata=metadata2,
            mappings=[],
            indexer_configuration_id=tool_id,
        )
        metadata2_origin = OriginIntrinsicMetadataRow(
            id=data.origin_url_2,
            metadata=metadata2,
            mappings=[],
            indexer_configuration_id=tool_id,
            from_directory=data.directory_id_2,
        )

        # when
        storage.directory_intrinsic_metadata_add([metadata1_dir])
        storage.origin_intrinsic_metadata_add([metadata1_origin])
        storage.directory_intrinsic_metadata_add([metadata2_dir])
        storage.origin_intrinsic_metadata_add([metadata2_origin])

        # then
        search = storage.origin_intrinsic_metadata_search_fulltext
        assert [res.id for res in search(["Doe"])] == [
            data.origin_url_1,
            data.origin_url_2,
        ]
        assert [res.id for res in search(["Doe"], limit=1)] == [data.origin_url_1]
        assert [res.id for res in search(["John"])] == [data.origin_url_1]
        assert [res.id for res in search(["Jane"])] == [
            data.origin_url_2,
            data.origin_url_1,
        ]
        assert [res.id for res in search(["John", "Jane"])] == [data.origin_url_1]

    def _fill_origin_intrinsic_metadata(
        self, swh_indexer_storage_with_data: Tuple[IndexerStorageInterface, Any]
    ) -> None:
        storage, data = swh_indexer_storage_with_data
        tool1_id = data.tools["swh-metadata-detector"]["id"]
        tool2_id = data.tools["swh-metadata-detector2"]["id"]

        metadata1 = {
            "@context": "foo",
            "author": "John Doe",
        }
        metadata1_dir = DirectoryIntrinsicMetadataRow(
            id=data.directory_id_1,
            metadata=metadata1,
            mappings=["npm"],
            indexer_configuration_id=tool1_id,
        )
        metadata1_origin = OriginIntrinsicMetadataRow(
            id=data.origin_url_1,
            metadata=metadata1,
            mappings=["npm"],
            indexer_configuration_id=tool1_id,
            from_directory=data.directory_id_1,
        )
        metadata2 = {
            "@context": "foo",
            "author": "Jane Doe",
        }
        metadata2_dir = DirectoryIntrinsicMetadataRow(
            id=data.directory_id_2,
            metadata=metadata2,
            mappings=["npm", "gemspec"],
            indexer_configuration_id=tool2_id,
        )
        metadata2_origin = OriginIntrinsicMetadataRow(
            id=data.origin_url_2,
            metadata=metadata2,
            mappings=["npm", "gemspec"],
            indexer_configuration_id=tool2_id,
            from_directory=data.directory_id_2,
        )
        metadata3 = {
            "@context": "foo",
        }
        metadata3_dir = DirectoryIntrinsicMetadataRow(
            id=data.directory_id_3,
            metadata=metadata3,
            mappings=["npm", "gemspec"],
            indexer_configuration_id=tool2_id,
        )
        metadata3_origin = OriginIntrinsicMetadataRow(
            id=data.origin_url_3,
            metadata=metadata3,
            mappings=["pkg-info"],
            indexer_configuration_id=tool2_id,
            from_directory=data.directory_id_3,
        )

        storage.directory_intrinsic_metadata_add([metadata1_dir])
        storage.origin_intrinsic_metadata_add([metadata1_origin])
        storage.directory_intrinsic_metadata_add([metadata2_dir])
        storage.origin_intrinsic_metadata_add([metadata2_origin])
        storage.directory_intrinsic_metadata_add([metadata3_dir])
        storage.origin_intrinsic_metadata_add([metadata3_origin])

    def test_origin_intrinsic_metadata_search_by_producer(
        self, swh_indexer_storage_with_data: Tuple[IndexerStorageInterface, Any]
    ) -> None:
        storage, data = swh_indexer_storage_with_data
        self._fill_origin_intrinsic_metadata(swh_indexer_storage_with_data)
        tool1 = data.tools["swh-metadata-detector"]
        tool2 = data.tools["swh-metadata-detector2"]
        endpoint = storage.origin_intrinsic_metadata_search_by_producer

        # test pagination
        # no 'page_token' param, return all origins
        result = endpoint(ids_only=True)
        assert result == PagedResult(
            results=[
                data.origin_url_1,
                data.origin_url_2,
                data.origin_url_3,
            ],
            next_page_token=None,
        )

        # 'page_token' is < than origin_1, return everything
        result = endpoint(page_token=data.origin_url_1[:-1], ids_only=True)
        assert result == PagedResult(
            results=[
                data.origin_url_1,
                data.origin_url_2,
                data.origin_url_3,
            ],
            next_page_token=None,
        )

        # 'page_token' is origin_3, return nothing
        result = endpoint(page_token=data.origin_url_3, ids_only=True)
        assert result == PagedResult(results=[], next_page_token=None)

        # test limit argument
        result = endpoint(page_token=data.origin_url_1[:-1], limit=2, ids_only=True)
        assert result == PagedResult(
            results=[data.origin_url_1, data.origin_url_2],
            next_page_token=data.origin_url_2,
        )

        result = endpoint(page_token=data.origin_url_1, limit=2, ids_only=True)
        assert result == PagedResult(
            results=[data.origin_url_2, data.origin_url_3],
            next_page_token=None,
        )

        result = endpoint(page_token=data.origin_url_2, limit=2, ids_only=True)
        assert result == PagedResult(
            results=[data.origin_url_3],
            next_page_token=None,
        )

        # test mappings filtering
        result = endpoint(mappings=["npm"], ids_only=True)
        assert result == PagedResult(
            results=[data.origin_url_1, data.origin_url_2],
            next_page_token=None,
        )

        result = endpoint(mappings=["npm", "gemspec"], ids_only=True)
        assert result == PagedResult(
            results=[data.origin_url_1, data.origin_url_2],
            next_page_token=None,
        )

        result = endpoint(mappings=["gemspec"], ids_only=True)
        assert result == PagedResult(
            results=[data.origin_url_2],
            next_page_token=None,
        )

        result = endpoint(mappings=["pkg-info"], ids_only=True)
        assert result == PagedResult(
            results=[data.origin_url_3],
            next_page_token=None,
        )

        result = endpoint(mappings=["foobar"], ids_only=True)
        assert result == PagedResult(
            results=[],
            next_page_token=None,
        )

        # test pagination + mappings
        result = endpoint(mappings=["npm"], limit=1, ids_only=True)
        assert result == PagedResult(
            results=[data.origin_url_1],
            next_page_token=data.origin_url_1,
        )

        # test tool filtering
        result = endpoint(tool_ids=[tool1["id"]], ids_only=True)
        assert result == PagedResult(
            results=[data.origin_url_1],
            next_page_token=None,
        )

        result = endpoint(tool_ids=[tool2["id"]], ids_only=True)
        assert sorted(result.results) == [data.origin_url_2, data.origin_url_3]
        assert result.next_page_token is None

        result = endpoint(tool_ids=[tool1["id"], tool2["id"]], ids_only=True)
        assert sorted(result.results) == [
            data.origin_url_1,
            data.origin_url_2,
            data.origin_url_3,
        ]
        assert result.next_page_token is None

        # test ids_only=False
        assert endpoint(mappings=["gemspec"]) == PagedResult(
            results=[
                OriginIntrinsicMetadataRow(
                    id=data.origin_url_2,
                    metadata={
                        "@context": "foo",
                        "author": "Jane Doe",
                    },
                    mappings=["npm", "gemspec"],
                    tool=tool2,
                    from_directory=data.directory_id_2,
                )
            ],
            next_page_token=None,
        )

    def test_origin_intrinsic_metadata_stats(
        self, swh_indexer_storage_with_data: Tuple[IndexerStorageInterface, Any]
    ) -> None:
        storage, data = swh_indexer_storage_with_data
        self._fill_origin_intrinsic_metadata(swh_indexer_storage_with_data)

        result = storage.origin_intrinsic_metadata_stats()
        assert result == {
            "per_mapping": {
                "cff": 0,
                "gemspec": 1,
                "npm": 2,
                "pkg-info": 1,
                "codemeta": 0,
                "maven": 0,
            },
            "total": 3,
            "non_empty": 2,
        }


class TestIndexerStorageOriginExtrinsicMetadata:
    def test_origin_extrinsic_metadata_add(
        self, swh_indexer_storage_with_data: Tuple[IndexerStorageInterface, Any]
    ) -> None:
        storage, data = swh_indexer_storage_with_data
        # given
        tool_id = data.tools["swh-metadata-detector"]["id"]

        metadata = {
            "version": None,
            "name": None,
        }
        metadata_origin = OriginExtrinsicMetadataRow(
            id=data.origin_url_1,
            metadata=metadata,
            indexer_configuration_id=tool_id,
            mappings=["mapping1"],
            from_remd_id=b"\x02" * 20,
        )

        # when
        storage.origin_extrinsic_metadata_add([metadata_origin])

        # then
        actual_metadata = list(
            storage.origin_extrinsic_metadata_get([data.origin_url_1, "no://where"])
        )

        expected_metadata = [
            OriginExtrinsicMetadataRow(
                id=data.origin_url_1,
                metadata=metadata,
                tool=data.tools["swh-metadata-detector"],
                from_remd_id=b"\x02" * 20,
                mappings=["mapping1"],
            )
        ]

        assert actual_metadata == expected_metadata

        journal_objects = storage.journal_writer.journal.objects  # type: ignore
        actual_journal_metadata = [
            obj
            for (obj_type, obj) in journal_objects
            if obj_type == "origin_extrinsic_metadata"
        ]
        assert list(sorted(actual_journal_metadata)) == list(sorted(expected_metadata))

    def test_origin_extrinsic_metadata_add_update_in_place_duplicate(
        self, swh_indexer_storage_with_data: Tuple[IndexerStorageInterface, Any]
    ) -> None:
        storage, data = swh_indexer_storage_with_data
        # given
        tool_id = data.tools["swh-metadata-detector"]["id"]

        metadata_v1: Dict[str, Any] = {
            "version": None,
            "name": None,
        }
        metadata_origin_v1 = OriginExtrinsicMetadataRow(
            id=data.origin_url_1,
            metadata=metadata_v1.copy(),
            indexer_configuration_id=tool_id,
            mappings=[],
            from_remd_id=b"\x02" * 20,
        )

        # given
        storage.origin_extrinsic_metadata_add([metadata_origin_v1])

        # when
        actual_metadata = list(
            storage.origin_extrinsic_metadata_get([data.origin_url_1])
        )

        # then
        expected_metadata_v1 = [
            OriginExtrinsicMetadataRow(
                id=data.origin_url_1,
                metadata=metadata_v1,
                tool=data.tools["swh-metadata-detector"],
                from_remd_id=b"\x02" * 20,
                mappings=[],
            )
        ]
        assert actual_metadata == expected_metadata_v1

        # given
        metadata_v2 = metadata_v1.copy()
        metadata_v2.update(
            {
                "name": "test_update_duplicated_metadata",
                "author": "MG",
            }
        )
        metadata_origin_v2 = OriginExtrinsicMetadataRow(
            id=data.origin_url_1,
            metadata=metadata_v2.copy(),
            indexer_configuration_id=tool_id,
            mappings=["github"],
            from_remd_id=b"\x02" * 20,
        )

        storage.origin_extrinsic_metadata_add([metadata_origin_v2])

        actual_metadata = list(
            storage.origin_extrinsic_metadata_get([data.origin_url_1])
        )

        expected_metadata_v2 = [
            OriginExtrinsicMetadataRow(
                id=data.origin_url_1,
                metadata=metadata_v2,
                tool=data.tools["swh-metadata-detector"],
                from_remd_id=b"\x02" * 20,
                mappings=["github"],
            )
        ]

        # metadata did change as the v2 was used to overwrite v1
        assert actual_metadata == expected_metadata_v2

    def test_origin_extrinsic_metadata_add__deadlock(
        self, swh_indexer_storage_with_data: Tuple[IndexerStorageInterface, Any]
    ) -> None:
        storage, data = swh_indexer_storage_with_data
        # given
        tool_id = data.tools["swh-metadata-detector"]["id"]

        origins = ["file:///tmp/origin{:02d}".format(i) for i in range(100)]

        example_data1: Dict[str, Any] = {
            "metadata": {
                "version": None,
                "name": None,
            },
            "mappings": [],
        }
        example_data2: Dict[str, Any] = {
            "metadata": {
                "version": "v1.1.1",
                "name": "foo",
            },
            "mappings": [],
        }

        data_v1 = [
            OriginExtrinsicMetadataRow(
                id=origin,
                from_remd_id=b"\x02" * 20,
                indexer_configuration_id=tool_id,
                **example_data1,
            )
            for origin in origins
        ]
        data_v2 = [
            OriginExtrinsicMetadataRow(
                id=origin,
                from_remd_id=b"\x02" * 20,
                indexer_configuration_id=tool_id,
                **example_data2,
            )
            for origin in origins
        ]

        # Remove one item from each, so that both queries have to succeed for
        # all items to be in the DB.
        data_v2a = data_v2[1:]
        data_v2b = list(reversed(data_v2[0:-1]))

        # given
        storage.origin_extrinsic_metadata_add(data_v1)

        # when
        actual_data = list(storage.origin_extrinsic_metadata_get(origins))

        expected_data_v1 = [
            OriginExtrinsicMetadataRow(
                id=origin,
                from_remd_id=b"\x02" * 20,
                tool=data.tools["swh-metadata-detector"],
                **example_data1,
            )
            for origin in origins
        ]

        # then
        assert actual_data == expected_data_v1

        # given
        def f1() -> None:
            storage.origin_extrinsic_metadata_add(data_v2a)

        def f2() -> None:
            storage.origin_extrinsic_metadata_add(data_v2b)

        t1 = threading.Thread(target=f1)
        t2 = threading.Thread(target=f2)
        t2.start()
        t1.start()

        t1.join()
        t2.join()

        actual_data = list(storage.origin_extrinsic_metadata_get(origins))

        expected_data_v2 = [
            OriginExtrinsicMetadataRow(
                id=origin,
                from_remd_id=b"\x02" * 20,
                tool=data.tools["swh-metadata-detector"],
                **example_data2,
            )
            for origin in origins
        ]

        actual_data.sort(key=lambda item: item.id)
        assert len(actual_data) == len(expected_data_v1) == len(expected_data_v2)
        for (item, expected_item_v1, expected_item_v2) in zip(
            actual_data, expected_data_v1, expected_data_v2
        ):
            assert item in (expected_item_v1, expected_item_v2)

    def test_origin_extrinsic_metadata_add__duplicate_twice(
        self, swh_indexer_storage_with_data: Tuple[IndexerStorageInterface, Any]
    ) -> None:
        storage, data = swh_indexer_storage_with_data
        # given
        tool_id = data.tools["swh-metadata-detector"]["id"]

        metadata = {
            "developmentStatus": None,
            "name": None,
        }
        metadata_origin = OriginExtrinsicMetadataRow(
            id=data.origin_url_1,
            metadata=metadata,
            indexer_configuration_id=tool_id,
            mappings=["mapping1"],
            from_remd_id=b"\x02" * 20,
        )

        # when
        with pytest.raises(DuplicateId):
            storage.origin_extrinsic_metadata_add([metadata_origin, metadata_origin])


class TestIndexerStorageIndexerConfiguration:
    def test_indexer_configuration_add(
        self, swh_indexer_storage_with_data: Tuple[IndexerStorageInterface, Any]
    ) -> None:
        storage, data = swh_indexer_storage_with_data
        tool = {
            "tool_name": "some-unknown-tool",
            "tool_version": "some-version",
            "tool_configuration": {"debian-package": "some-package"},
        }

        actual_tool = storage.indexer_configuration_get(tool)
        assert actual_tool is None  # does not exist

        # add it
        actual_tools = list(storage.indexer_configuration_add([tool]))

        assert len(actual_tools) == 1
        actual_tool = actual_tools[0]
        assert actual_tool is not None  # now it exists
        new_id = actual_tool.pop("id")
        assert actual_tool == tool

        actual_tools2 = list(storage.indexer_configuration_add([tool]))
        actual_tool2 = actual_tools2[0]
        assert actual_tool2 is not None  # now it exists
        new_id2 = actual_tool2.pop("id")

        assert new_id == new_id2
        assert actual_tool == actual_tool2

    def test_indexer_configuration_add_multiple(
        self, swh_indexer_storage_with_data: Tuple[IndexerStorageInterface, Any]
    ) -> None:
        storage, data = swh_indexer_storage_with_data
        tool = {
            "tool_name": "some-unknown-tool",
            "tool_version": "some-version",
            "tool_configuration": {"debian-package": "some-package"},
        }

        actual_tools = list(storage.indexer_configuration_add([tool]))
        assert len(actual_tools) == 1

        new_tools = [
            tool,
            {
                "tool_name": "yet-another-tool",
                "tool_version": "version",
                "tool_configuration": {},
            },
        ]

        actual_tools = list(storage.indexer_configuration_add(new_tools))
        assert len(actual_tools) == 2

        # order not guaranteed, so we iterate over results to check
        for tool in actual_tools:
            _id = tool.pop("id")
            assert _id is not None
            assert tool in new_tools

    def test_indexer_configuration_get_missing(
        self, swh_indexer_storage_with_data: Tuple[IndexerStorageInterface, Any]
    ) -> None:
        storage, data = swh_indexer_storage_with_data
        tool = {
            "tool_name": "unknown-tool",
            "tool_version": "3.1.0rc2-31-ga2cbb8c",
            "tool_configuration": {"command_line": "nomossa <filepath>"},
        }

        actual_tool = storage.indexer_configuration_get(tool)

        assert actual_tool is None

    def test_indexer_configuration_get(
        self, swh_indexer_storage_with_data: Tuple[IndexerStorageInterface, Any]
    ) -> None:
        storage, data = swh_indexer_storage_with_data
        tool = {
            "tool_name": "nomos",
            "tool_version": "3.1.0rc2-31-ga2cbb8c",
            "tool_configuration": {"command_line": "nomossa <filepath>"},
        }

        actual_tool = storage.indexer_configuration_get(tool)
        assert actual_tool

        expected_tool = tool.copy()
        del actual_tool["id"]

        assert expected_tool == actual_tool

    def test_indexer_configuration_metadata_get_missing_context(
        self, swh_indexer_storage_with_data: Tuple[IndexerStorageInterface, Any]
    ) -> None:
        storage, data = swh_indexer_storage_with_data
        tool = {
            "tool_name": "swh-metadata-translator",
            "tool_version": "0.0.1",
            "tool_configuration": {"context": "unknown-context"},
        }

        actual_tool = storage.indexer_configuration_get(tool)

        assert actual_tool is None

    def test_indexer_configuration_metadata_get(
        self, swh_indexer_storage_with_data: Tuple[IndexerStorageInterface, Any]
    ) -> None:
        storage, data = swh_indexer_storage_with_data
        tool = {
            "tool_name": "swh-metadata-translator",
            "tool_version": "0.0.1",
            "tool_configuration": {"type": "local", "context": "NpmMapping"},
        }

        storage.indexer_configuration_add([tool])
        actual_tool = storage.indexer_configuration_get(tool)
        assert actual_tool

        expected_tool = tool.copy()
        expected_tool["id"] = actual_tool["id"]

        assert expected_tool == actual_tool
