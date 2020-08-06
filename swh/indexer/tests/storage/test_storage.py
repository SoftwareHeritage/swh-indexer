# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import inspect
import math
import threading

from typing import Dict

import pytest

from swh.model.hashutil import hash_to_bytes

from swh.indexer.storage.exc import (
    IndexerStorageArgumentException,
    DuplicateId,
)
from swh.indexer.storage.interface import IndexerStorageInterface


def prepare_mimetypes_from(fossology_licenses):
    """Fossology license needs some consistent data in db to run.

    """
    mimetypes = []
    for c in fossology_licenses:
        mimetypes.append(
            {
                "id": c["id"],
                "mimetype": "text/plain",  # for filtering on textual data to work
                "encoding": "utf-8",
                "indexer_configuration_id": c["indexer_configuration_id"],
            }
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


def test_check_config(swh_indexer_storage):
    assert swh_indexer_storage.check_config(check_write=True)
    assert swh_indexer_storage.check_config(check_write=False)


def test_types(swh_indexer_storage):
    """Checks all methods of StorageInterface are implemented by this
    backend, and that they have the same signature."""
    # Create an instance of the protocol (which cannot be instantiated
    # directly, so this creates a subclass, then instantiates it)
    interface = type("_", (IndexerStorageInterface,), {})()

    assert "content_mimetype_add" in dir(interface)

    missing_methods = []

    for meth_name in dir(interface):
        if meth_name.startswith("_"):
            continue
        interface_meth = getattr(interface, meth_name)
        try:
            concrete_meth = getattr(swh_indexer_storage, meth_name)
        except AttributeError:
            missing_methods.append(meth_name)
            continue

        expected_signature = inspect.signature(interface_meth)
        actual_signature = inspect.signature(concrete_meth)

        assert expected_signature == actual_signature, meth_name

    assert missing_methods == []


class StorageETypeTester:
    """Base class for testing a series of common behaviour between a bunch of
    endpoint types supported by an IndexerStorage.

    This is supposed to be inherited with the following class attributes:
    - endpoint_type
    - tool_name
    - example_data

    See below for example usage.
    """

    def test_missing(self, swh_indexer_storage_with_data):
        storage, data = swh_indexer_storage_with_data
        etype = self.endpoint_type
        tool_id = data.tools[self.tool_name]["id"]

        # given 2 (hopefully) unknown objects
        query = [
            {"id": data.sha1_1, "indexer_configuration_id": tool_id,},
            {"id": data.sha1_2, "indexer_configuration_id": tool_id,},
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
                {
                    "id": data.sha1_2,
                    **self.example_data[0],
                    "indexer_configuration_id": tool_id,
                }
            ]
        )

        assert summary == expected_summary(1, etype)

        # we expect only the other one returned
        actual_missing = endpoint(storage, etype, "missing")(query)
        assert list(actual_missing) == [data.sha1_1]

    def test_add__drop_duplicate(self, swh_indexer_storage_with_data):
        storage, data = swh_indexer_storage_with_data
        etype = self.endpoint_type
        tool_id = data.tools[self.tool_name]["id"]

        # add the first object
        data_v1 = {
            "id": data.sha1_2,
            **self.example_data[0],
            "indexer_configuration_id": tool_id,
        }
        summary = endpoint(storage, etype, "add")([data_v1])
        assert summary == expected_summary(1, etype)

        # should be able to retrieve it
        actual_data = list(endpoint(storage, etype, "get")([data.sha1_2]))
        expected_data_v1 = [
            {
                "id": data.sha1_2,
                **self.example_data[0],
                "tool": data.tools[self.tool_name],
            }
        ]
        assert actual_data == expected_data_v1

        # now if we add a modified version of the same object (same id)
        data_v2 = data_v1.copy()
        data_v2.update(self.example_data[1])
        summary2 = endpoint(storage, etype, "add")([data_v2])
        assert summary2 == expected_summary(0, etype)  # not added

        # we expect to retrieve the original data, not the modified one
        actual_data = list(endpoint(storage, etype, "get")([data.sha1_2]))
        assert actual_data == expected_data_v1

    def test_add__update_in_place_duplicate(self, swh_indexer_storage_with_data):
        storage, data = swh_indexer_storage_with_data
        etype = self.endpoint_type
        tool = data.tools[self.tool_name]

        data_v1 = {
            "id": data.sha1_2,
            **self.example_data[0],
            "indexer_configuration_id": tool["id"],
        }

        # given
        summary = endpoint(storage, etype, "add")([data_v1])
        assert summary == expected_summary(1, etype)  # not added

        # when
        actual_data = list(endpoint(storage, etype, "get")([data.sha1_2]))

        expected_data_v1 = [{"id": data.sha1_2, **self.example_data[0], "tool": tool,}]

        # then
        assert actual_data == expected_data_v1

        # given
        data_v2 = data_v1.copy()
        data_v2.update(self.example_data[1])

        endpoint(storage, etype, "add")([data_v2], conflict_update=True)
        assert summary == expected_summary(1, etype)  # modified so counted

        actual_data = list(endpoint(storage, etype, "get")([data.sha1_2]))

        expected_data_v2 = [{"id": data.sha1_2, **self.example_data[1], "tool": tool,}]

        # data did change as the v2 was used to overwrite v1
        assert actual_data == expected_data_v2

    def test_add__update_in_place_deadlock(self, swh_indexer_storage_with_data):
        storage, data = swh_indexer_storage_with_data
        etype = self.endpoint_type
        tool = data.tools[self.tool_name]

        hashes = [
            hash_to_bytes("34973274ccef6ab4dfaaf86599792fa9c3fe4{:03d}".format(i))
            for i in range(1000)
        ]

        data_v1 = [
            {
                "id": hash_,
                **self.example_data[0],
                "indexer_configuration_id": tool["id"],
            }
            for hash_ in hashes
        ]
        data_v2 = [
            {
                "id": hash_,
                **self.example_data[1],
                "indexer_configuration_id": tool["id"],
            }
            for hash_ in hashes
        ]

        # Remove one item from each, so that both queries have to succeed for
        # all items to be in the DB.
        data_v2a = data_v2[1:]
        data_v2b = list(reversed(data_v2[0:-1]))

        # given
        endpoint(storage, etype, "add")(data_v1)

        # when
        actual_data = list(endpoint(storage, etype, "get")(hashes))

        expected_data_v1 = [
            {"id": hash_, **self.example_data[0], "tool": tool,} for hash_ in hashes
        ]

        # then
        assert actual_data == expected_data_v1

        # given
        def f1():
            endpoint(storage, etype, "add")(data_v2a, conflict_update=True)

        def f2():
            endpoint(storage, etype, "add")(data_v2b, conflict_update=True)

        t1 = threading.Thread(target=f1)
        t2 = threading.Thread(target=f2)
        t2.start()
        t1.start()

        t1.join()
        t2.join()

        actual_data = sorted(
            endpoint(storage, etype, "get")(hashes), key=lambda x: x["id"]
        )

        expected_data_v2 = [
            {"id": hash_, **self.example_data[1], "tool": tool,} for hash_ in hashes
        ]

        assert actual_data == expected_data_v2

    def test_add__duplicate_twice(self, swh_indexer_storage_with_data):
        storage, data = swh_indexer_storage_with_data
        etype = self.endpoint_type
        tool = data.tools[self.tool_name]

        data_rev1 = {
            "id": data.revision_id_2,
            **self.example_data[0],
            "indexer_configuration_id": tool["id"],
        }

        data_rev2 = {
            "id": data.revision_id_2,
            **self.example_data[1],
            "indexer_configuration_id": tool["id"],
        }

        # when
        summary = endpoint(storage, etype, "add")([data_rev1])
        assert summary == expected_summary(1, etype)

        with pytest.raises(DuplicateId):
            endpoint(storage, etype, "add")(
                [data_rev2, data_rev2], conflict_update=True
            )

        # then
        actual_data = list(
            endpoint(storage, etype, "get")([data.revision_id_2, data.revision_id_1])
        )

        expected_data = [
            {"id": data.revision_id_2, **self.example_data[0], "tool": tool,}
        ]
        assert actual_data == expected_data

    def test_get(self, swh_indexer_storage_with_data):
        storage, data = swh_indexer_storage_with_data
        etype = self.endpoint_type
        tool = data.tools[self.tool_name]

        query = [data.sha1_2, data.sha1_1]
        data1 = {
            "id": data.sha1_2,
            **self.example_data[0],
            "indexer_configuration_id": tool["id"],
        }

        # when
        summary = endpoint(storage, etype, "add")([data1])
        assert summary == expected_summary(1, etype)

        # then
        actual_data = list(endpoint(storage, etype, "get")(query))

        # then
        expected_data = [{"id": data.sha1_2, **self.example_data[0], "tool": tool,}]

        assert actual_data == expected_data


class TestIndexerStorageContentMimetypes(StorageETypeTester):
    """Test Indexer Storage content_mimetype related methods
    """

    endpoint_type = "content_mimetype"
    tool_name = "file"
    example_data = [
        {"mimetype": "text/plain", "encoding": "utf-8",},
        {"mimetype": "text/html", "encoding": "us-ascii",},
    ]

    def test_generate_content_mimetype_get_partition_failure(self, swh_indexer_storage):
        """get_partition call with wrong limit input should fail"""
        storage = swh_indexer_storage
        indexer_configuration_id = None
        with pytest.raises(
            IndexerStorageArgumentException, match="limit should not be None"
        ):
            storage.content_mimetype_get_partition(
                indexer_configuration_id, 0, 3, limit=None
            )

    def test_generate_content_mimetype_get_partition_no_limit(
        self, swh_indexer_storage_with_data
    ):
        """get_partition should return result"""
        storage, data = swh_indexer_storage_with_data
        mimetypes = data.mimetypes

        expected_ids = set([c["id"] for c in mimetypes])
        indexer_configuration_id = mimetypes[0]["indexer_configuration_id"]

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
        self, swh_indexer_storage_with_data
    ):
        """get_partition for a single partition should return available ids

        """
        storage, data = swh_indexer_storage_with_data
        mimetypes = data.mimetypes
        expected_ids = set([c["id"] for c in mimetypes])
        indexer_configuration_id = mimetypes[0]["indexer_configuration_id"]

        actual_result = storage.content_mimetype_get_partition(
            indexer_configuration_id, 0, 1
        )
        assert actual_result.next_page_token is None
        actual_ids = actual_result.results
        assert len(actual_ids) == len(expected_ids)
        for actual_id in actual_ids:
            assert actual_id in expected_ids

    def test_generate_content_mimetype_get_partition_empty(
        self, swh_indexer_storage_with_data
    ):
        """get_partition when at least one of the partitions is empty"""
        storage, data = swh_indexer_storage_with_data
        mimetypes = data.mimetypes
        expected_ids = set([c["id"] for c in mimetypes])
        indexer_configuration_id = mimetypes[0]["indexer_configuration_id"]

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
        self, swh_indexer_storage_with_data
    ):
        """get_partition should return ids provided with pagination

        """
        storage, data = swh_indexer_storage_with_data
        mimetypes = data.mimetypes
        expected_ids = set([c["id"] for c in mimetypes])
        indexer_configuration_id = mimetypes[0]["indexer_configuration_id"]

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
    """Test Indexer Storage content_language related methods
    """

    endpoint_type = "content_language"
    tool_name = "pygments"
    example_data = [
        {"lang": "haskell",},
        {"lang": "common-lisp",},
    ]


class TestIndexerStorageContentCTags(StorageETypeTester):
    """Test Indexer Storage content_ctags related methods
    """

    endpoint_type = "content_ctags"
    tool_name = "universal-ctags"
    example_data = [
        {
            "ctags": [
                {"name": "done", "kind": "variable", "line": 119, "lang": "OCaml",}
            ]
        },
        {
            "ctags": [
                {"name": "done", "kind": "variable", "line": 100, "lang": "Python",},
                {"name": "main", "kind": "function", "line": 119, "lang": "Python",},
            ]
        },
    ]

    # the following tests are disabled because CTAGS behaves differently
    @pytest.mark.skip
    def test_add__drop_duplicate(self):
        pass

    @pytest.mark.skip
    def test_add__update_in_place_duplicate(self):
        pass

    @pytest.mark.skip
    def test_add__update_in_place_deadlock(self):
        pass

    @pytest.mark.skip
    def test_add__duplicate_twice(self):
        pass

    @pytest.mark.skip
    def test_get(self):
        pass

    def test_content_ctags_search(self, swh_indexer_storage_with_data):
        storage, data = swh_indexer_storage_with_data
        # 1. given
        tool = data.tools["universal-ctags"]
        tool_id = tool["id"]

        ctag1 = {
            "id": data.sha1_1,
            "indexer_configuration_id": tool_id,
            "ctags": [
                {"name": "hello", "kind": "function", "line": 133, "lang": "Python",},
                {"name": "counter", "kind": "variable", "line": 119, "lang": "Python",},
                {"name": "hello", "kind": "variable", "line": 210, "lang": "Python",},
            ],
        }

        ctag2 = {
            "id": data.sha1_2,
            "indexer_configuration_id": tool_id,
            "ctags": [
                {"name": "hello", "kind": "variable", "line": 100, "lang": "C",},
                {"name": "result", "kind": "variable", "line": 120, "lang": "C",},
            ],
        }

        storage.content_ctags_add([ctag1, ctag2])

        # 1. when
        actual_ctags = list(storage.content_ctags_search("hello", limit=1))

        # 1. then
        assert actual_ctags == [
            {
                "id": ctag1["id"],
                "tool": tool,
                "name": "hello",
                "kind": "function",
                "line": 133,
                "lang": "Python",
            }
        ]

        # 2. when
        actual_ctags = list(
            storage.content_ctags_search("hello", limit=1, last_sha1=ctag1["id"])
        )

        # 2. then
        assert actual_ctags == [
            {
                "id": ctag2["id"],
                "tool": tool,
                "name": "hello",
                "kind": "variable",
                "line": 100,
                "lang": "C",
            }
        ]

        # 3. when
        actual_ctags = list(storage.content_ctags_search("hello"))

        # 3. then
        assert actual_ctags == [
            {
                "id": ctag1["id"],
                "tool": tool,
                "name": "hello",
                "kind": "function",
                "line": 133,
                "lang": "Python",
            },
            {
                "id": ctag1["id"],
                "tool": tool,
                "name": "hello",
                "kind": "variable",
                "line": 210,
                "lang": "Python",
            },
            {
                "id": ctag2["id"],
                "tool": tool,
                "name": "hello",
                "kind": "variable",
                "line": 100,
                "lang": "C",
            },
        ]

        # 4. when
        actual_ctags = list(storage.content_ctags_search("counter"))

        # then
        assert actual_ctags == [
            {
                "id": ctag1["id"],
                "tool": tool,
                "name": "counter",
                "kind": "variable",
                "line": 119,
                "lang": "Python",
            }
        ]

        # 5. when
        actual_ctags = list(storage.content_ctags_search("result", limit=1))

        # then
        assert actual_ctags == [
            {
                "id": ctag2["id"],
                "tool": tool,
                "name": "result",
                "kind": "variable",
                "line": 120,
                "lang": "C",
            }
        ]

    def test_content_ctags_search_no_result(self, swh_indexer_storage):
        storage = swh_indexer_storage
        actual_ctags = list(storage.content_ctags_search("counter"))

        assert not actual_ctags

    def test_content_ctags_add__add_new_ctags_added(
        self, swh_indexer_storage_with_data
    ):
        storage, data = swh_indexer_storage_with_data

        # given
        tool = data.tools["universal-ctags"]
        tool_id = tool["id"]

        ctag_v1 = {
            "id": data.sha1_2,
            "indexer_configuration_id": tool_id,
            "ctags": [
                {"name": "done", "kind": "variable", "line": 100, "lang": "Scheme",}
            ],
        }

        # given
        storage.content_ctags_add([ctag_v1])
        storage.content_ctags_add([ctag_v1])  # conflict does nothing

        # when
        actual_ctags = list(storage.content_ctags_get([data.sha1_2]))

        # then
        expected_ctags = [
            {
                "id": data.sha1_2,
                "name": "done",
                "kind": "variable",
                "line": 100,
                "lang": "Scheme",
                "tool": tool,
            }
        ]

        assert actual_ctags == expected_ctags

        # given
        ctag_v2 = ctag_v1.copy()
        ctag_v2.update(
            {
                "ctags": [
                    {"name": "defn", "kind": "function", "line": 120, "lang": "Scheme",}
                ]
            }
        )

        storage.content_ctags_add([ctag_v2])

        expected_ctags = [
            {
                "id": data.sha1_2,
                "name": "done",
                "kind": "variable",
                "line": 100,
                "lang": "Scheme",
                "tool": tool,
            },
            {
                "id": data.sha1_2,
                "name": "defn",
                "kind": "function",
                "line": 120,
                "lang": "Scheme",
                "tool": tool,
            },
        ]

        actual_ctags = list(storage.content_ctags_get([data.sha1_2]))

        assert actual_ctags == expected_ctags

    def test_content_ctags_add__update_in_place(self, swh_indexer_storage_with_data):
        storage, data = swh_indexer_storage_with_data
        # given
        tool = data.tools["universal-ctags"]
        tool_id = tool["id"]

        ctag_v1 = {
            "id": data.sha1_2,
            "indexer_configuration_id": tool_id,
            "ctags": [
                {"name": "done", "kind": "variable", "line": 100, "lang": "Scheme",}
            ],
        }

        # given
        storage.content_ctags_add([ctag_v1])

        # when
        actual_ctags = list(storage.content_ctags_get([data.sha1_2]))

        # then
        expected_ctags = [
            {
                "id": data.sha1_2,
                "name": "done",
                "kind": "variable",
                "line": 100,
                "lang": "Scheme",
                "tool": tool,
            }
        ]
        assert actual_ctags == expected_ctags

        # given
        ctag_v2 = ctag_v1.copy()
        ctag_v2.update(
            {
                "ctags": [
                    {
                        "name": "done",
                        "kind": "variable",
                        "line": 100,
                        "lang": "Scheme",
                    },
                    {
                        "name": "defn",
                        "kind": "function",
                        "line": 120,
                        "lang": "Scheme",
                    },
                ]
            }
        )

        storage.content_ctags_add([ctag_v2], conflict_update=True)

        actual_ctags = list(storage.content_ctags_get([data.sha1_2]))

        # ctag did change as the v2 was used to overwrite v1
        expected_ctags = [
            {
                "id": data.sha1_2,
                "name": "done",
                "kind": "variable",
                "line": 100,
                "lang": "Scheme",
                "tool": tool,
            },
            {
                "id": data.sha1_2,
                "name": "defn",
                "kind": "function",
                "line": 120,
                "lang": "Scheme",
                "tool": tool,
            },
        ]
        assert actual_ctags == expected_ctags


class TestIndexerStorageContentMetadata(StorageETypeTester):
    """Test Indexer Storage content_metadata related methods
    """

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
        {"metadata": {"other": {}, "name": "test_metadata", "version": "0.0.1"},},
    ]


class TestIndexerStorageRevisionIntrinsicMetadata(StorageETypeTester):
    """Test Indexer Storage revision_intrinsic_metadata related methods
    """

    tool_name = "swh-metadata-detector"
    endpoint_type = "revision_intrinsic_metadata"
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

    def test_revision_intrinsic_metadata_delete(self, swh_indexer_storage_with_data):
        storage, data = swh_indexer_storage_with_data
        etype = self.endpoint_type
        tool = data.tools[self.tool_name]

        query = [data.sha1_2, data.sha1_1]
        data1 = {
            "id": data.sha1_2,
            **self.example_data[0],
            "indexer_configuration_id": tool["id"],
        }

        # when
        summary = endpoint(storage, etype, "add")([data1])
        assert summary == expected_summary(1, etype)

        summary2 = endpoint(storage, etype, "delete")(
            [{"id": data.sha1_2, "indexer_configuration_id": tool["id"],}]
        )
        assert summary2 == expected_summary(1, etype, "del")

        # then
        actual_data = list(endpoint(storage, etype, "get")(query))

        # then
        assert not actual_data

    def test_revision_intrinsic_metadata_delete_nonexisting(
        self, swh_indexer_storage_with_data
    ):
        storage, data = swh_indexer_storage_with_data
        etype = self.endpoint_type
        tool = data.tools[self.tool_name]
        endpoint(storage, etype, "delete")(
            [{"id": data.sha1_2, "indexer_configuration_id": tool["id"],}]
        )


class TestIndexerStorageContentFossologyLicence:
    def test_content_fossology_license_add__new_license_added(
        self, swh_indexer_storage_with_data
    ):
        storage, data = swh_indexer_storage_with_data
        # given
        tool = data.tools["nomos"]
        tool_id = tool["id"]

        license_v1 = {
            "id": data.sha1_1,
            "licenses": ["Apache-2.0"],
            "indexer_configuration_id": tool_id,
        }

        # given
        storage.content_fossology_license_add([license_v1])
        # conflict does nothing
        storage.content_fossology_license_add([license_v1])

        # when
        actual_licenses = list(storage.content_fossology_license_get([data.sha1_1]))

        # then
        expected_license = {data.sha1_1: [{"licenses": ["Apache-2.0"], "tool": tool,}]}
        assert actual_licenses == [expected_license]

        # given
        license_v2 = license_v1.copy()
        license_v2.update(
            {"licenses": ["BSD-2-Clause"],}
        )

        storage.content_fossology_license_add([license_v2])

        actual_licenses = list(storage.content_fossology_license_get([data.sha1_1]))

        expected_license = {
            data.sha1_1: [{"licenses": ["Apache-2.0", "BSD-2-Clause"], "tool": tool}]
        }

        # license did not change as the v2 was dropped.
        assert actual_licenses == [expected_license]

    def test_generate_content_fossology_license_get_partition_failure(
        self, swh_indexer_storage_with_data
    ):
        """get_partition call with wrong limit input should fail"""
        storage, data = swh_indexer_storage_with_data
        indexer_configuration_id = None
        with pytest.raises(
            IndexerStorageArgumentException, match="limit should not be None"
        ):
            storage.content_fossology_license_get_partition(
                indexer_configuration_id, 0, 3, limit=None,
            )

    def test_generate_content_fossology_license_get_partition_no_limit(
        self, swh_indexer_storage_with_data
    ):
        """get_partition should return results"""
        storage, data = swh_indexer_storage_with_data
        # craft some consistent mimetypes
        fossology_licenses = data.fossology_licenses
        mimetypes = prepare_mimetypes_from(fossology_licenses)
        indexer_configuration_id = fossology_licenses[0]["indexer_configuration_id"]

        storage.content_mimetype_add(mimetypes, conflict_update=True)
        # add fossology_licenses to storage
        storage.content_fossology_license_add(fossology_licenses)

        # All ids from the db
        expected_ids = set([c["id"] for c in fossology_licenses])

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
        self, swh_indexer_storage_with_data
    ):
        """get_partition for a single partition should return available ids

        """
        storage, data = swh_indexer_storage_with_data
        # craft some consistent mimetypes
        fossology_licenses = data.fossology_licenses
        mimetypes = prepare_mimetypes_from(fossology_licenses)
        indexer_configuration_id = fossology_licenses[0]["indexer_configuration_id"]

        storage.content_mimetype_add(mimetypes, conflict_update=True)
        # add fossology_licenses to storage
        storage.content_fossology_license_add(fossology_licenses)

        # All ids from the db
        expected_ids = set([c["id"] for c in fossology_licenses])

        actual_result = storage.content_fossology_license_get_partition(
            indexer_configuration_id, 0, 1
        )
        assert actual_result.next_page_token is None
        actual_ids = actual_result.results
        assert len(set(actual_ids)) == len(expected_ids)
        for actual_id in actual_ids:
            assert actual_id in expected_ids

    def test_generate_content_fossology_license_get_partition_empty(
        self, swh_indexer_storage_with_data
    ):
        """get_partition when at least one of the partitions is empty"""
        storage, data = swh_indexer_storage_with_data
        # craft some consistent mimetypes
        fossology_licenses = data.fossology_licenses
        mimetypes = prepare_mimetypes_from(fossology_licenses)
        indexer_configuration_id = fossology_licenses[0]["indexer_configuration_id"]

        storage.content_mimetype_add(mimetypes, conflict_update=True)
        # add fossology_licenses to storage
        storage.content_fossology_license_add(fossology_licenses)

        # All ids from the db
        expected_ids = set([c["id"] for c in fossology_licenses])

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
        self, swh_indexer_storage_with_data
    ):
        """get_partition should return ids provided with paginationv

        """
        storage, data = swh_indexer_storage_with_data
        # craft some consistent mimetypes
        fossology_licenses = data.fossology_licenses
        mimetypes = prepare_mimetypes_from(fossology_licenses)
        indexer_configuration_id = fossology_licenses[0]["indexer_configuration_id"]

        storage.content_mimetype_add(mimetypes, conflict_update=True)
        # add fossology_licenses to storage
        storage.content_fossology_license_add(fossology_licenses)

        # All ids from the db
        expected_ids = [c["id"] for c in fossology_licenses]

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


class TestIndexerStorageOriginIntrinsicMetadata:
    def test_origin_intrinsic_metadata_get(self, swh_indexer_storage_with_data):
        storage, data = swh_indexer_storage_with_data
        # given
        tool_id = data.tools["swh-metadata-detector"]["id"]

        metadata = {
            "version": None,
            "name": None,
        }
        metadata_rev = {
            "id": data.revision_id_2,
            "metadata": metadata,
            "mappings": ["mapping1"],
            "indexer_configuration_id": tool_id,
        }
        metadata_origin = {
            "id": data.origin_url_1,
            "metadata": metadata,
            "indexer_configuration_id": tool_id,
            "mappings": ["mapping1"],
            "from_revision": data.revision_id_2,
        }

        # when
        storage.revision_intrinsic_metadata_add([metadata_rev])
        storage.origin_intrinsic_metadata_add([metadata_origin])

        # then
        actual_metadata = list(
            storage.origin_intrinsic_metadata_get([data.origin_url_1, "no://where"])
        )

        expected_metadata = [
            {
                "id": data.origin_url_1,
                "metadata": metadata,
                "tool": data.tools["swh-metadata-detector"],
                "from_revision": data.revision_id_2,
                "mappings": ["mapping1"],
            }
        ]

        assert actual_metadata == expected_metadata

    def test_origin_intrinsic_metadata_delete(self, swh_indexer_storage_with_data):
        storage, data = swh_indexer_storage_with_data
        # given
        tool_id = data.tools["swh-metadata-detector"]["id"]

        metadata = {
            "version": None,
            "name": None,
        }
        metadata_rev = {
            "id": data.revision_id_2,
            "metadata": metadata,
            "mappings": ["mapping1"],
            "indexer_configuration_id": tool_id,
        }
        metadata_origin = {
            "id": data.origin_url_1,
            "metadata": metadata,
            "indexer_configuration_id": tool_id,
            "mappings": ["mapping1"],
            "from_revision": data.revision_id_2,
        }
        metadata_origin2 = metadata_origin.copy()
        metadata_origin2["id"] = data.origin_url_2

        # when
        storage.revision_intrinsic_metadata_add([metadata_rev])
        storage.origin_intrinsic_metadata_add([metadata_origin, metadata_origin2])

        storage.origin_intrinsic_metadata_delete(
            [{"id": data.origin_url_1, "indexer_configuration_id": tool_id}]
        )

        # then
        actual_metadata = list(
            storage.origin_intrinsic_metadata_get(
                [data.origin_url_1, data.origin_url_2, "no://where"]
            )
        )
        for item in actual_metadata:
            item["indexer_configuration_id"] = item.pop("tool")["id"]
        assert actual_metadata == [metadata_origin2]

    def test_origin_intrinsic_metadata_delete_nonexisting(
        self, swh_indexer_storage_with_data
    ):
        storage, data = swh_indexer_storage_with_data
        tool_id = data.tools["swh-metadata-detector"]["id"]
        storage.origin_intrinsic_metadata_delete(
            [{"id": data.origin_url_1, "indexer_configuration_id": tool_id}]
        )

    def test_origin_intrinsic_metadata_add_drop_duplicate(
        self, swh_indexer_storage_with_data
    ):
        storage, data = swh_indexer_storage_with_data
        # given
        tool_id = data.tools["swh-metadata-detector"]["id"]

        metadata_v1 = {
            "version": None,
            "name": None,
        }
        metadata_rev_v1 = {
            "id": data.revision_id_1,
            "metadata": metadata_v1.copy(),
            "mappings": [],
            "indexer_configuration_id": tool_id,
        }
        metadata_origin_v1 = {
            "id": data.origin_url_1,
            "metadata": metadata_v1.copy(),
            "indexer_configuration_id": tool_id,
            "mappings": [],
            "from_revision": data.revision_id_1,
        }

        # given
        storage.revision_intrinsic_metadata_add([metadata_rev_v1])
        storage.origin_intrinsic_metadata_add([metadata_origin_v1])

        # when
        actual_metadata = list(
            storage.origin_intrinsic_metadata_get([data.origin_url_1, "no://where"])
        )

        expected_metadata_v1 = [
            {
                "id": data.origin_url_1,
                "metadata": metadata_v1,
                "tool": data.tools["swh-metadata-detector"],
                "from_revision": data.revision_id_1,
                "mappings": [],
            }
        ]

        assert actual_metadata == expected_metadata_v1

        # given
        metadata_v2 = metadata_v1.copy()
        metadata_v2.update(
            {"name": "test_metadata", "author": "MG",}
        )
        metadata_rev_v2 = metadata_rev_v1.copy()
        metadata_origin_v2 = metadata_origin_v1.copy()
        metadata_rev_v2["metadata"] = metadata_v2
        metadata_origin_v2["metadata"] = metadata_v2

        storage.revision_intrinsic_metadata_add([metadata_rev_v2])
        storage.origin_intrinsic_metadata_add([metadata_origin_v2])

        # then
        actual_metadata = list(
            storage.origin_intrinsic_metadata_get([data.origin_url_1])
        )

        # metadata did not change as the v2 was dropped.
        assert actual_metadata == expected_metadata_v1

    def test_origin_intrinsic_metadata_add_update_in_place_duplicate(
        self, swh_indexer_storage_with_data
    ):
        storage, data = swh_indexer_storage_with_data
        # given
        tool_id = data.tools["swh-metadata-detector"]["id"]

        metadata_v1 = {
            "version": None,
            "name": None,
        }
        metadata_rev_v1 = {
            "id": data.revision_id_2,
            "metadata": metadata_v1,
            "mappings": [],
            "indexer_configuration_id": tool_id,
        }
        metadata_origin_v1 = {
            "id": data.origin_url_1,
            "metadata": metadata_v1.copy(),
            "indexer_configuration_id": tool_id,
            "mappings": [],
            "from_revision": data.revision_id_2,
        }

        # given
        storage.revision_intrinsic_metadata_add([metadata_rev_v1])
        storage.origin_intrinsic_metadata_add([metadata_origin_v1])

        # when
        actual_metadata = list(
            storage.origin_intrinsic_metadata_get([data.origin_url_1])
        )

        # then
        expected_metadata_v1 = [
            {
                "id": data.origin_url_1,
                "metadata": metadata_v1,
                "tool": data.tools["swh-metadata-detector"],
                "from_revision": data.revision_id_2,
                "mappings": [],
            }
        ]
        assert actual_metadata == expected_metadata_v1

        # given
        metadata_v2 = metadata_v1.copy()
        metadata_v2.update(
            {"name": "test_update_duplicated_metadata", "author": "MG",}
        )
        metadata_rev_v2 = metadata_rev_v1.copy()
        metadata_origin_v2 = metadata_origin_v1.copy()
        metadata_rev_v2["metadata"] = metadata_v2
        metadata_origin_v2 = {
            "id": data.origin_url_1,
            "metadata": metadata_v2.copy(),
            "indexer_configuration_id": tool_id,
            "mappings": ["npm"],
            "from_revision": data.revision_id_1,
        }

        storage.revision_intrinsic_metadata_add([metadata_rev_v2], conflict_update=True)
        storage.origin_intrinsic_metadata_add(
            [metadata_origin_v2], conflict_update=True
        )

        actual_metadata = list(
            storage.origin_intrinsic_metadata_get([data.origin_url_1])
        )

        expected_metadata_v2 = [
            {
                "id": data.origin_url_1,
                "metadata": metadata_v2,
                "tool": data.tools["swh-metadata-detector"],
                "from_revision": data.revision_id_1,
                "mappings": ["npm"],
            }
        ]

        # metadata did change as the v2 was used to overwrite v1
        assert actual_metadata == expected_metadata_v2

    def test_origin_intrinsic_metadata_add__update_in_place_deadlock(
        self, swh_indexer_storage_with_data
    ):
        storage, data = swh_indexer_storage_with_data
        # given
        tool_id = data.tools["swh-metadata-detector"]["id"]

        ids = list(range(10))

        example_data1 = {
            "metadata": {"version": None, "name": None,},
            "mappings": [],
        }
        example_data2 = {
            "metadata": {"version": "v1.1.1", "name": "foo",},
            "mappings": [],
        }

        metadata_rev_v1 = {
            "id": data.revision_id_2,
            "metadata": {"version": None, "name": None,},
            "mappings": [],
            "indexer_configuration_id": tool_id,
        }

        data_v1 = [
            {
                "id": "file:///tmp/origin%d" % id_,
                "from_revision": data.revision_id_2,
                **example_data1,
                "indexer_configuration_id": tool_id,
            }
            for id_ in ids
        ]
        data_v2 = [
            {
                "id": "file:///tmp/origin%d" % id_,
                "from_revision": data.revision_id_2,
                **example_data2,
                "indexer_configuration_id": tool_id,
            }
            for id_ in ids
        ]

        # Remove one item from each, so that both queries have to succeed for
        # all items to be in the DB.
        data_v2a = data_v2[1:]
        data_v2b = list(reversed(data_v2[0:-1]))

        # given
        storage.revision_intrinsic_metadata_add([metadata_rev_v1])
        storage.origin_intrinsic_metadata_add(data_v1)

        # when
        origins = ["file:///tmp/origin%d" % i for i in ids]
        actual_data = list(storage.origin_intrinsic_metadata_get(origins))

        expected_data_v1 = [
            {
                "id": "file:///tmp/origin%d" % id_,
                "from_revision": data.revision_id_2,
                **example_data1,
                "tool": data.tools["swh-metadata-detector"],
            }
            for id_ in ids
        ]

        # then
        assert actual_data == expected_data_v1

        # given
        def f1():
            storage.origin_intrinsic_metadata_add(data_v2a, conflict_update=True)

        def f2():
            storage.origin_intrinsic_metadata_add(data_v2b, conflict_update=True)

        t1 = threading.Thread(target=f1)
        t2 = threading.Thread(target=f2)
        t2.start()
        t1.start()

        t1.join()
        t2.join()

        actual_data = list(storage.origin_intrinsic_metadata_get(origins))

        expected_data_v2 = [
            {
                "id": "file:///tmp/origin%d" % id_,
                "from_revision": data.revision_id_2,
                **example_data2,
                "tool": data.tools["swh-metadata-detector"],
            }
            for id_ in ids
        ]

        assert len(actual_data) == len(expected_data_v2)
        assert sorted(actual_data, key=lambda x: x["id"]) == expected_data_v2

    def test_origin_intrinsic_metadata_add__duplicate_twice(
        self, swh_indexer_storage_with_data
    ):
        storage, data = swh_indexer_storage_with_data
        # given
        tool_id = data.tools["swh-metadata-detector"]["id"]

        metadata = {
            "developmentStatus": None,
            "name": None,
        }
        metadata_rev = {
            "id": data.revision_id_2,
            "metadata": metadata,
            "mappings": ["mapping1"],
            "indexer_configuration_id": tool_id,
        }
        metadata_origin = {
            "id": data.origin_url_1,
            "metadata": metadata,
            "indexer_configuration_id": tool_id,
            "mappings": ["mapping1"],
            "from_revision": data.revision_id_2,
        }

        # when
        storage.revision_intrinsic_metadata_add([metadata_rev])

        with pytest.raises(DuplicateId):
            storage.origin_intrinsic_metadata_add([metadata_origin, metadata_origin])

    def test_origin_intrinsic_metadata_search_fulltext(
        self, swh_indexer_storage_with_data
    ):
        storage, data = swh_indexer_storage_with_data
        # given
        tool_id = data.tools["swh-metadata-detector"]["id"]

        metadata1 = {
            "author": "John Doe",
        }
        metadata1_rev = {
            "id": data.revision_id_1,
            "metadata": metadata1,
            "mappings": [],
            "indexer_configuration_id": tool_id,
        }
        metadata1_origin = {
            "id": data.origin_url_1,
            "metadata": metadata1,
            "mappings": [],
            "indexer_configuration_id": tool_id,
            "from_revision": data.revision_id_1,
        }
        metadata2 = {
            "author": "Jane Doe",
        }
        metadata2_rev = {
            "id": data.revision_id_2,
            "metadata": metadata2,
            "mappings": [],
            "indexer_configuration_id": tool_id,
        }
        metadata2_origin = {
            "id": data.origin_url_2,
            "metadata": metadata2,
            "mappings": [],
            "indexer_configuration_id": tool_id,
            "from_revision": data.revision_id_2,
        }

        # when
        storage.revision_intrinsic_metadata_add([metadata1_rev])
        storage.origin_intrinsic_metadata_add([metadata1_origin])
        storage.revision_intrinsic_metadata_add([metadata2_rev])
        storage.origin_intrinsic_metadata_add([metadata2_origin])

        # then
        search = storage.origin_intrinsic_metadata_search_fulltext
        assert set([res["id"] for res in search(["Doe"])]) == set(
            [data.origin_url_1, data.origin_url_2]
        )
        assert [res["id"] for res in search(["John", "Doe"])] == [data.origin_url_1]
        assert [res["id"] for res in search(["John"])] == [data.origin_url_1]
        assert not list(search(["John", "Jane"]))

    def test_origin_intrinsic_metadata_search_fulltext_rank(
        self, swh_indexer_storage_with_data
    ):
        storage, data = swh_indexer_storage_with_data
        # given
        tool_id = data.tools["swh-metadata-detector"]["id"]

        # The following authors have "Random Person" to add some more content
        # to the JSON data, to work around normalization quirks when there
        # are few words (rank/(1+ln(nb_words)) is very sensitive to nb_words
        # for small values of nb_words).
        metadata1 = {"author": ["Random Person", "John Doe", "Jane Doe",]}
        metadata1_rev = {
            "id": data.revision_id_1,
            "metadata": metadata1,
            "mappings": [],
            "indexer_configuration_id": tool_id,
        }
        metadata1_origin = {
            "id": data.origin_url_1,
            "metadata": metadata1,
            "mappings": [],
            "indexer_configuration_id": tool_id,
            "from_revision": data.revision_id_1,
        }
        metadata2 = {"author": ["Random Person", "Jane Doe",]}
        metadata2_rev = {
            "id": data.revision_id_2,
            "metadata": metadata2,
            "mappings": [],
            "indexer_configuration_id": tool_id,
        }
        metadata2_origin = {
            "id": data.origin_url_2,
            "metadata": metadata2,
            "mappings": [],
            "indexer_configuration_id": tool_id,
            "from_revision": data.revision_id_2,
        }

        # when
        storage.revision_intrinsic_metadata_add([metadata1_rev])
        storage.origin_intrinsic_metadata_add([metadata1_origin])
        storage.revision_intrinsic_metadata_add([metadata2_rev])
        storage.origin_intrinsic_metadata_add([metadata2_origin])

        # then
        search = storage.origin_intrinsic_metadata_search_fulltext
        assert [res["id"] for res in search(["Doe"])] == [
            data.origin_url_1,
            data.origin_url_2,
        ]
        assert [res["id"] for res in search(["Doe"], limit=1)] == [data.origin_url_1]
        assert [res["id"] for res in search(["John"])] == [data.origin_url_1]
        assert [res["id"] for res in search(["Jane"])] == [
            data.origin_url_2,
            data.origin_url_1,
        ]
        assert [res["id"] for res in search(["John", "Jane"])] == [data.origin_url_1]

    def _fill_origin_intrinsic_metadata(self, swh_indexer_storage_with_data):
        storage, data = swh_indexer_storage_with_data
        tool1_id = data.tools["swh-metadata-detector"]["id"]
        tool2_id = data.tools["swh-metadata-detector2"]["id"]

        metadata1 = {
            "@context": "foo",
            "author": "John Doe",
        }
        metadata1_rev = {
            "id": data.revision_id_1,
            "metadata": metadata1,
            "mappings": ["npm"],
            "indexer_configuration_id": tool1_id,
        }
        metadata1_origin = {
            "id": data.origin_url_1,
            "metadata": metadata1,
            "mappings": ["npm"],
            "indexer_configuration_id": tool1_id,
            "from_revision": data.revision_id_1,
        }
        metadata2 = {
            "@context": "foo",
            "author": "Jane Doe",
        }
        metadata2_rev = {
            "id": data.revision_id_2,
            "metadata": metadata2,
            "mappings": ["npm", "gemspec"],
            "indexer_configuration_id": tool2_id,
        }
        metadata2_origin = {
            "id": data.origin_url_2,
            "metadata": metadata2,
            "mappings": ["npm", "gemspec"],
            "indexer_configuration_id": tool2_id,
            "from_revision": data.revision_id_2,
        }
        metadata3 = {
            "@context": "foo",
        }
        metadata3_rev = {
            "id": data.revision_id_3,
            "metadata": metadata3,
            "mappings": ["npm", "gemspec"],
            "indexer_configuration_id": tool2_id,
        }
        metadata3_origin = {
            "id": data.origin_url_3,
            "metadata": metadata3,
            "mappings": ["pkg-info"],
            "indexer_configuration_id": tool2_id,
            "from_revision": data.revision_id_3,
        }

        storage.revision_intrinsic_metadata_add([metadata1_rev])
        storage.origin_intrinsic_metadata_add([metadata1_origin])
        storage.revision_intrinsic_metadata_add([metadata2_rev])
        storage.origin_intrinsic_metadata_add([metadata2_origin])
        storage.revision_intrinsic_metadata_add([metadata3_rev])
        storage.origin_intrinsic_metadata_add([metadata3_origin])

    def test_origin_intrinsic_metadata_search_by_producer(
        self, swh_indexer_storage_with_data
    ):
        storage, data = swh_indexer_storage_with_data
        self._fill_origin_intrinsic_metadata(swh_indexer_storage_with_data)
        tool1 = data.tools["swh-metadata-detector"]
        tool2 = data.tools["swh-metadata-detector2"]
        endpoint = storage.origin_intrinsic_metadata_search_by_producer

        # test pagination
        # no 'page_token' param, return all origins
        result = endpoint(ids_only=True)
        assert result["origins"] == [
            data.origin_url_1,
            data.origin_url_2,
            data.origin_url_3,
        ]
        assert "next_page_token" not in result

        # 'page_token' is < than origin_1, return everything
        result = endpoint(page_token=data.origin_url_1[:-1], ids_only=True)
        assert result["origins"] == [
            data.origin_url_1,
            data.origin_url_2,
            data.origin_url_3,
        ]
        assert "next_page_token" not in result

        # 'page_token' is origin_3, return nothing
        result = endpoint(page_token=data.origin_url_3, ids_only=True)
        assert not result["origins"]
        assert "next_page_token" not in result

        # test limit argument
        result = endpoint(page_token=data.origin_url_1[:-1], limit=2, ids_only=True)
        assert result["origins"] == [data.origin_url_1, data.origin_url_2]
        assert result["next_page_token"] == result["origins"][-1]

        result = endpoint(page_token=data.origin_url_1, limit=2, ids_only=True)
        assert result["origins"] == [data.origin_url_2, data.origin_url_3]
        assert "next_page_token" not in result

        result = endpoint(page_token=data.origin_url_2, limit=2, ids_only=True)
        assert result["origins"] == [data.origin_url_3]
        assert "next_page_token" not in result

        # test mappings filtering
        result = endpoint(mappings=["npm"], ids_only=True)
        assert result["origins"] == [data.origin_url_1, data.origin_url_2]
        assert "next_page_token" not in result

        result = endpoint(mappings=["npm", "gemspec"], ids_only=True)
        assert result["origins"] == [data.origin_url_1, data.origin_url_2]
        assert "next_page_token" not in result

        result = endpoint(mappings=["gemspec"], ids_only=True)
        assert result["origins"] == [data.origin_url_2]
        assert "next_page_token" not in result

        result = endpoint(mappings=["pkg-info"], ids_only=True)
        assert result["origins"] == [data.origin_url_3]
        assert "next_page_token" not in result

        result = endpoint(mappings=["foobar"], ids_only=True)
        assert not result["origins"]
        assert "next_page_token" not in result

        # test pagination + mappings
        result = endpoint(mappings=["npm"], limit=1, ids_only=True)
        assert result["origins"] == [data.origin_url_1]
        assert result["next_page_token"] == result["origins"][-1]

        # test tool filtering
        result = endpoint(tool_ids=[tool1["id"]], ids_only=True)
        assert result["origins"] == [data.origin_url_1]
        assert "next_page_token" not in result

        result = endpoint(tool_ids=[tool2["id"]], ids_only=True)
        assert sorted(result["origins"]) == [data.origin_url_2, data.origin_url_3]
        assert "next_page_token" not in result

        result = endpoint(tool_ids=[tool1["id"], tool2["id"]], ids_only=True)
        assert sorted(result["origins"]) == [
            data.origin_url_1,
            data.origin_url_2,
            data.origin_url_3,
        ]
        assert "next_page_token" not in result

        # test ids_only=False
        assert endpoint(mappings=["gemspec"])["origins"] == [
            {
                "id": data.origin_url_2,
                "metadata": {"@context": "foo", "author": "Jane Doe",},
                "mappings": ["npm", "gemspec"],
                "tool": tool2,
                "from_revision": data.revision_id_2,
            }
        ]

    def test_origin_intrinsic_metadata_stats(self, swh_indexer_storage_with_data):
        storage, data = swh_indexer_storage_with_data
        self._fill_origin_intrinsic_metadata(swh_indexer_storage_with_data)

        result = storage.origin_intrinsic_metadata_stats()
        assert result == {
            "per_mapping": {
                "gemspec": 1,
                "npm": 2,
                "pkg-info": 1,
                "codemeta": 0,
                "maven": 0,
            },
            "total": 3,
            "non_empty": 2,
        }


class TestIndexerStorageIndexerCondifuration:
    def test_indexer_configuration_add(self, swh_indexer_storage_with_data):
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

    def test_indexer_configuration_add_multiple(self, swh_indexer_storage_with_data):
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

    def test_indexer_configuration_get_missing(self, swh_indexer_storage_with_data):
        storage, data = swh_indexer_storage_with_data
        tool = {
            "tool_name": "unknown-tool",
            "tool_version": "3.1.0rc2-31-ga2cbb8c",
            "tool_configuration": {"command_line": "nomossa <filepath>"},
        }

        actual_tool = storage.indexer_configuration_get(tool)

        assert actual_tool is None

    def test_indexer_configuration_get(self, swh_indexer_storage_with_data):
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
        self, swh_indexer_storage_with_data
    ):
        storage, data = swh_indexer_storage_with_data
        tool = {
            "tool_name": "swh-metadata-translator",
            "tool_version": "0.0.1",
            "tool_configuration": {"context": "unknown-context"},
        }

        actual_tool = storage.indexer_configuration_get(tool)

        assert actual_tool is None

    def test_indexer_configuration_metadata_get(self, swh_indexer_storage_with_data):
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
