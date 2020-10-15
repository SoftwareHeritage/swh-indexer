# Copyright (C) 2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import inspect

import pytest

from swh.indexer.storage import IndexerStorage, get_indexer_storage
from swh.indexer.storage.api.client import RemoteStorage
from swh.indexer.storage.in_memory import IndexerStorage as MemoryIndexerStorage
from swh.indexer.storage.interface import IndexerStorageInterface

SERVER_IMPLEMENTATIONS_KWARGS = [
    ("remote", RemoteStorage, {"url": "localhost"}),
    ("local", IndexerStorage, {"db": "something"}),
]

SERVER_IMPLEMENTATIONS = SERVER_IMPLEMENTATIONS_KWARGS + [
    ("memory", MemoryIndexerStorage, {}),
]


@pytest.fixture
def mock_psycopg2(mocker):
    mocker.patch("swh.indexer.storage.psycopg2.pool")
    return mocker


def test_init_get_indexer_storage_failure():
    with pytest.raises(ValueError, match="Unknown indexer storage class"):
        get_indexer_storage("unknown-idx-storage")


@pytest.mark.parametrize("class_name,expected_class,kwargs", SERVER_IMPLEMENTATIONS)
def test_init_get_indexer_storage(class_name, expected_class, kwargs, mock_psycopg2):
    if kwargs:
        concrete_idx_storage = get_indexer_storage(class_name, **kwargs)
    else:
        concrete_idx_storage = get_indexer_storage(class_name)
    assert isinstance(concrete_idx_storage, expected_class)
    assert isinstance(concrete_idx_storage, IndexerStorageInterface)


@pytest.mark.parametrize(
    "class_name,expected_class,kwargs", SERVER_IMPLEMENTATIONS_KWARGS
)
def test_init_get_indexer_storage_deprecation_warning(
    class_name, expected_class, kwargs, mock_psycopg2
):
    with pytest.warns(DeprecationWarning):
        concrete_idx_storage = get_indexer_storage(class_name, args=kwargs)
    assert isinstance(concrete_idx_storage, expected_class)


def test_types(swh_indexer_storage) -> None:
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

    # If all the assertions above succeed, then this one should too.
    # But there's no harm in double-checking.
    # And we could replace the assertions above by this one, but unlike
    # the assertions above, it doesn't explain what is missing.
    assert isinstance(swh_indexer_storage, IndexerStorageInterface)
