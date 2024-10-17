# Copyright (C) 2015-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import psycopg2
import pytest

from swh.core.api import RemoteException, TransientRemoteException
from swh.indexer.storage import get_indexer_storage
from swh.indexer.storage.api.client import RemoteStorage
import swh.indexer.storage.api.server as server

from .test_storage import *  # noqa


@pytest.fixture
def app_server(swh_indexer_storage_postgresql):
    server.storage = get_indexer_storage(
        "postgresql",
        db=swh_indexer_storage_postgresql.info.dsn,
        journal_writer={
            "cls": "memory",
        },
    )
    yield server


@pytest.fixture
def app(app_server):
    return app_server.app


@pytest.fixture
def swh_rpc_client_class():
    # these are needed for the swh_indexer_storage_with_data fixture
    assert hasattr(RemoteStorage, "indexer_configuration_add")
    assert hasattr(RemoteStorage, "content_mimetype_add")
    return RemoteStorage


@pytest.fixture
def swh_indexer_storage(swh_rpc_client, app_server):
    # This version of the swh_storage fixture uses the swh_rpc_client fixture
    # to instantiate a RemoteStorage (see swh_rpc_client_class above) that
    # proxies, via the swh.core RPC mechanism, the local (in memory) storage
    # configured in the app fixture above.
    #
    # Also note that, for the sake of
    # making it easier to write tests, the in-memory journal writer of the
    # in-memory backend storage is attached to the RemoteStorage as its
    # journal_writer attribute.
    storage = swh_rpc_client

    journal_writer = getattr(storage, "journal_writer", None)
    storage.journal_writer = app_server.storage.journal_writer
    yield storage
    storage.journal_writer = journal_writer


def test_exception(app_server, swh_indexer_storage, mocker):
    """Checks the client re-raises unknown exceptions as a :exc:`RemoteException`"""
    assert swh_indexer_storage.content_mimetype_get([b"\x01" * 20]) == []
    mocker.patch.object(
        app_server.storage,
        "content_mimetype_get",
        side_effect=ValueError("crash"),
    )
    with pytest.raises(RemoteException) as e:
        swh_indexer_storage.content_mimetype_get([b"\x01" * 20])
    assert not isinstance(e, TransientRemoteException)


def test_operationalerror_exception(app_server, swh_indexer_storage, mocker):
    """Checks the client re-raises as a :exc:`TransientRemoteException`
    rather than the base :exc:`RemoteException`; so the retrying proxy
    retries for longer."""
    assert swh_indexer_storage.content_mimetype_get([b"\x01" * 20]) == []
    mocker.patch.object(
        app_server.storage,
        "content_mimetype_get",
        side_effect=psycopg2.errors.AdminShutdown("cluster is shutting down"),
    )
    with pytest.raises(RemoteException) as excinfo:
        swh_indexer_storage.content_mimetype_get([b"\x01" * 20])
    assert isinstance(excinfo.value, TransientRemoteException)


def test_querycancelled_exception(app_server, swh_indexer_storage, mocker):
    """Checks the client re-raises as a :exc:`TransientRemoteException`
    rather than the base :exc:`RemoteException`; so the retrying proxy
    retries for longer."""
    assert swh_indexer_storage.content_mimetype_get([b"\x01" * 20]) == []
    mocker.patch.object(
        app_server.storage,
        "content_mimetype_get",
        side_effect=psycopg2.errors.QueryCanceled("too big!"),
    )
    with pytest.raises(RemoteException) as excinfo:
        swh_indexer_storage.content_mimetype_get([b"\x01" * 20])
    assert not isinstance(excinfo.value, TransientRemoteException)
