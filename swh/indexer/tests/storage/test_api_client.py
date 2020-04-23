# Copyright (C) 2015-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.indexer.storage.api.client import RemoteStorage
import swh.indexer.storage.api.server as server

from swh.indexer.storage import get_indexer_storage

from .test_storage import *  # noqa


@pytest.fixture
def app(swh_indexer_storage_postgresql):
    storage_config = {
        "cls": "local",
        "args": {"db": swh_indexer_storage_postgresql.dsn,},
    }
    server.storage = get_indexer_storage(**storage_config)
    return server.app


@pytest.fixture
def swh_rpc_client_class():
    # these are needed for the swh_indexer_storage_with_data fixture
    assert hasattr(RemoteStorage, "indexer_configuration_add")
    assert hasattr(RemoteStorage, "content_mimetype_add")
    return RemoteStorage


@pytest.fixture
def swh_indexer_storage(swh_rpc_client, app):
    # This version of the swh_storage fixture uses the swh_rpc_client fixture
    # to instantiate a RemoteStorage (see swh_rpc_client_class above) that
    # proxies, via the swh.core RPC mechanism, the local (in memory) storage
    # configured in the app fixture above.
    return swh_rpc_client
