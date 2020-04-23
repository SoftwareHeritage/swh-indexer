# Copyright (C) 2015-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.core.api import RPCClient

from swh.indexer.storage.exc import (
    IndexerStorageAPIError,
    IndexerStorageArgumentException,
    DuplicateId,
)

from ..interface import IndexerStorageInterface


class RemoteStorage(RPCClient):
    """Proxy to a remote storage API"""

    backend_class = IndexerStorageInterface
    api_exception = IndexerStorageAPIError
    reraise_exceptions = [IndexerStorageArgumentException, DuplicateId]
