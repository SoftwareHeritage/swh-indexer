# Copyright (C) 2015-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.core.api import RPCClient

from swh.storage.exc import StorageAPIError

from .. import IndexerStorage


class RemoteStorage(RPCClient):
    """Proxy to a remote storage API"""

    backend_class = IndexerStorage
    api_exception = StorageAPIError
