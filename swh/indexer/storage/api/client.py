# Copyright (C) 2015-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.core.api import SWHRemoteAPI

from swh.storage.exc import StorageAPIError

from .. import IndexerStorage


class RemoteStorage(SWHRemoteAPI):
    """Proxy to a remote storage API"""

    backend_class = IndexerStorage

    def __init__(self, url, timeout=None):
        super().__init__(
            api_exception=StorageAPIError, url=url, timeout=timeout)
