# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


class IndexerStorageAPIError(Exception):
    """Generic error of the indexer storage."""

    pass


class IndexerStorageArgumentException(Exception):
    """Argument passed to an IndexerStorage endpoint is invalid."""

    pass


class DuplicateId(IndexerStorageArgumentException):
    """The same identifier is present more than once."""

    pass
