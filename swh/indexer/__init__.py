# Copyright (C) 2016-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


# implemented as a function to help lazy loading
def get_datastore(*args, **kw):
    from .indexer import get_indexer_storage

    return get_indexer_storage(*args, **kw)
