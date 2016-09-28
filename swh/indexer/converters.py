# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


def content_to_storage(content):
    """Only keep the data we want to store.

    In effect, dropping the data property.

    Args:
        content (dict): content

    """
    content_to_store = content.copy()
    content_to_store.pop('data', None)
    return content_to_store
