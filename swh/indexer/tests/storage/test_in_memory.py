# Copyright (C) 2015-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.indexer.storage import get_indexer_storage

from .test_storage import *  # noqa


@pytest.fixture
def swh_indexer_storage():
    return get_indexer_storage(
        "memory",
        journal_writer={
            "cls": "memory",
        },
    )
