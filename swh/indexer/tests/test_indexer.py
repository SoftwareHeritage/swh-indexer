# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Any, Dict, Optional, Union

import pytest

from swh.indexer.indexer import ContentIndexer
from swh.model.model import Revision

from .utils import BASE_TEST_CONFIG


class TestException(Exception):
    pass


class CrashingIndexer(ContentIndexer):
    USE_TOOLS = False

    def index(
        self, id: Union[bytes, Dict, Revision], data: Optional[bytes] = None, **kwargs
    ) -> Dict[str, Any]:
        pass

    def persist_index_computations(self, results, policy_update) -> Dict[str, int]:
        raise TestException()


def test_catch_exceptions():
    indexer = CrashingIndexer(config=BASE_TEST_CONFIG)

    assert indexer.run([b"foo"], policy_update=True) == {"status": "failed"}

    indexer.catch_exceptions = False

    with pytest.raises(TestException):
        indexer.run([b"foo"], policy_update=True)
