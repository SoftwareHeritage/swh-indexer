import pytest

from swh.indexer.storage import get_indexer_storage

from .test_storage import *  # noqa


@pytest.fixture
def swh_indexer_storage(swh_indexer_storage_postgresql):
    storage_config = {
        'cls': 'local',
        'args': {
            'db': swh_indexer_storage_postgresql.dsn,
        },
    }
    return get_indexer_storage(**storage_config)
