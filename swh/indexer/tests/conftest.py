from datetime import timedelta
from unittest.mock import patch

import pytest

from swh.objstorage import get_objstorage
from swh.scheduler.tests.conftest import *  # noqa
from swh.storage.in_memory import Storage

from swh.indexer.storage.in_memory import IndexerStorage

from .utils import fill_storage, fill_obj_storage


TASK_NAMES = ['revision_intrinsic_metadata', 'origin_intrinsic_metadata']


@pytest.fixture
def indexer_scheduler(swh_scheduler):
    for taskname in TASK_NAMES:
        swh_scheduler.create_task_type({
            'type': taskname,
            'description': 'The {} indexer testing task'.format(taskname),
            'backend_name': 'swh.indexer.tests.tasks.{}'.format(taskname),
            'default_interval': timedelta(days=1),
            'min_interval': timedelta(hours=6),
            'max_interval': timedelta(days=12),
            'num_retries': 3,
        })
    return swh_scheduler


@pytest.fixture
def idx_storage():
    """An instance of swh.indexer.storage.in_memory.IndexerStorage that
    gets injected into all indexers classes."""
    idx_storage = IndexerStorage()
    with patch('swh.indexer.storage.in_memory.IndexerStorage') \
            as idx_storage_mock:
        idx_storage_mock.return_value = idx_storage
        yield idx_storage


@pytest.fixture
def storage():
    """An instance of swh.storage.in_memory.Storage that gets injected
    into all indexers classes."""
    storage = Storage()
    fill_storage(storage)
    with patch('swh.storage.in_memory.Storage') as storage_mock:
        storage_mock.return_value = storage
        yield storage


@pytest.fixture
def obj_storage():
    """An instance of swh.objstorage.objstorage_in_memory.InMemoryObjStorage
    that gets injected into all indexers classes."""
    objstorage = get_objstorage('memory', {})
    fill_obj_storage(objstorage)
    with patch.dict('swh.objstorage._STORAGE_CLASSES',
                    {'memory': lambda: objstorage}):
        yield objstorage


@pytest.fixture(scope='session')
def celery_includes():
    return [
        'swh.indexer.tests.tasks',
        'swh.indexer.tasks',
    ]
