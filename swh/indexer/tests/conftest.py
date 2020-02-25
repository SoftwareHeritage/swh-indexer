# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import timedelta
from unittest.mock import patch

import pytest

from swh.objstorage import get_objstorage
from swh.scheduler.tests.conftest import *  # noqa
from swh.storage import get_storage
from swh.indexer.storage import get_indexer_storage

from .utils import fill_storage, fill_obj_storage


TASK_NAMES = ['revision_intrinsic_metadata', 'origin_intrinsic_metadata']


storage_config = {
    'cls': 'pipeline',
    'steps': [
        {'cls': 'validate'},
        {'cls': 'memory'},
    ]
}


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
    """An instance of in-memory indexer storage that gets injected into all
    indexers classes.

    """
    idx_storage = get_indexer_storage('memory', {})
    with patch('swh.indexer.storage.in_memory.IndexerStorage') \
            as idx_storage_mock:
        idx_storage_mock.return_value = idx_storage
        yield idx_storage


@pytest.fixture
def storage():
    """An instance of in-memory storage that gets injected into all indexers
       classes.

    """
    storage = get_storage(**storage_config)
    fill_storage(storage)
    with patch('swh.storage.in_memory.InMemoryStorage') as storage_mock:
        storage_mock.return_value = storage
        yield storage


@pytest.fixture
def obj_storage():
    """An instance of in-memory objstorage that gets injected into all indexers
    classes.

    """
    objstorage = get_objstorage('memory', {})
    fill_obj_storage(objstorage)
    with patch.dict('swh.objstorage._STORAGE_CLASSES',
                    {'memory': lambda: objstorage}):
        yield objstorage


@pytest.fixture(scope='session')  # type: ignore  # expected redefinition
def celery_includes():
    return [
        'swh.indexer.tests.tasks',
        'swh.indexer.tasks',
    ]
