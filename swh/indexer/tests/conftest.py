import pytest
from datetime import timedelta
from swh.scheduler.tests.conftest import *  # noqa


TASK_NAMES = ['revision_metadata', 'origin_intrinsic_metadata']


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


@pytest.fixture(scope='session')
def celery_includes():
    return [
        'swh.indexer.tests.tasks',
        'swh.indexer.tasks',
    ]
