from os import path
import swh.indexer

from celery import shared_task
from celery.contrib.testing.worker import _start_worker_thread
from celery import current_app

__all__ = ['start_worker_thread']

SQL_DIR = path.join(path.dirname(swh.indexer.__file__), 'sql')


def start_worker_thread():
    return _start_worker_thread(current_app)


# Needed to pass an assertion, see
# https://github.com/celery/celery/pull/5111
@shared_task(name='celery.ping')
def ping():
    return 'pong'
