import swh.indexer
from os import path

from celery.contrib.testing.worker import start_worker
import celery.contrib.testing.tasks  # noqa

from swh.scheduler.celery_backend.config import app

__all__ = ["start_worker_thread"]

SQL_DIR = path.join(path.dirname(swh.indexer.__file__), "sql")


def start_worker_thread():
    return start_worker(app)
