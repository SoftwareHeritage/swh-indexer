#!/usr/bin/env python3

from swh.scheduler.celery_backend.config import app
from swh.indexer import tasks  # noqa

task_name = 'swh.indexer.tasks.SWHReaderTask'
task_destination = 'swh.indexer.tasks.SWHMimeTypeTask'

sha1 = '8572f048c91b3f43fdd02d68abc4475bb0488112'

content = {'sha1': sha1}

task1 = app.tasks[task_name]

task1.delay(content, task_destination)
