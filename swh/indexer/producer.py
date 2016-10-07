# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import random

from swh.core.config import SWHConfig
from swh.core import utils
from swh.objstorage import get_objstorage
from swh.scheduler.celery_backend.config import app
from . import tasks, TASK_NAMES  # noqa

task_name = TASK_NAMES['orchestrator']

orchestrator_task = app.tasks[task_name]


class ContentIndexerProducer(SWHConfig):
    DEFAULT_CONFIG = {
        'objstorage': ('dict', {
            'cls': 'pathslicing',
            'args': {
                'slicing': '0:2/2:4/4:6',
                'root': '/srv/softwareheritage/objects/'
            },
        }),
        'batch': ('int', 10),
        'limit': ('str', 'none'),
    }

    CONFIG_BASE_FILENAME = 'indexer/producer'

    def __init__(self):
        super().__init__()
        self.config = self.parse_config_file()
        objstorage = self.config['objstorage']
        self.objstorage = get_objstorage(objstorage['cls'], objstorage['args'])

        self.limit = self.config['limit']
        if self.limit == 'none':
            self.limit = None
        else:
            self.limit = int(self.limit)
        self.batch = self.config['batch']

    def get_contents(self):
        """Read contents and retrieve randomly one possible path.

        """
        yield from self.objstorage

    def gen_sha1(self):
        """Generate batch of grouped sha1s from the objstorage.

        """
        for sha1s in utils.grouper(self.get_contents(), self.batch):
            sha1s = list(sha1s)
            random.shuffle(sha1s)
            yield sha1s

    def run_with_limit(self):
        count = 0
        for sha1s in self.gen_sha1():
            count += len(sha1s)
            print('%s sent - [%s, ...]' % (len(sha1s), sha1s[0]))
            orchestrator_task.delay(sha1s)
            if count >= self.limit:
                return

    def run_no_limit(self):
        for sha1s in self.gen_sha1():
            print('%s sent - [%s, ...]' % (len(sha1s), sha1s[0]))
            orchestrator_task.delay(sha1s)

    def run(self, *args, **kwargs):
        if self.limit:
            self.run_with_limit()
        else:
            self.run_no_limit()


if __name__ == '__main__':
    ContentIndexerProducer().run()
