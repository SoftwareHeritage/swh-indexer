# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import random

from swh.core.config import SWHConfig
from swh.core import hashutil, utils
from swh.objstorage import get_objstorage
from swh.indexer import tasks  # noqa
from swh.scheduler.celery_backend.config import app

task_name = 'swh.indexer.tasks.SWHReaderTask'
task_destination = 'swh.indexer.tasks.SWHMimeTypeTask'

task1 = app.tasks[task_name]


class BasicProducer(SWHConfig):
    DEFAULT_CONFIG = {
        'storage': ('dict', {
            'cls': 'pathslicing',
            'args': {
                'slicing': '0:2/2:4/4:6',
                'root': '/srv/softwareheritage/objects/'
            },
        }),
        'batch': ('int', 10),
        'limit': ('str', 'none'),
    }

    CONFIG_BASE_FILENAME = 'indexer/reader'

    def __init__(self):
        super().__init__()
        self.config = self.parse_config_file()
        storage = self.config['storage']
        self.objstorage = get_objstorage(storage['cls'], storage['args'])
        self.limit = self.config['limit']
        if self.limit == 'none':
            self.limit = None
        else:
            self.limit = int(self.limit)
        self.batch = self.config['batch']

    def gen_sha1(self):
        """Generate batch of grouped sha1s from the objstorage.

        """
        for sha1s in utils.grouper(({'sha1': hashutil.hash_to_hex(s)}
                                     for s in self.objstorage),
                                   self.batch):
            sha1s = list(sha1s)
            random.shuffle(sha1s)
            yield sha1s

    def run_with_limit(self):
        count = 0
        for sha1s in self.gen_sha1():
            count += len(sha1s)
            print('%s sent - [%s, ...]' % (len(sha1s), sha1s[0]))
            task1.delay(sha1s, task_destination)
            if count >= self.limit:
                return

    def run_no_limit(self):
        for sha1s in self.gen_sha1():
            print('%s sent - [%s, ...]' % (len(sha1s), sha1s[0]))
            task1.delay(sha1s, task_destination)

    def run(self, *args, **kwargs):
        if self.limit:
            self.run_with_limit()
        else:
            self.run_no_limit()

if __name__ == '__main__':
    BasicProducer().run()
