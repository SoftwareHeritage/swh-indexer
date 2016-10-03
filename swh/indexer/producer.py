# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import random

from swh.core.config import SWHConfig
from swh.core import hashutil, utils
from swh.objstorage import get_objstorage
from swh.storage import get_storage
from swh.scheduler.celery_backend.config import app
from . import tasks

task_name = 'swh.indexer.tasks.SWHReaderTask'
task_destination = 'swh.indexer.tasks.SWHFilePropertiesTask'

reader_task = app.tasks[task_name]


class ContentIndexerProducer(SWHConfig):
    DEFAULT_CONFIG = {
        'storage': ('dict', {
            'cls': 'remote_storage',
            'args': ['http://localhost:5000/'],
        }),
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

    CONFIG_BASE_FILENAME = 'indexer/reader'

    def __init__(self):
        super().__init__()
        self.config = self.parse_config_file()
        storage = self.config['storage']
        self.storage = get_storage(storage['cls'], storage['args'])
        objstorage = self.config['objstorage']
        self.objstorage = get_objstorage(objstorage['cls'], objstorage['args'])

        self.limit = self.config['limit']
        if self.limit == 'none':
            self.limit = None
        else:
            self.limit = int(self.limit)
        self.batch = self.config['batch']

    def _get_random_name(self, sha1, revision_paths, total_retry=10):
        """Retrieve a random name which is utf-8 decodable.

        """
        retry = 0
        while retry <= total_retry:
            name = random.choice(revision_paths)
            try:
                return name.decode('utf-8')
            except UnicodeDecodeError as e:
                print('sha1 %s with path %s is not utf-8 decodable - %s' % (
                    sha1, name, e))
                pass
            retry += 1

    def get_contents(self):
        """Read contents and retrieve randomly one possible path.

        """
        for sha1 in self.objstorage:
            c = self.storage.cache_content_get({'sha1': sha1})
            if not c:
                print('No reference found for %s' % sha1)
                continue
            revision_paths = [
                os.path.basename(path) for _, path in c['revision_paths']
            ]

            name = self._get_random_name(sha1, revision_paths)
            if not name:  # nothing found, drop that content (for now)
                print('No valid path found for %s' % sha1)
                continue

            yield {
                'sha1': hashutil.hash_to_hex(sha1),
                'name': name,
            }

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
            reader_task.delay(sha1s, task_destination)
            if count >= self.limit:
                return

    def run_no_limit(self):
        for sha1s in self.gen_sha1():
            print('%s sent - [%s, ...]' % (len(sha1s), sha1s[0]))
            reader_task.delay(sha1s, task_destination)

    def run(self, *args, **kwargs):
        if self.limit:
            self.run_with_limit()
        else:
            self.run_no_limit()

if __name__ == '__main__':
    ContentIndexerProducer().run()
