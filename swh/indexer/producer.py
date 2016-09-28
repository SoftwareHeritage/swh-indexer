# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.core.config import SWHConfig
from swh.core import hashutil
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
            }
        })
    }
    CONFIG_BASE_FILENAME = 'indexer/producer'

    def __init__(self):
        super().__init__()
        self.config = self.parse_config_file()
        storage = self.config['storage']
        self.objstorage = get_objstorage(storage['cls'], storage['args'])

    def run(self, *args, **kwargs):
        for sha1 in self.objstorage:
            sha1 = hashutil.hash_to_hex(sha1)
            print('sha1 %s sent' % sha1)
            task1.delay({'sha1': sha1}, task_destination)


if __name__ == '__main__':
    BasicProducer().run()
