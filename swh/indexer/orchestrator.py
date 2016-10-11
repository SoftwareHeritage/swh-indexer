# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import random

from swh.core.config import SWHConfig
from swh.scheduler.celery_backend.config import app

from . import TASK_NAMES, INDEXER_CLASSES


class OrchestratorIndexer(SWHConfig):
    """The indexer orchestrator is in charge of:
    - reading batch of contents (list of sha1s as bytes)
    - according to its configuration, filter or not the contents
    - and then broadcast those contents to indexers

    """
    CONFIG_BASE_FILENAME = 'indexer/orchestrator'

    DEFAULT_CONFIG = {
        'indexers': ('[str]', ['mimetype']),
    }

    def __init__(self):
        super().__init__()
        self.config = self.parse_config_file()
        indexer_names = self.config['indexers']
        random.shuffle(indexer_names)
        self.indexers = {
            TASK_NAMES[name]: INDEXER_CLASSES[name]
            for name in indexer_names
        }

    def run(self, sha1s):
        for task_name, indexer_class in self.indexers.items():
            indexer = indexer_class()

            # first filter the contents per indexers
            sha1s_filtered = list(indexer.filter_contents(sha1s))

            if not sha1s_filtered:
                continue

            # now send the message for the indexer to compute and store results
            app.tasks[task_name].delay(sha1s_filtered)
