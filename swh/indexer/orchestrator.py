# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import random

from celery import group

from swh.core.config import SWHConfig
from swh.scheduler.celery_backend.config import app
from . import TASK_NAMES, INDEXER_CLASSES


class BaseOrchestratorIndexer(SWHConfig):
    """The indexer orchestrator is in charge of:
    - reading batch of contents (list of sha1s as bytes)
    - according to its configuration, filter or not the contents
    - and then broadcast those contents to indexers

    """
    CONFIG_BASE_FILENAME = 'indexer/orchestrator'

    DEFAULT_CONFIG = {
        'indexers': ('[str]', ['mimetype']),
        'check_presence': ('bool', 'true'),
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
        self.check_presence = self.config['check_presence']

    def run_with_check(self, sha1s):
        """Run with checking the presence on sha1s in db to filter them out.

        """
        celery_tasks = []
        for task_name, indexer_class in self.indexers.items():
            indexer = indexer_class()

            # filter the contents per indexers
            sha1s_filtered = list(indexer.filter_contents(sha1s))

            if not sha1s_filtered:
                continue

            # send message for indexer to compute and store results on
            # filtered sha1s
            celery_task = app.tasks[task_name].s(sha1s=sha1s_filtered,
                                                 policy_update='ignore-dups')
            celery_tasks.append(celery_task)

        return celery_tasks

    def run_no_check(self, sha1s):
        """Simply broadcast sha1s to the indexers' queue.

        """
        celery_tasks = []
        for task_name, _ in self.indexers.items():
            # send message for indexer to compute and store results
            celery_task = app.tasks[task_name].s(sha1s=sha1s,
                                                 policy_update='update-dups')
            celery_tasks.append(celery_task)

        return celery_tasks

    def run(self, sha1s):
        if self.check_presence:
            celery_tasks = self.run_with_check(sha1s)
        else:
            celery_tasks = self.run_no_check(sha1s)

        group(celery_tasks).delay()


class OrchestratorAllContentsIndexer(BaseOrchestratorIndexer):
    """Orchestrator which deals with batch of any types of contents.

    """


class OrchestratorTextContentsIndexer(BaseOrchestratorIndexer):
    """Orchestrator which deals with batch of text contents.

    """
    CONFIG_BASE_FILENAME = 'indexer/orchestrator_text'
