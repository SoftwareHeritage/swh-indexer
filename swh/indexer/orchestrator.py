# Copyright (C) 2016-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import random

from celery import group

from swh.core.config import SWHConfig
from swh.core.utils import grouper
from swh.scheduler import utils
from . import TASK_NAMES, INDEXER_CLASSES


def get_class(clazz):
    """Get a symbol class dynamically by its fully qualified name string
       representation.

    """
    parts = clazz.split('.')
    module = '.'.join(parts[:-1])
    m = __import__(module)
    for comp in parts[1:]:
        m = getattr(m, comp)
    return m


class BaseOrchestratorIndexer(SWHConfig):
    """The indexer orchestrator is in charge of dispatching batch of
    contents (filtered or not based on presence) to indexers.

    That dispatch is indexer specific, so the configuration reflects it:

    - when `check_presence` flag is true, filter out the
      contents already present for that indexer, otherwise send
      everything

    - broadcast those (filtered or not) contents to indexers in a
      `batch_size` fashioned

    For example::

        indexers:
          mimetype:
            batch_size: 10
            check_presence: false
          language:
            batch_size: 2
            check_presence: true

    means:

    - send all contents received as batch of size 10 to the 'mimetype' indexer
    - send only unknown contents as batch of size 2 to the 'language' indexer.

    """
    CONFIG_BASE_FILENAME = 'indexer/orchestrator'

    DEFAULT_CONFIG = {
        'indexers': ('dict', {
            'mimetype': {
                'batch_size': 10,
                'check_presence': True,
            },
        }),
    }

    def __init__(self):
        super().__init__()
        self.config = self.parse_config_file()
        indexer_names = list(self.config['indexers'].keys())
        random.shuffle(indexer_names)

        indexers = {}
        tasks = {}
        for name in indexer_names:
            if name not in TASK_NAMES:
                raise ValueError('%s must be one of %s' % (
                    name, TASK_NAMES.keys()))

            opts = self.config['indexers'][name]
            indexers[name] = (
                INDEXER_CLASSES[name],
                opts['check_presence'],
                opts['batch_size'])
            tasks[name] = utils.get_task(TASK_NAMES[name])

        self.indexers = indexers
        self.tasks = tasks

    def run(self, ids):
        for name, (idx_class, filtering, batch_size) in self.indexers.items():
            if filtering:
                policy_update = 'ignore-dups'
                indexer_class = get_class(idx_class)
                ids_filtered = list(indexer_class().filter(ids))
                if not ids_filtered:
                    continue
            else:
                policy_update = 'update-dups'
                ids_filtered = ids

            celery_tasks = []
            for ids_to_send in grouper(ids_filtered, batch_size):
                celery_task = self.tasks[name].s(
                    ids=list(ids_to_send),
                    policy_update=policy_update)
                celery_tasks.append(celery_task)

            group(celery_tasks).delay()


class OrchestratorAllContentsIndexer(BaseOrchestratorIndexer):
    """Orchestrator which deals with batch of any types of contents.

    """


class OrchestratorTextContentsIndexer(BaseOrchestratorIndexer):
    """Orchestrator which deals with batch of text contents.

    """
    CONFIG_BASE_FILENAME = 'indexer/orchestrator_text'
