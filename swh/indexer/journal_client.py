# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

from swh.journal.client import JournalClient
from swh.scheduler import get_scheduler
from swh.scheduler.utils import create_task_dict


class IndexerJournalClient(JournalClient):
    """Client in charge of listing new received origins and origin_visits
       in the swh journal.

    """
    CONFIG_BASE_FILENAME = 'indexer/journal_client'

    ADDITIONAL_CONFIG = {
        'scheduler': ('dict', {
            'cls': 'remote',
            'args': {
                'url': 'http://localhost:5008/',
            }
        }),
        'origin_visit_tasks': ('List[dict]', [
            {
                'type': 'index-origin-metadata',
                'kwargs': {
                    'policy_update': 'update-dups',
                    'parse_ids': False,
                }
            }
        ]),
    }

    def __init__(self):
        super().__init__(extra_configuration={
            'object_types': ['origin_visit'],
        })
        self.scheduler = get_scheduler(**self.config['scheduler'])
        logging.info(
            'Starting indexer journal client with config %r',
            self.config)

    def process_objects(self, messages):
        assert set(messages) == {'origin_visit'}, set(messages)
        for origin_visit in messages['origin_visit']:
            self.process_origin_visit(origin_visit)

    def process_origin_visit(self, origin_visit):
        task_dicts = []
        logging.debug('processing origin visit %r', origin_visit)
        if origin_visit[b'status'] == b'full':
            for task_config in self.config['origin_visit_tasks']:
                logging.info(
                    'Scheduling %s for visit of origin %d',
                    task_config['type'], origin_visit[b'origin'])
                task_dicts.append(create_task_dict(
                    task_config['type'],
                    'oneshot',
                    [origin_visit[b'origin']],
                    **task_config['kwargs'],
                ))
        else:
            logging.debug('status is not "full", ignoring.')

        if task_dicts:
            self.scheduler.create_tasks(task_dicts)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(process)d %(levelname)s %(message)s'
    )

    import click

    @click.command()
    def main():
        """Log the new received origin and origin_visits.

        """
        IndexerJournalClient().process()

    main()
