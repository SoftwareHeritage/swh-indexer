# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

from swh.scheduler.utils import create_task_dict


def process_journal_objects(messages, *, scheduler, task_names):
    """Worker function for `JournalClient.process(worker_fn)`, after
    currification of `scheduler` and `task_names`."""
    assert set(messages) == {'origin_visit'}, set(messages)
    for origin_visit in messages['origin_visit']:
        process_origin_visit(origin_visit, scheduler, task_names)


def process_origin_visit(origin_visit,  scheduler, task_names):
    task_dicts = []
    logging.debug('processing origin visit %r', origin_visit)
    if origin_visit[b'status'] == b'full':
        if task_names.get('origin_metadata'):
            task_dicts.append(create_task_dict(
                task_names['origin_metadata'],
                'oneshot',
                [origin_visit[b'origin'][b'url']],
                policy_update='update-dups',
            ))
    else:
        logging.debug('status is not "full", ignoring.')

    if task_dicts:
        scheduler.create_tasks(task_dicts)
