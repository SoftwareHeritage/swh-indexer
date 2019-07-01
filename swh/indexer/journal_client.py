# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

from swh.core.utils import grouper
from swh.scheduler.utils import create_task_dict


MAX_ORIGINS_PER_TASK = 100


def process_journal_objects(messages, *, scheduler, task_names):
    """Worker function for `JournalClient.process(worker_fn)`, after
    currification of `scheduler` and `task_names`."""
    assert set(messages) == {'origin_visit'}, set(messages)
    process_origin_visits(messages['origin_visit'], scheduler, task_names)


def process_origin_visits(visits, scheduler, task_names):
    task_dicts = []
    logging.debug('processing origin visits %r', visits)
    if task_names.get('origin_metadata'):
        visits = [visit for visit in visits if visit[b'status'] == b'full']
        visit_batches = grouper(visits, MAX_ORIGINS_PER_TASK)
        for visit_batch in visit_batches:
            task_dicts.append(create_task_dict(
                task_names['origin_metadata'],
                'oneshot',
                [visit[b'origin'][b'url'] for visit in visit_batch],
                policy_update='update-dups',
            ))

    if task_dicts:
        scheduler.create_tasks(task_dicts)
