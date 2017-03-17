# Copyright (C) 2016-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import click
import random
import sys

from swh.core import utils
from swh.model import hashutil
from swh.scheduler.utils import get_task


def read_from_stdin():
    for sha1 in sys.stdin:
        yield hashutil.hash_to_bytes(sha1.strip())


def gen_sha1(batch):
    """Generate batch of grouped sha1s from the objstorage.

    """
    for sha1s in utils.grouper(read_from_stdin(), batch):
        sha1s = list(sha1s)
        random.shuffle(sha1s)
        yield sha1s


def run_with_limit(task, limit, batch):
    count = 0
    for sha1s in gen_sha1(batch):
        count += len(sha1s)
        print('%s sent - [%s, ...]' % (len(sha1s), sha1s[0]))
        task.delay(sha1s)
        if count >= limit:
            return


def run_no_limit(task, batch):
    for sha1s in gen_sha1(batch):
        print('%s sent - [%s, ...]' % (len(sha1s), sha1s[0]))
        task.delay(sha1s)


@click.command(help='Read sha1 from stdin and send them for indexing')
@click.option('--limit', default=None, help='Limit the number of data to read')
@click.option('--batch', default='10', help='Group data by batch')
@click.option('--task-name', default='orchestrator_all', help='')
def main(limit, batch, task_name):
    batch = int(batch)

    from . import tasks, TASK_NAMES  # noqa
    possible_tasks = TASK_NAMES.keys()

    if task_name not in possible_tasks:
        print('The task_name can only be one of %s' %
              ', '.join(possible_tasks))
        return

    task = get_task(TASK_NAMES[task_name])

    if limit:
        run_with_limit(task, int(limit), batch)
    else:
        run_no_limit(task, batch)


if __name__ == '__main__':
    main()
