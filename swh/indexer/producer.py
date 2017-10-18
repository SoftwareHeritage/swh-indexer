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


def gen_sha1(batch, dict_with_key=None):
    """Generate batch of grouped sha1s from the objstorage.

    """
    def _gen():
        for ids in utils.grouper(read_from_stdin(), batch):
            ids = list(ids)
            random.shuffle(ids)
            yield ids

    if dict_with_key:
        for ids in _gen():
            yield [{dict_with_key: sha1} for sha1 in ids]
    else:
        yield from _gen()


def make_function_execute(task, sync):
    """Compute a function which executes computations on sha1s
       synchronously or asynchronously.

    """
    if sync:
        return (lambda ids, task=task: task(ids))
    return (lambda ids, task=task: task.delay(ids))


def run_with_limit(task, limit, batch, dict_with_key=None, sync=False):
    execute_fn = make_function_execute(task, sync)

    count = 0
    for ids in gen_sha1(batch, dict_with_key):
        count += len(ids)
        execute_fn(ids)
        print('%s sent - [%s, ...]' % (len(ids), ids[0]))
        if count >= limit:
            return


def run(task, batch, dict_with_key=None, sync=False):
    execute_fn = make_function_execute(task, sync)

    for ids in gen_sha1(batch, dict_with_key):
        execute_fn(ids)
        print('%s sent - [%s, ...]' % (len(ids), ids[0]))


@click.command(help='Read sha1 from stdin and send them for indexing')
@click.option('--limit', default=None, help='Limit the number of data to read')
@click.option('--batch', default='10', help='Group data by batch')
@click.option('--task-name', default='orchestrator_all', help='Task\'s name')
@click.option('--sync/--nosync', default=False,
              help='Make the producer actually execute the routine.')
@click.option('--dict-with-key', default=None)
def main(limit, batch, task_name, sync, dict_with_key):
    """Read sha1 from stdin and send them for indexing.

    By default, send directly list of hashes.  Using the
    --dict-with-key, this will send dict list with one key mentioned
    as parameter to the dict-with-key flag.

    """
    batch = int(batch)

    from . import tasks, TASK_NAMES  # noqa
    possible_tasks = TASK_NAMES.keys()

    if task_name not in possible_tasks:
        print('The task_name can only be one of %s' %
              ', '.join(possible_tasks))
        return

    task = get_task(TASK_NAMES[task_name])

    if limit:
        run_with_limit(task, int(limit), batch,
                       dict_with_key=dict_with_key, sync=sync)
    else:
        run(task, batch,
            dict_with_key=dict_with_key, sync=sync)


if __name__ == '__main__':
    main()
