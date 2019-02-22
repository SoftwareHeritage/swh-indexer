# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import click

from swh.core import config
from swh.scheduler import get_scheduler
from swh.scheduler.utils import create_task_dict
from swh.storage import get_storage

from swh.indexer.metadata_dictionary import MAPPINGS
from swh.indexer.storage import get_indexer_storage
from swh.indexer.storage.api.server import load_and_check_config, app


CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])

TASK_BATCH_SIZE = 1000  # Number of tasks per query to the scheduler


@click.group(context_settings=CONTEXT_SETTINGS)
@click.option('--config-file', '-C', default=None,
              type=click.Path(exists=True, dir_okay=False,),
              help="Configuration file.")
@click.pass_context
def cli(ctx, config_file):
    """Software Heritage Indexer CLI interface
    """
    ctx.ensure_object(dict)

    conf = config.read(config_file)
    ctx.obj['config'] = conf


def _get_api(getter, config, config_key, url):
    if url:
        config[config_key] = {
            'cls': 'remote',
            'args': {'url': url}
        }
    elif config_key not in config:
        raise click.ClickException(
            'Missing configuration for {}'.format(config_key))
    return getter(**config[config_key])


@cli.group('mapping')
def mapping():
    pass


@mapping.command('list')
def mapping_list():
    """Prints the list of known mappings."""
    mapping_names = [mapping.name for mapping in MAPPINGS.values()]
    mapping_names.sort()
    for mapping_name in mapping_names:
        click.echo(mapping_name)


@cli.group('schedule')
@click.option('--scheduler-url', '-s', default=None,
              help="URL of the scheduler API")
@click.option('--indexer-storage-url', '-i', default=None,
              help="URL of the indexer storage API")
@click.option('--storage-url', '-g', default=None,
              help="URL of the (graph) storage API")
@click.option('--dry-run/--no-dry-run', is_flag=True,
              default=False,
              help='Default to list only what would be scheduled.')
@click.pass_context
def schedule(ctx, scheduler_url, storage_url, indexer_storage_url,
             dry_run):
    """Manipulate indexer tasks via SWH Scheduler's API."""
    ctx.obj['indexer_storage'] = _get_api(
        get_indexer_storage,
        ctx.obj['config'],
        'indexer_storage',
        indexer_storage_url
    )
    ctx.obj['storage'] = _get_api(
        get_storage,
        ctx.obj['config'],
        'storage',
        storage_url
    )
    ctx.obj['scheduler'] = _get_api(
        get_scheduler,
        ctx.obj['config'],
        'scheduler',
        scheduler_url
    )
    if dry_run:
        ctx.obj['scheduler'] = None


def list_origins_by_producer(idx_storage, mappings, tool_ids):
    start = 0
    limit = 10000
    while True:
        origins = list(
            idx_storage.origin_intrinsic_metadata_search_by_producer(
                start=start, limit=limit, ids_only=True,
                mappings=mappings or None, tool_ids=tool_ids or None))
        if not origins:
            break
        start = origins[-1]+1
        yield from origins


@schedule.command('reindex_origin_metadata')
@click.option('--batch-size', '-b', 'origin_batch_size',
              default=10, show_default=True, type=int,
              help="Number of origins per task")
@click.option('--tool-id', '-t', 'tool_ids', type=int, multiple=True,
              help="Restrict search of old metadata to this/these tool ids.")
@click.option('--mapping', '-m', 'mappings', multiple=True,
              help="Mapping(s) that should be re-scheduled (eg. 'npm', "
                   "'gemspec', 'maven')")
@click.option('--task-type',
              default='indexer_origin_metadata', show_default=True,
              help="Name of the task type to schedule.")
@click.pass_context
def schedule_origin_metadata_reindex(
        ctx, origin_batch_size, mappings, tool_ids, task_type):
    """Schedules indexing tasks for origins that were already indexed."""
    idx_storage = ctx.obj['indexer_storage']
    scheduler = ctx.obj['scheduler']

    origins = list_origins_by_producer(idx_storage, mappings, tool_ids)
    kwargs = {"policy_update": "update-dups", "parse_ids": False}
    nb_origins = 0
    nb_tasks = 0

    while True:
        task_batch = []
        for _ in range(TASK_BATCH_SIZE):
            # Group origins
            origin_batch = []
            for (_, origin) in zip(range(origin_batch_size), origins):
                origin_batch.append(origin)
            nb_origins += len(origin_batch)
            if not origin_batch:
                break

            # Create a task for these origins
            args = [origin_batch]
            task_dict = create_task_dict(task_type, 'oneshot', *args, **kwargs)
            task_batch.append(task_dict)

        # Schedule a batch of tasks
        if not task_batch:
            break
        nb_tasks += len(task_batch)
        if scheduler:
            scheduler.create_tasks(task_batch)
        click.echo('Scheduled %d tasks (%d origins).' % (nb_tasks, nb_origins))

    # Print final status.
    if nb_tasks:
        click.echo('Done.')
    else:
        click.echo('Nothing to do (no origin metadata matched the criteria).')


@cli.command('api-server')
@click.argument('config-path', required=1)
@click.option('--host', default='0.0.0.0', help="Host to run the server")
@click.option('--port', default=5007, type=click.INT,
              help="Binding port of the server")
@click.option('--debug/--nodebug', default=True,
              help="Indicates if the server should run in debug mode")
def api_server(config_path, host, port, debug):
    api_cfg = load_and_check_config(config_path, type='any')
    app.config.update(api_cfg)
    app.run(host, port=int(port), debug=bool(debug))


def main():
    return cli(auto_envvar_prefix='SWH_INDEXER')


if __name__ == '__main__':
    main()
