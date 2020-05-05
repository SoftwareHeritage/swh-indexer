# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import functools
import json

import click

from swh.core import config
from swh.core.cli import CONTEXT_SETTINGS, AliasedGroup
from swh.journal.client import get_journal_client
from swh.scheduler import get_scheduler
from swh.scheduler.cli_utils import schedule_origin_batches
from swh.storage import get_storage

from swh.indexer import metadata_dictionary
from swh.indexer.journal_client import process_journal_objects
from swh.indexer.storage import get_indexer_storage
from swh.indexer.storage.api.server import load_and_check_config, app


@click.group(name="indexer", context_settings=CONTEXT_SETTINGS, cls=AliasedGroup)
@click.option(
    "--config-file",
    "-C",
    default=None,
    type=click.Path(exists=True, dir_okay=False,),
    help="Configuration file.",
)
@click.pass_context
def cli(ctx, config_file):
    """Software Heritage Indexer tools.

    The Indexer is used to mine the content of the archive and extract derived
    information from archive source code artifacts.

    """
    ctx.ensure_object(dict)

    conf = config.read(config_file)
    ctx.obj["config"] = conf


def _get_api(getter, config, config_key, url):
    if url:
        config[config_key] = {"cls": "remote", "args": {"url": url}}
    elif config_key not in config:
        raise click.ClickException("Missing configuration for {}".format(config_key))
    return getter(**config[config_key])


@cli.group("mapping")
def mapping():
    """Manage Software Heritage Indexer mappings."""
    pass


@mapping.command("list")
def mapping_list():
    """Prints the list of known mappings."""
    mapping_names = [mapping.name for mapping in metadata_dictionary.MAPPINGS.values()]
    mapping_names.sort()
    for mapping_name in mapping_names:
        click.echo(mapping_name)


@mapping.command("list-terms")
@click.option(
    "--exclude-mapping", multiple=True, help="Exclude the given mapping from the output"
)
@click.option(
    "--concise",
    is_flag=True,
    default=False,
    help="Don't print the list of mappings supporting each term.",
)
def mapping_list_terms(concise, exclude_mapping):
    """Prints the list of known CodeMeta terms, and which mappings
    support them."""
    properties = metadata_dictionary.list_terms()
    for (property_name, supported_mappings) in sorted(properties.items()):
        supported_mappings = {m.name for m in supported_mappings}
        supported_mappings -= set(exclude_mapping)
        if supported_mappings:
            if concise:
                click.echo(property_name)
            else:
                click.echo("{}:".format(property_name))
                click.echo("\t" + ", ".join(sorted(supported_mappings)))


@mapping.command("translate")
@click.argument("mapping-name")
@click.argument("file", type=click.File("rb"))
def mapping_translate(mapping_name, file):
    """Prints the list of known mappings."""
    mapping_cls = [
        cls for cls in metadata_dictionary.MAPPINGS.values() if cls.name == mapping_name
    ]
    if not mapping_cls:
        raise click.ClickException("Unknown mapping {}".format(mapping_name))
    assert len(mapping_cls) == 1
    mapping_cls = mapping_cls[0]
    mapping = mapping_cls()
    codemeta_doc = mapping.translate(file.read())
    click.echo(json.dumps(codemeta_doc, indent=4))


@cli.group("schedule")
@click.option("--scheduler-url", "-s", default=None, help="URL of the scheduler API")
@click.option(
    "--indexer-storage-url", "-i", default=None, help="URL of the indexer storage API"
)
@click.option(
    "--storage-url", "-g", default=None, help="URL of the (graph) storage API"
)
@click.option(
    "--dry-run/--no-dry-run",
    is_flag=True,
    default=False,
    help="List only what would be scheduled.",
)
@click.pass_context
def schedule(ctx, scheduler_url, storage_url, indexer_storage_url, dry_run):
    """Manipulate Software Heritage Indexer tasks.

    Via SWH Scheduler's API."""
    ctx.obj["indexer_storage"] = _get_api(
        get_indexer_storage, ctx.obj["config"], "indexer_storage", indexer_storage_url
    )
    ctx.obj["storage"] = _get_api(
        get_storage, ctx.obj["config"], "storage", storage_url
    )
    ctx.obj["scheduler"] = _get_api(
        get_scheduler, ctx.obj["config"], "scheduler", scheduler_url
    )
    if dry_run:
        ctx.obj["scheduler"] = None


def list_origins_by_producer(idx_storage, mappings, tool_ids):
    next_page_token = ""
    limit = 10000
    while next_page_token is not None:
        result = idx_storage.origin_intrinsic_metadata_search_by_producer(
            page_token=next_page_token,
            limit=limit,
            ids_only=True,
            mappings=mappings or None,
            tool_ids=tool_ids or None,
        )
        next_page_token = result.get("next_page_token")
        yield from result["origins"]


@schedule.command("reindex_origin_metadata")
@click.option(
    "--batch-size",
    "-b",
    "origin_batch_size",
    default=10,
    show_default=True,
    type=int,
    help="Number of origins per task",
)
@click.option(
    "--tool-id",
    "-t",
    "tool_ids",
    type=int,
    multiple=True,
    help="Restrict search of old metadata to this/these tool ids.",
)
@click.option(
    "--mapping",
    "-m",
    "mappings",
    multiple=True,
    help="Mapping(s) that should be re-scheduled (eg. 'npm', 'gemspec', 'maven')",
)
@click.option(
    "--task-type",
    default="index-origin-metadata",
    show_default=True,
    help="Name of the task type to schedule.",
)
@click.pass_context
def schedule_origin_metadata_reindex(
    ctx, origin_batch_size, tool_ids, mappings, task_type
):
    """Schedules indexing tasks for origins that were already indexed."""
    idx_storage = ctx.obj["indexer_storage"]
    scheduler = ctx.obj["scheduler"]

    origins = list_origins_by_producer(idx_storage, mappings, tool_ids)

    kwargs = {"policy_update": "update-dups"}
    schedule_origin_batches(scheduler, task_type, origins, origin_batch_size, kwargs)


@cli.command("journal-client")
@click.option("--scheduler-url", "-s", default=None, help="URL of the scheduler API")
@click.option(
    "--origin-metadata-task-type",
    default="index-origin-metadata",
    help="Name of the task running the origin metadata indexer.",
)
@click.option(
    "--broker", "brokers", type=str, multiple=True, help="Kafka broker to connect to."
)
@click.option(
    "--prefix", type=str, default=None, help="Prefix of Kafka topic names to read from."
)
@click.option("--group-id", type=str, help="Consumer/group id for reading from Kafka.")
@click.option(
    "--stop-after-objects",
    "-m",
    default=None,
    type=int,
    help="Maximum number of objects to replay. Default is to run forever.",
)
@click.pass_context
def journal_client(
    ctx,
    scheduler_url,
    origin_metadata_task_type,
    brokers,
    prefix,
    group_id,
    stop_after_objects,
):
    """Listens for new objects from the SWH Journal, and schedules tasks
    to run relevant indexers (currently, only origin-intrinsic-metadata)
    on these new objects."""
    scheduler = _get_api(get_scheduler, ctx.obj["config"], "scheduler", scheduler_url)

    client = get_journal_client(
        cls="kafka",
        brokers=brokers,
        prefix=prefix,
        group_id=group_id,
        object_types=["origin_visit"],
        stop_after_objects=stop_after_objects,
    )

    worker_fn = functools.partial(
        process_journal_objects,
        scheduler=scheduler,
        task_names={"origin_metadata": origin_metadata_task_type,},
    )
    try:
        client.process(worker_fn)
    except KeyboardInterrupt:
        ctx.exit(0)
    else:
        print("Done.")
    finally:
        client.close()


@cli.command("rpc-serve")
@click.argument("config-path", required=True)
@click.option("--host", default="0.0.0.0", help="Host to run the server")
@click.option("--port", default=5007, type=click.INT, help="Binding port of the server")
@click.option(
    "--debug/--nodebug",
    default=True,
    help="Indicates if the server should run in debug mode",
)
def rpc_server(config_path, host, port, debug):
    """Starts a Software Heritage Indexer RPC HTTP server."""
    api_cfg = load_and_check_config(config_path, type="any")
    app.config.update(api_cfg)
    app.run(host, port=int(port), debug=bool(debug))


def main():
    return cli(auto_envvar_prefix="SWH_INDEXER")


if __name__ == "__main__":
    main()
