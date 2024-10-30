# Copyright (C) 2019-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import os
from typing import Callable, Dict, Iterator, List, Optional
import warnings

# WARNING: do not import unnecessary things here to keep cli startup time under
# control
import click

from swh.core.cli import CONTEXT_SETTINGS, AliasedGroup
from swh.core.cli import swh as swh_cli_group

logger = logging.getLogger(__name__)


@swh_cli_group.group(
    name="indexer", context_settings=CONTEXT_SETTINGS, cls=AliasedGroup
)
@click.option(
    "--config-file",
    "-C",
    default=None,
    type=click.Path(
        exists=True,
        dir_okay=False,
    ),
    help="Configuration file.",
)
@click.pass_context
def indexer_cli_group(ctx, config_file):
    """Software Heritage Indexer tools.

    The Indexer is used to mine the content of the archive and extract derived
    information from archive source code artifacts.

    """
    from swh.core import config

    ctx.ensure_object(dict)
    if not config_file:
        config_file = os.environ.get("SWH_CONFIG_FILENAME")

    if config_file:
        if not os.path.exists(config_file):
            raise ValueError("%s does not exist" % config_file)
        cfg = config.read(config_file)
    else:
        cfg = {}

    if "indexer.storage" in cfg:
        warnings.warn(
            "The 'indexer.storage' configuration section should be renamed "
            "as 'indexer_storage'",
            DeprecationWarning,
        )
        cfg["indexer_storage"] = cfg.pop("indexer.storage")

    ctx.obj["config"] = cfg


def _get_api(getter, config, config_key, url):
    if url:
        config[config_key] = {"cls": "remote", "url": url}
    elif config_key not in config:
        raise click.ClickException("Missing configuration for {}".format(config_key))
    return getter(**config[config_key])


@indexer_cli_group.group("mapping")
def mapping():
    """Manage Software Heritage Indexer mappings."""
    pass


@mapping.command("list")
def mapping_list():
    """Prints the list of known mappings."""
    from swh.indexer import metadata_dictionary

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
    from swh.indexer import metadata_dictionary

    properties = metadata_dictionary.list_terms()
    for property_name, supported_mappings in sorted(properties.items()):
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
    """Translates file from mapping-name to codemeta format."""
    import json

    from swh.indexer import metadata_dictionary

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


def list_origins_by_producer(idx_storage, mappings, tool_ids) -> Iterator[str]:
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
        next_page_token = result.next_page_token
        yield from result.results


@indexer_cli_group.command("journal-client")
@click.argument(
    "indexer",
    type=click.Choice(
        [
            "origin_intrinsic_metadata",
            "extrinsic_metadata",
            "content_mimetype",
            "content_fossology_license",
            "*",
        ]
    ),
    required=True,
)
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
@click.option(
    "--batch-size",
    "-b",
    default=None,
    type=int,
    help="Batch size. Default is 200.",
)
@click.pass_context
def journal_client(
    ctx,
    indexer: Optional[str],
    origin_metadata_task_type: str,
    brokers: List[str],
    prefix: str,
    group_id: str,
    stop_after_objects: Optional[int],
    batch_size: Optional[int],
):
    """
    Listens for new objects from the SWH Journal, and either:

    * runs the indexer with the name passed as argument, if any
    * schedules tasks to run relevant indexers (currently, only
      origin_intrinsic_metadata) on these new objects otherwise.

    Passing '*' as indexer name runs all indexers.
    """
    from swh.indexer.indexer import BaseIndexer, ObjectsDict
    from swh.journal.client import get_journal_client

    cfg = ctx.obj["config"]
    journal_cfg = cfg.get("journal_client", cfg.get("journal", {}))

    if brokers:
        journal_cfg["brokers"] = brokers
    if not journal_cfg.get("brokers"):
        raise ValueError("The brokers configuration is mandatory.")

    if prefix:
        journal_cfg["prefix"] = prefix
    if group_id:
        journal_cfg["group_id"] = group_id
    origin_metadata_task_type = origin_metadata_task_type or journal_cfg.get(
        "origin_metadata_task_type"
    )
    if stop_after_objects:
        journal_cfg["stop_after_objects"] = stop_after_objects
    if batch_size:
        journal_cfg["batch_size"] = batch_size

    object_types = set()
    worker_fns: List[Callable[[ObjectsDict], Dict]] = []

    idx: Optional[BaseIndexer] = None

    if indexer in ("origin_intrinsic_metadata", "*"):
        from swh.indexer.metadata import OriginMetadataIndexer

        object_types.add("origin_visit_status")
        idx = OriginMetadataIndexer()
        idx.catch_exceptions = False  # don't commit offsets if indexation failed
        worker_fns.append(idx.process_journal_objects)

    if indexer in ("extrinsic_metadata", "*"):
        from swh.indexer.metadata import ExtrinsicMetadataIndexer

        object_types.add("raw_extrinsic_metadata")
        idx = ExtrinsicMetadataIndexer()
        idx.catch_exceptions = False  # don't commit offsets if indexation failed
        worker_fns.append(idx.process_journal_objects)

    if indexer in ("content_mimetype", "*"):
        from swh.indexer.mimetype import MimetypeIndexer

        object_types.add("content")
        idx = MimetypeIndexer()
        idx.catch_exceptions = False  # don't commit offsets if indexation failed
        worker_fns.append(idx.process_journal_objects)

    if indexer in ("content_fossology_license", "*"):
        from swh.indexer.fossology_license import FossologyLicenseIndexer

        object_types.add("content")
        idx = FossologyLicenseIndexer()
        idx.catch_exceptions = False  # don't commit offsets if indexation failed
        worker_fns.append(idx.process_journal_objects)

    if not worker_fns:
        raise click.ClickException(f"Unknown indexer: {indexer}")

    if "cls" not in journal_cfg:
        journal_cfg["cls"] = "kafka"

    if (
        journal_cfg.get("object_types")
        and set(journal_cfg["object_types"]) != object_types
    ):
        logger.warning(
            "Overriding configured journal client object types (%s) with %s",
            ", ".join(sorted(journal_cfg["object_types"])),
            ", ".join(sorted(object_types)),
        )

    journal_cfg["object_types"] = list(object_types)

    client = get_journal_client(**journal_cfg)

    def worker_fn(objects: ObjectsDict):
        for fn in worker_fns:
            fn(objects)

    try:
        client.process(worker_fn)
    except KeyboardInterrupt:
        ctx.exit(0)
    else:
        print("Done.")
    finally:
        client.close()


@indexer_cli_group.command("rpc-serve")
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
    from swh.indexer.storage.api.server import app, load_and_check_config

    api_cfg = load_and_check_config(config_path, type="any")
    app.config.update(api_cfg)
    app.run(host, port=int(port), debug=bool(debug))


def main():
    return indexer_cli_group(auto_envvar_prefix="SWH_INDEXER")


if __name__ == "__main__":
    main()
