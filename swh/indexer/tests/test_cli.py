# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
from functools import reduce
import re
from typing import Any, Dict, List
from unittest.mock import patch

from click.testing import CliRunner
from confluent_kafka import Consumer
import pytest

from swh.indexer.cli import indexer_cli_group
from swh.indexer.storage.interface import IndexerStorageInterface
from swh.indexer.storage.model import (
    DirectoryIntrinsicMetadataRow,
    OriginIntrinsicMetadataRow,
)
from swh.journal.writer import get_journal_writer
from swh.model.hashutil import hash_to_bytes
from swh.model.model import OriginVisitStatus

from .utils import DIRECTORY2, REVISION


def fill_idx_storage(idx_storage: IndexerStorageInterface, nb_rows: int) -> List[int]:
    tools: List[Dict[str, Any]] = [
        {
            "tool_name": "tool %d" % i,
            "tool_version": "0.0.1",
            "tool_configuration": {},
        }
        for i in range(2)
    ]
    tools = idx_storage.indexer_configuration_add(tools)

    origin_metadata = [
        OriginIntrinsicMetadataRow(
            id="file://dev/%04d" % origin_id,
            from_directory=hash_to_bytes("abcd{:0>36}".format(origin_id)),
            indexer_configuration_id=tools[origin_id % 2]["id"],
            metadata={"name": "origin %d" % origin_id},
            mappings=["mapping%d" % (origin_id % 10)],
        )
        for origin_id in range(nb_rows)
    ]
    directory_metadata = [
        DirectoryIntrinsicMetadataRow(
            id=hash_to_bytes("abcd{:0>36}".format(origin_id)),
            indexer_configuration_id=tools[origin_id % 2]["id"],
            metadata={"name": "origin %d" % origin_id},
            mappings=["mapping%d" % (origin_id % 10)],
        )
        for origin_id in range(nb_rows)
    ]

    idx_storage.directory_intrinsic_metadata_add(directory_metadata)
    idx_storage.origin_intrinsic_metadata_add(origin_metadata)

    return [tool["id"] for tool in tools]


def _origins_in_task_args(tasks):
    """Returns the set of origins contained in the arguments of the
    provided tasks (assumed to be of type index-origin-metadata)."""
    return reduce(
        set.union, (set(task["arguments"]["args"][0]) for task in tasks), set()
    )


def _assert_tasks_for_origins(tasks, origins):
    expected_kwargs = {}
    assert {task["type"] for task in tasks} == {"index-origin-metadata"}
    assert all(len(task["arguments"]["args"]) == 1 for task in tasks)
    for task in tasks:
        assert task["arguments"]["kwargs"] == expected_kwargs, task
    assert _origins_in_task_args(tasks) == set(["file://dev/%04d" % i for i in origins])


@pytest.fixture
def cli_runner():
    return CliRunner()


def test_cli_mapping_list(cli_runner, swh_config):
    result = cli_runner.invoke(
        indexer_cli_group,
        ["-C", swh_config, "mapping", "list"],
        catch_exceptions=False,
    )
    expected_output = "\n".join(
        [
            "cff",
            "codemeta",
            "composer",
            "gemspec",
            "github",
            "maven",
            "npm",
            "pkg-info",
            "pubspec",
            "",
        ]  # must be sorted for test to pass
    )
    assert result.exit_code == 0, result.output
    assert result.output == expected_output


def test_cli_mapping_list_terms(cli_runner, swh_config):
    result = cli_runner.invoke(
        indexer_cli_group,
        ["-C", swh_config, "mapping", "list-terms"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output
    assert re.search(r"http://schema.org/url:\n.*npm", result.output)
    assert re.search(r"http://schema.org/url:\n.*codemeta", result.output)
    assert re.search(
        r"https://codemeta.github.io/terms/developmentStatus:\n\tcodemeta",
        result.output,
    )


def test_cli_mapping_list_terms_exclude(cli_runner, swh_config):
    result = cli_runner.invoke(
        indexer_cli_group,
        ["-C", swh_config, "mapping", "list-terms", "--exclude-mapping", "codemeta"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output
    assert re.search(r"http://schema.org/url:\n.*npm", result.output)
    assert not re.search(r"http://schema.org/url:\n.*codemeta", result.output)
    assert not re.search(
        r"https://codemeta.github.io/terms/developmentStatus:\n\tcodemeta",
        result.output,
    )


@patch("swh.scheduler.cli.utils.TASK_BATCH_SIZE", 3)
@patch("swh.scheduler.cli_utils.TASK_BATCH_SIZE", 3)
def test_cli_origin_metadata_reindex_empty_db(
    cli_runner, swh_config, indexer_scheduler, idx_storage, storage
):
    result = cli_runner.invoke(
        indexer_cli_group,
        [
            "-C",
            swh_config,
            "schedule",
            "reindex_origin_metadata",
        ],
        catch_exceptions=False,
    )
    expected_output = "Nothing to do (no origin metadata matched the criteria).\n"
    assert result.exit_code == 0, result.output
    assert result.output == expected_output
    tasks = indexer_scheduler.search_tasks()
    assert len(tasks) == 0


@patch("swh.scheduler.cli.utils.TASK_BATCH_SIZE", 3)
@patch("swh.scheduler.cli_utils.TASK_BATCH_SIZE", 3)
def test_cli_origin_metadata_reindex_divisor(
    cli_runner, swh_config, indexer_scheduler, idx_storage, storage
):
    """Tests the re-indexing when origin_batch_size*task_batch_size is a
    divisor of nb_origins."""
    fill_idx_storage(idx_storage, 90)

    result = cli_runner.invoke(
        indexer_cli_group,
        [
            "-C",
            swh_config,
            "schedule",
            "reindex_origin_metadata",
        ],
        catch_exceptions=False,
    )

    # Check the output
    expected_output = (
        "Scheduled 3 tasks (30 origins).\n"
        "Scheduled 6 tasks (60 origins).\n"
        "Scheduled 9 tasks (90 origins).\n"
        "Done.\n"
    )
    assert result.exit_code == 0, result.output
    assert result.output == expected_output

    # Check scheduled tasks
    tasks = indexer_scheduler.search_tasks()
    assert len(tasks) == 9
    _assert_tasks_for_origins(tasks, range(90))


@patch("swh.scheduler.cli.utils.TASK_BATCH_SIZE", 3)
@patch("swh.scheduler.cli_utils.TASK_BATCH_SIZE", 3)
def test_cli_origin_metadata_reindex_dry_run(
    cli_runner, swh_config, indexer_scheduler, idx_storage, storage
):
    """Tests the re-indexing when origin_batch_size*task_batch_size is a
    divisor of nb_origins."""
    fill_idx_storage(idx_storage, 90)

    result = cli_runner.invoke(
        indexer_cli_group,
        [
            "-C",
            swh_config,
            "schedule",
            "--dry-run",
            "reindex_origin_metadata",
        ],
        catch_exceptions=False,
    )

    # Check the output
    expected_output = (
        "Scheduled 3 tasks (30 origins).\n"
        "Scheduled 6 tasks (60 origins).\n"
        "Scheduled 9 tasks (90 origins).\n"
        "Done.\n"
    )
    assert result.exit_code == 0, result.output
    assert result.output == expected_output

    # Check scheduled tasks
    tasks = indexer_scheduler.search_tasks()
    assert len(tasks) == 0


@patch("swh.scheduler.cli.utils.TASK_BATCH_SIZE", 3)
@patch("swh.scheduler.cli_utils.TASK_BATCH_SIZE", 3)
def test_cli_origin_metadata_reindex_nondivisor(
    cli_runner, swh_config, indexer_scheduler, idx_storage, storage
):
    """Tests the re-indexing when neither origin_batch_size or
    task_batch_size is a divisor of nb_origins."""
    fill_idx_storage(idx_storage, 70)

    result = cli_runner.invoke(
        indexer_cli_group,
        [
            "-C",
            swh_config,
            "schedule",
            "reindex_origin_metadata",
            "--batch-size",
            "20",
        ],
        catch_exceptions=False,
    )

    # Check the output
    expected_output = (
        "Scheduled 3 tasks (60 origins).\n"
        "Scheduled 4 tasks (70 origins).\n"
        "Done.\n"
    )
    assert result.exit_code == 0, result.output
    assert result.output == expected_output

    # Check scheduled tasks
    tasks = indexer_scheduler.search_tasks()
    assert len(tasks) == 4
    _assert_tasks_for_origins(tasks, range(70))


@patch("swh.scheduler.cli.utils.TASK_BATCH_SIZE", 3)
@patch("swh.scheduler.cli_utils.TASK_BATCH_SIZE", 3)
def test_cli_origin_metadata_reindex_filter_one_mapping(
    cli_runner, swh_config, indexer_scheduler, idx_storage, storage
):
    """Tests the re-indexing when origin_batch_size*task_batch_size is a
    divisor of nb_origins."""
    fill_idx_storage(idx_storage, 110)

    result = cli_runner.invoke(
        indexer_cli_group,
        [
            "-C",
            swh_config,
            "schedule",
            "reindex_origin_metadata",
            "--mapping",
            "mapping1",
        ],
        catch_exceptions=False,
    )

    # Check the output
    expected_output = "Scheduled 2 tasks (11 origins).\nDone.\n"
    assert result.exit_code == 0, result.output
    assert result.output == expected_output

    # Check scheduled tasks
    tasks = indexer_scheduler.search_tasks()
    assert len(tasks) == 2
    _assert_tasks_for_origins(tasks, [1, 11, 21, 31, 41, 51, 61, 71, 81, 91, 101])


@patch("swh.scheduler.cli.utils.TASK_BATCH_SIZE", 3)
@patch("swh.scheduler.cli_utils.TASK_BATCH_SIZE", 3)
def test_cli_origin_metadata_reindex_filter_two_mappings(
    cli_runner, swh_config, indexer_scheduler, idx_storage, storage
):
    """Tests the re-indexing when origin_batch_size*task_batch_size is a
    divisor of nb_origins."""
    fill_idx_storage(idx_storage, 110)

    result = cli_runner.invoke(
        indexer_cli_group,
        [
            "--config-file",
            swh_config,
            "schedule",
            "reindex_origin_metadata",
            "--mapping",
            "mapping1",
            "--mapping",
            "mapping2",
        ],
        catch_exceptions=False,
    )

    # Check the output
    expected_output = "Scheduled 3 tasks (22 origins).\nDone.\n"
    assert result.exit_code == 0, result.output
    assert result.output == expected_output

    # Check scheduled tasks
    tasks = indexer_scheduler.search_tasks()
    assert len(tasks) == 3
    _assert_tasks_for_origins(
        tasks,
        [
            1,
            11,
            21,
            31,
            41,
            51,
            61,
            71,
            81,
            91,
            101,
            2,
            12,
            22,
            32,
            42,
            52,
            62,
            72,
            82,
            92,
            102,
        ],
    )


@patch("swh.scheduler.cli.utils.TASK_BATCH_SIZE", 3)
@patch("swh.scheduler.cli_utils.TASK_BATCH_SIZE", 3)
def test_cli_origin_metadata_reindex_filter_one_tool(
    cli_runner, swh_config, indexer_scheduler, idx_storage, storage
):
    """Tests the re-indexing when origin_batch_size*task_batch_size is a
    divisor of nb_origins."""
    tool_ids = fill_idx_storage(idx_storage, 110)

    result = cli_runner.invoke(
        indexer_cli_group,
        [
            "-C",
            swh_config,
            "schedule",
            "reindex_origin_metadata",
            "--tool-id",
            str(tool_ids[0]),
        ],
        catch_exceptions=False,
    )

    # Check the output
    expected_output = (
        "Scheduled 3 tasks (30 origins).\n"
        "Scheduled 6 tasks (55 origins).\n"
        "Done.\n"
    )
    assert result.exit_code == 0, result.output
    assert result.output == expected_output

    # Check scheduled tasks
    tasks = indexer_scheduler.search_tasks()
    assert len(tasks) == 6
    _assert_tasks_for_origins(tasks, [x * 2 for x in range(55)])


def now():
    return datetime.datetime.now(tz=datetime.timezone.utc)


def test_cli_journal_client_schedule(
    cli_runner,
    swh_config,
    indexer_scheduler,
    kafka_prefix: str,
    kafka_server,
    consumer: Consumer,
):
    """Test the 'swh indexer journal-client' cli tool."""
    journal_writer = get_journal_writer(
        "kafka",
        brokers=[kafka_server],
        prefix=kafka_prefix,
        client_id="test producer",
        value_sanitizer=lambda object_type, value: value,
        flush_timeout=3,  # fail early if something is going wrong
    )

    visit_statuses = [
        OriginVisitStatus(
            origin="file:///dev/zero",
            visit=1,
            date=now(),
            status="full",
            snapshot=None,
        ),
        OriginVisitStatus(
            origin="file:///dev/foobar",
            visit=2,
            date=now(),
            status="full",
            snapshot=None,
        ),
        OriginVisitStatus(
            origin="file:///tmp/spamegg",
            visit=3,
            date=now(),
            status="full",
            snapshot=None,
        ),
        OriginVisitStatus(
            origin="file:///dev/0002",
            visit=6,
            date=now(),
            status="full",
            snapshot=None,
        ),
        OriginVisitStatus(  # will be filtered out due to its 'partial' status
            origin="file:///dev/0000",
            visit=4,
            date=now(),
            status="partial",
            snapshot=None,
        ),
        OriginVisitStatus(  # will be filtered out due to its 'ongoing' status
            origin="file:///dev/0001",
            visit=5,
            date=now(),
            status="ongoing",
            snapshot=None,
        ),
    ]

    journal_writer.write_additions("origin_visit_status", visit_statuses)
    visit_statuses_full = [vs for vs in visit_statuses if vs.status == "full"]

    result = cli_runner.invoke(
        indexer_cli_group,
        [
            "-C",
            swh_config,
            "journal-client",
            "--broker",
            kafka_server,
            "--prefix",
            kafka_prefix,
            "--group-id",
            "test-consumer",
            "--stop-after-objects",
            len(visit_statuses),
            "--origin-metadata-task-type",
            "index-origin-metadata",
        ],
        catch_exceptions=False,
    )

    # Check the output
    expected_output = "Done.\n"
    assert result.exit_code == 0, result.output
    assert result.output == expected_output

    # Check scheduled tasks
    tasks = indexer_scheduler.search_tasks(task_type="index-origin-metadata")

    # This can be split into multiple tasks but no more than the origin-visit-statuses
    # written in the journal
    assert len(tasks) <= len(visit_statuses_full)

    actual_origins = []
    for task in tasks:
        actual_task = dict(task)
        assert actual_task["type"] == "index-origin-metadata"
        scheduled_origins = actual_task["arguments"]["args"][0]
        actual_origins.extend(scheduled_origins)

    assert set(actual_origins) == {vs.origin for vs in visit_statuses_full}


def test_cli_journal_client_without_brokers(
    cli_runner, swh_config, kafka_prefix: str, kafka_server, consumer: Consumer
):
    """Without brokers configuration, the cli fails."""

    with pytest.raises(ValueError, match="brokers"):
        cli_runner.invoke(
            indexer_cli_group,
            [
                "-C",
                swh_config,
                "journal-client",
            ],
            catch_exceptions=False,
        )


@pytest.mark.parametrize("indexer_name", ["origin-intrinsic-metadata", "*"])
def test_cli_journal_client_index(
    cli_runner,
    swh_config,
    kafka_prefix: str,
    kafka_server,
    consumer: Consumer,
    idx_storage,
    storage,
    mocker,
    swh_indexer_config,
    indexer_name: str,
):
    """Test the 'swh indexer journal-client' cli tool."""
    journal_writer = get_journal_writer(
        "kafka",
        brokers=[kafka_server],
        prefix=kafka_prefix,
        client_id="test producer",
        value_sanitizer=lambda object_type, value: value,
        flush_timeout=3,  # fail early if something is going wrong
    )

    visit_statuses = [
        OriginVisitStatus(
            origin="file:///dev/zero",
            visit=1,
            date=now(),
            status="full",
            snapshot=None,
        ),
        OriginVisitStatus(
            origin="file:///dev/foobar",
            visit=2,
            date=now(),
            status="full",
            snapshot=None,
        ),
        OriginVisitStatus(
            origin="file:///tmp/spamegg",
            visit=3,
            date=now(),
            status="full",
            snapshot=None,
        ),
        OriginVisitStatus(
            origin="file:///dev/0002",
            visit=6,
            date=now(),
            status="full",
            snapshot=None,
        ),
        OriginVisitStatus(  # will be filtered out due to its 'partial' status
            origin="file:///dev/0000",
            visit=4,
            date=now(),
            status="partial",
            snapshot=None,
        ),
        OriginVisitStatus(  # will be filtered out due to its 'ongoing' status
            origin="file:///dev/0001",
            visit=5,
            date=now(),
            status="ongoing",
            snapshot=None,
        ),
    ]

    journal_writer.write_additions("origin_visit_status", visit_statuses)
    visit_statuses_full = [vs for vs in visit_statuses if vs.status == "full"]
    storage.revision_add([REVISION])

    mocker.patch(
        "swh.indexer.metadata.get_head_swhid",
        return_value=REVISION.swhid(),
    )

    mocker.patch(
        "swh.indexer.metadata.DirectoryMetadataIndexer.index",
        return_value=[
            DirectoryIntrinsicMetadataRow(
                id=DIRECTORY2.id,
                indexer_configuration_id=1,
                mappings=["cff"],
                metadata={"foo": "bar"},
            )
        ],
    )
    result = cli_runner.invoke(
        indexer_cli_group,
        [
            "-C",
            swh_config,
            "journal-client",
            indexer_name,
            "--broker",
            kafka_server,
            "--prefix",
            kafka_prefix,
            "--group-id",
            "test-consumer",
            "--stop-after-objects",
            len(visit_statuses),
        ],
        catch_exceptions=False,
    )

    # Check the output
    expected_output = "Done.\n"
    assert result.exit_code == 0, result.output
    assert result.output == expected_output

    results = idx_storage.origin_intrinsic_metadata_get(
        [status.origin for status in visit_statuses]
    )
    expected_results = [
        OriginIntrinsicMetadataRow(
            id=status.origin,
            from_directory=DIRECTORY2.id,
            tool={"id": 1, **swh_indexer_config["tools"]},
            mappings=["cff"],
            metadata={"foo": "bar"},
        )
        for status in sorted(visit_statuses_full, key=lambda r: r.origin)
    ]
    assert sorted(results, key=lambda r: r.id) == expected_results
