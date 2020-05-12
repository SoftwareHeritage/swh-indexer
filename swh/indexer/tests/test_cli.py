# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from functools import reduce
import re
import tempfile
from unittest.mock import patch

from click.testing import CliRunner
from confluent_kafka import Consumer, Producer

from swh.journal.serializers import value_to_kafka
from swh.model.hashutil import hash_to_bytes

from swh.indexer.cli import cli


CLI_CONFIG = """
scheduler:
    cls: foo
    args: {}
storage:
    cls: memory
indexer_storage:
    cls: memory
    args: {}
"""


def fill_idx_storage(idx_storage, nb_rows):
    tools = [
        {"tool_name": "tool %d" % i, "tool_version": "0.0.1", "tool_configuration": {},}
        for i in range(2)
    ]
    tools = idx_storage.indexer_configuration_add(tools)

    origin_metadata = [
        {
            "id": "file://dev/%04d" % origin_id,
            "from_revision": hash_to_bytes("abcd{:0>4}".format(origin_id)),
            "indexer_configuration_id": tools[origin_id % 2]["id"],
            "metadata": {"name": "origin %d" % origin_id},
            "mappings": ["mapping%d" % (origin_id % 10)],
        }
        for origin_id in range(nb_rows)
    ]
    revision_metadata = [
        {
            "id": hash_to_bytes("abcd{:0>4}".format(origin_id)),
            "indexer_configuration_id": tools[origin_id % 2]["id"],
            "metadata": {"name": "origin %d" % origin_id},
            "mappings": ["mapping%d" % (origin_id % 10)],
        }
        for origin_id in range(nb_rows)
    ]

    idx_storage.revision_intrinsic_metadata_add(revision_metadata)
    idx_storage.origin_intrinsic_metadata_add(origin_metadata)

    return [tool["id"] for tool in tools]


def _origins_in_task_args(tasks):
    """Returns the set of origins contained in the arguments of the
    provided tasks (assumed to be of type index-origin-metadata)."""
    return reduce(
        set.union, (set(task["arguments"]["args"][0]) for task in tasks), set()
    )


def _assert_tasks_for_origins(tasks, origins):
    expected_kwargs = {"policy_update": "update-dups"}
    assert {task["type"] for task in tasks} == {"index-origin-metadata"}
    assert all(len(task["arguments"]["args"]) == 1 for task in tasks)
    for task in tasks:
        assert task["arguments"]["kwargs"] == expected_kwargs, task
    assert _origins_in_task_args(tasks) == set(["file://dev/%04d" % i for i in origins])


def invoke(scheduler, catch_exceptions, args):
    runner = CliRunner()
    with patch(
        "swh.indexer.cli.get_scheduler"
    ) as get_scheduler_mock, tempfile.NamedTemporaryFile(
        "a", suffix=".yml"
    ) as config_fd:
        config_fd.write(CLI_CONFIG)
        config_fd.seek(0)
        get_scheduler_mock.return_value = scheduler
        result = runner.invoke(cli, ["-C" + config_fd.name] + args)
    if not catch_exceptions and result.exception:
        print(result.output)
        raise result.exception
    return result


def test_mapping_list(indexer_scheduler):
    result = invoke(indexer_scheduler, False, ["mapping", "list",])
    expected_output = "\n".join(
        ["codemeta", "gemspec", "maven", "npm", "pkg-info", "",]
    )
    assert result.exit_code == 0, result.output
    assert result.output == expected_output


def test_mapping_list_terms(indexer_scheduler):
    result = invoke(indexer_scheduler, False, ["mapping", "list-terms",])
    assert result.exit_code == 0, result.output
    assert re.search(r"http://schema.org/url:\n.*npm", result.output)
    assert re.search(r"http://schema.org/url:\n.*codemeta", result.output)
    assert re.search(
        r"https://codemeta.github.io/terms/developmentStatus:\n\tcodemeta",
        result.output,
    )


def test_mapping_list_terms_exclude(indexer_scheduler):
    result = invoke(
        indexer_scheduler,
        False,
        ["mapping", "list-terms", "--exclude-mapping", "codemeta"],
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
def test_origin_metadata_reindex_empty_db(indexer_scheduler, idx_storage, storage):
    result = invoke(indexer_scheduler, False, ["schedule", "reindex_origin_metadata",])
    expected_output = "Nothing to do (no origin metadata matched the criteria).\n"
    assert result.exit_code == 0, result.output
    assert result.output == expected_output
    tasks = indexer_scheduler.search_tasks()
    assert len(tasks) == 0


@patch("swh.scheduler.cli.utils.TASK_BATCH_SIZE", 3)
@patch("swh.scheduler.cli_utils.TASK_BATCH_SIZE", 3)
def test_origin_metadata_reindex_divisor(indexer_scheduler, idx_storage, storage):
    """Tests the re-indexing when origin_batch_size*task_batch_size is a
    divisor of nb_origins."""
    fill_idx_storage(idx_storage, 90)

    result = invoke(indexer_scheduler, False, ["schedule", "reindex_origin_metadata",])

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
def test_origin_metadata_reindex_dry_run(indexer_scheduler, idx_storage, storage):
    """Tests the re-indexing when origin_batch_size*task_batch_size is a
    divisor of nb_origins."""
    fill_idx_storage(idx_storage, 90)

    result = invoke(
        indexer_scheduler, False, ["schedule", "--dry-run", "reindex_origin_metadata",]
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
def test_origin_metadata_reindex_nondivisor(indexer_scheduler, idx_storage, storage):
    """Tests the re-indexing when neither origin_batch_size or
    task_batch_size is a divisor of nb_origins."""
    fill_idx_storage(idx_storage, 70)

    result = invoke(
        indexer_scheduler,
        False,
        ["schedule", "reindex_origin_metadata", "--batch-size", "20",],
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
def test_origin_metadata_reindex_filter_one_mapping(
    indexer_scheduler, idx_storage, storage
):
    """Tests the re-indexing when origin_batch_size*task_batch_size is a
    divisor of nb_origins."""
    fill_idx_storage(idx_storage, 110)

    result = invoke(
        indexer_scheduler,
        False,
        ["schedule", "reindex_origin_metadata", "--mapping", "mapping1",],
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
def test_origin_metadata_reindex_filter_two_mappings(
    indexer_scheduler, idx_storage, storage
):
    """Tests the re-indexing when origin_batch_size*task_batch_size is a
    divisor of nb_origins."""
    fill_idx_storage(idx_storage, 110)

    result = invoke(
        indexer_scheduler,
        False,
        [
            "schedule",
            "reindex_origin_metadata",
            "--mapping",
            "mapping1",
            "--mapping",
            "mapping2",
        ],
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
def test_origin_metadata_reindex_filter_one_tool(
    indexer_scheduler, idx_storage, storage
):
    """Tests the re-indexing when origin_batch_size*task_batch_size is a
    divisor of nb_origins."""
    tool_ids = fill_idx_storage(idx_storage, 110)

    result = invoke(
        indexer_scheduler,
        False,
        ["schedule", "reindex_origin_metadata", "--tool-id", str(tool_ids[0]),],
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


def test_journal_client(
    storage, indexer_scheduler, kafka_prefix: str, kafka_server, consumer: Consumer
):
    """Test the 'swh indexer journal-client' cli tool."""
    producer = Producer(
        {
            "bootstrap.servers": kafka_server,
            "client.id": "test producer",
            "acks": "all",
        }
    )

    STATUS = {"status": "full", "origin": {"url": "file://dev/0000",}}
    producer.produce(
        topic=kafka_prefix + ".origin_visit",
        key=b"bogus",
        value=value_to_kafka(STATUS),
    )

    result = invoke(
        indexer_scheduler,
        False,
        [
            "journal-client",
            "--stop-after-objects",
            "1",
            "--broker",
            kafka_server,
            "--prefix",
            kafka_prefix,
            "--group-id",
            "test-consumer",
        ],
    )

    # Check the output
    expected_output = "Done.\n"
    assert result.exit_code == 0, result.output
    assert result.output == expected_output

    # Check scheduled tasks
    tasks = indexer_scheduler.search_tasks()
    assert len(tasks) == 1
    _assert_tasks_for_origins(tasks, [0])
