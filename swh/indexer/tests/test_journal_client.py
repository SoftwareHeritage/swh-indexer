# Copyright (C) 2019-2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from unittest.mock import Mock, patch

from swh.indexer.journal_client import process_journal_objects


def test_one_origin_visit_status():
    mock_scheduler = Mock()
    messages = {
        "origin_visit_status": [{"status": "full", "origin": "file:///dev/zero",},]
    }
    process_journal_objects(
        messages, scheduler=mock_scheduler, task_names={"origin_metadata": "task-name"},
    )
    assert mock_scheduler.create_tasks.called is True
    call_args = mock_scheduler.create_tasks.call_args
    (args, kwargs) = call_args
    assert kwargs == {}
    del args[0][0]["next_run"]
    assert args == (
        [
            {
                "arguments": {"kwargs": {}, "args": (["file:///dev/zero"],),},
                "policy": "oneshot",
                "type": "task-name",
                "retries_left": 1,
            },
        ],
    )


def test_origin_visit_legacy():
    mock_scheduler = Mock()
    messages = {
        "origin_visit_status": [
            {"status": "full", "origin": {"url": "file:///dev/zero",}},
        ]
    }
    process_journal_objects(
        messages, scheduler=mock_scheduler, task_names={"origin_metadata": "task-name"},
    )
    assert mock_scheduler.create_tasks.called is True
    call_args = mock_scheduler.create_tasks.call_args
    (args, kwargs) = call_args
    assert kwargs == {}
    del args[0][0]["next_run"]
    assert args == (
        [
            {
                "arguments": {"kwargs": {}, "args": (["file:///dev/zero"],),},
                "policy": "oneshot",
                "type": "task-name",
                "retries_left": 1,
            },
        ],
    )


def test_one_origin_visit_batch():
    mock_scheduler = Mock()
    messages = {
        "origin_visit_status": [
            {"status": "full", "origin": "file:///dev/zero",},
            {"status": "full", "origin": "file:///tmp/foobar",},
        ]
    }
    process_journal_objects(
        messages, scheduler=mock_scheduler, task_names={"origin_metadata": "task-name"},
    )
    assert mock_scheduler.create_tasks.called is True
    call_args = mock_scheduler.create_tasks.call_args
    (args, kwargs) = call_args
    assert kwargs == {}
    del args[0][0]["next_run"]
    assert args == (
        [
            {
                "arguments": {
                    "kwargs": {},
                    "args": (["file:///dev/zero", "file:///tmp/foobar"],),
                },
                "policy": "oneshot",
                "type": "task-name",
                "retries_left": 1,
            },
        ],
    )


@patch("swh.indexer.journal_client.MAX_ORIGINS_PER_TASK", 2)
def test_origin_visit_batches():
    mock_scheduler = Mock()
    messages = {
        "origin_visit_status": [
            {"status": "full", "origin": "file:///dev/zero",},
            {"status": "full", "origin": "file:///tmp/foobar",},
            {"status": "full", "origin": "file:///tmp/spamegg",},
        ]
    }
    process_journal_objects(
        messages, scheduler=mock_scheduler, task_names={"origin_metadata": "task-name"},
    )
    assert mock_scheduler.create_tasks.called is True
    call_args = mock_scheduler.create_tasks.call_args
    (args, kwargs) = call_args
    assert kwargs == {}
    del args[0][0]["next_run"]
    del args[0][1]["next_run"]
    assert args == (
        [
            {
                "arguments": {
                    "kwargs": {},
                    "args": (["file:///dev/zero", "file:///tmp/foobar"],),
                },
                "policy": "oneshot",
                "type": "task-name",
                "retries_left": 1,
            },
            {
                "arguments": {"kwargs": {}, "args": (["file:///tmp/spamegg"],),},
                "policy": "oneshot",
                "type": "task-name",
                "retries_left": 1,
            },
        ],
    )
