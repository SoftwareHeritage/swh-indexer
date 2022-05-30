# Copyright (C) 2019-2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Dict, List
from unittest.mock import patch

import pytest

from swh.indexer.journal_client import process_journal_objects
from swh.scheduler.interface import SchedulerInterface


def search_tasks(indexer_scheduler: SchedulerInterface, task_type) -> List[Dict]:
    tasks = indexer_scheduler.search_tasks(task_type=task_type)

    keys_not_to_compare = ["next_run", "current_interval", "id", "priority", "status"]

    result_tasks = []
    for task in tasks:
        task = dict(task)

        for key in keys_not_to_compare:
            del task[key]

        result_tasks.append(task)

    return result_tasks


@pytest.mark.parametrize(
    "origin",
    [
        "file:///dev/zero",  # current format
        {
            "url": "file:///dev/zero",
        },  # legacy format
    ],
)
def test_journal_client_origin_visit_status(origin, indexer_scheduler):
    messages = {
        "origin_visit_status": [
            {"status": "full", "origin": origin},
        ]
    }
    process_journal_objects(
        messages,
        scheduler=indexer_scheduler,
        task_names={"origin_metadata": "index-origin-metadata"},
    )
    actual_tasks = search_tasks(indexer_scheduler, task_type="index-origin-metadata")

    assert actual_tasks == [
        {
            "arguments": {
                "kwargs": {},
                "args": [["file:///dev/zero"]],
            },
            "policy": "oneshot",
            "type": "index-origin-metadata",
            "retries_left": 1,
        }
    ]


def test_journal_client_one_origin_visit_batch(indexer_scheduler):
    messages = {
        "origin_visit_status": [
            {
                "status": "full",
                "origin": "file:///dev/zero",
            },
            {
                "status": "full",
                "origin": "file:///tmp/foobar",
            },
        ]
    }
    process_journal_objects(
        messages,
        scheduler=indexer_scheduler,
        task_names={"origin_metadata": "index-origin-metadata"},
    )

    actual_tasks = search_tasks(indexer_scheduler, task_type="index-origin-metadata")
    assert actual_tasks == [
        {
            "arguments": {
                "kwargs": {},
                "args": [["file:///dev/zero", "file:///tmp/foobar"]],
            },
            "policy": "oneshot",
            "type": "index-origin-metadata",
            "retries_left": 1,
        }
    ]


@patch("swh.indexer.journal_client.MAX_ORIGINS_PER_TASK", 2)
def test_journal_client_origin_visit_batches(indexer_scheduler):
    messages = {
        "origin_visit_status": [
            {
                "status": "full",
                "origin": "file:///dev/zero",
            },
            {
                "status": "full",
                "origin": "file:///tmp/foobar",
            },
            {
                "status": "full",
                "origin": "file:///tmp/spamegg",
            },
        ]
    }
    process_journal_objects(
        messages,
        scheduler=indexer_scheduler,
        task_names={"origin_metadata": "index-origin-metadata"},
    )
    actual_tasks = search_tasks(indexer_scheduler, task_type="index-origin-metadata")
    assert actual_tasks == [
        {
            "arguments": {
                "kwargs": {},
                "args": [
                    ["file:///dev/zero", "file:///tmp/foobar"],
                ],
            },
            "policy": "oneshot",
            "type": "index-origin-metadata",
            "retries_left": 1,
        },
        {
            "arguments": {
                "kwargs": {},
                "args": [["file:///tmp/spamegg"]],
            },
            "policy": "oneshot",
            "type": "index-origin-metadata",
            "retries_left": 1,
        },
    ]
