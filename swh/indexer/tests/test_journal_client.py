# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest
from unittest.mock import Mock, patch

from swh.indexer.journal_client import process_journal_objects


class JournalClientTest(unittest.TestCase):
    def testOneOriginVisit(self):
        mock_scheduler = Mock()
        messages = {
            "origin_visit": [{"status": "full", "origin": "file:///dev/zero",},]
        }
        process_journal_objects(
            messages,
            scheduler=mock_scheduler,
            task_names={"origin_metadata": "task-name"},
        )
        self.assertTrue(mock_scheduler.create_tasks.called)
        call_args = mock_scheduler.create_tasks.call_args
        (args, kwargs) = call_args
        self.assertEqual(kwargs, {})
        del args[0][0]["next_run"]
        self.assertEqual(
            args,
            (
                [
                    {
                        "arguments": {
                            "kwargs": {"policy_update": "update-dups"},
                            "args": (["file:///dev/zero"],),
                        },
                        "policy": "oneshot",
                        "type": "task-name",
                    },
                ],
            ),
        )

    def testOriginVisitLegacy(self):
        mock_scheduler = Mock()
        messages = {
            "origin_visit": [
                {"status": "full", "origin": {"url": "file:///dev/zero",}},
            ]
        }
        process_journal_objects(
            messages,
            scheduler=mock_scheduler,
            task_names={"origin_metadata": "task-name"},
        )
        self.assertTrue(mock_scheduler.create_tasks.called)
        call_args = mock_scheduler.create_tasks.call_args
        (args, kwargs) = call_args
        self.assertEqual(kwargs, {})
        del args[0][0]["next_run"]
        self.assertEqual(
            args,
            (
                [
                    {
                        "arguments": {
                            "kwargs": {"policy_update": "update-dups"},
                            "args": (["file:///dev/zero"],),
                        },
                        "policy": "oneshot",
                        "type": "task-name",
                    },
                ],
            ),
        )

    def testOneOriginVisitBatch(self):
        mock_scheduler = Mock()
        messages = {
            "origin_visit": [
                {"status": "full", "origin": "file:///dev/zero",},
                {"status": "full", "origin": "file:///tmp/foobar",},
            ]
        }
        process_journal_objects(
            messages,
            scheduler=mock_scheduler,
            task_names={"origin_metadata": "task-name"},
        )
        self.assertTrue(mock_scheduler.create_tasks.called)
        call_args = mock_scheduler.create_tasks.call_args
        (args, kwargs) = call_args
        self.assertEqual(kwargs, {})
        del args[0][0]["next_run"]
        self.assertEqual(
            args,
            (
                [
                    {
                        "arguments": {
                            "kwargs": {"policy_update": "update-dups"},
                            "args": (["file:///dev/zero", "file:///tmp/foobar"],),
                        },
                        "policy": "oneshot",
                        "type": "task-name",
                    },
                ],
            ),
        )

    @patch("swh.indexer.journal_client.MAX_ORIGINS_PER_TASK", 2)
    def testOriginVisitBatches(self):
        mock_scheduler = Mock()
        messages = {
            "origin_visit": [
                {"status": "full", "origin": "file:///dev/zero",},
                {"status": "full", "origin": "file:///tmp/foobar",},
                {"status": "full", "origin": "file:///tmp/spamegg",},
            ]
        }
        process_journal_objects(
            messages,
            scheduler=mock_scheduler,
            task_names={"origin_metadata": "task-name"},
        )
        self.assertTrue(mock_scheduler.create_tasks.called)
        call_args = mock_scheduler.create_tasks.call_args
        (args, kwargs) = call_args
        self.assertEqual(kwargs, {})
        del args[0][0]["next_run"]
        del args[0][1]["next_run"]
        self.assertEqual(
            args,
            (
                [
                    {
                        "arguments": {
                            "kwargs": {"policy_update": "update-dups"},
                            "args": (["file:///dev/zero", "file:///tmp/foobar"],),
                        },
                        "policy": "oneshot",
                        "type": "task-name",
                    },
                    {
                        "arguments": {
                            "kwargs": {"policy_update": "update-dups"},
                            "args": (["file:///tmp/spamegg"],),
                        },
                        "policy": "oneshot",
                        "type": "task-name",
                    },
                ],
            ),
        )
