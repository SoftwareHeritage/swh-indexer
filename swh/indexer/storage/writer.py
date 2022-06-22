# Copyright (C) 2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Any, Callable, Dict, Iterable, Optional

import attr

try:
    from swh.journal.writer import JournalWriterInterface, get_journal_writer
except ImportError:
    get_journal_writer = None  # type: ignore
    # mypy limitation, see https://github.com/python/mypy/issues/1153

from .model import BaseRow


class JournalWriter:
    """Journal writer storage collaborator. It's in charge of adding objects to
    the journal.

    """

    journal: Optional[JournalWriterInterface]

    def __init__(self, tool_getter: Callable[[int], Dict[str, Any]], journal_writer):
        """
        Args:
            tool_getter: a callable that takes a tool_id and return a dict representing
                         a tool object
            journal_writer: configuration passed to
                            `swh.journal.writer.get_journal_writer`
        """
        self._tool_getter = tool_getter
        if journal_writer:
            if get_journal_writer is None:
                raise EnvironmentError(
                    "You need the swh.journal package to use the "
                    "journal_writer feature"
                )
            self.journal = get_journal_writer(
                **journal_writer,
                value_sanitizer=lambda object_type, value_dict: value_dict,
            )
        else:
            self.journal = None

    def write_additions(self, obj_type, entries: Iterable[BaseRow]) -> None:
        if not self.journal:
            return

        # usually, all the additions in a batch are from the same indexer,
        # so this cache allows doing a single query for all the entries.
        tool_cache = {}

        for entry in entries:
            assert entry.object_type == obj_type  # type: ignore
            # get the tool used to generate this addition
            tool_id = entry.indexer_configuration_id
            assert tool_id
            if tool_id not in tool_cache:
                tool_cache[tool_id] = self._tool_getter(tool_id)
            entry = attr.evolve(
                entry, tool=tool_cache[tool_id], indexer_configuration_id=None
            )

            # write to kafka
            self.journal.write_addition(obj_type, entry)
