# Copyright (C) 2020-2022 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Any, Dict, Iterable, Optional

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

    def __init__(self, journal_writer: Dict[str, Any]):
        """
        Args:
            journal_writer: configuration passed to
                            `swh.journal.writer.get_journal_writer`
        """
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

        translated = []

        for entry in entries:
            assert entry.object_type == obj_type  # type: ignore

            # ids are internal to the database and should not be sent to postgresql
            if entry.indexer_configuration_id is not None:
                raise ValueError(
                    f"{entry} passed to JournalWriter.write_additions has "
                    f"indexer_configuration_id instead of full tool dict"
                )
            assert entry.tool, "Missing both indexer_configuration_id and tool dict"
            if "id" in entry.tool:
                raise ValueError(
                    f"{entry} passed to JournalWriter.write_additions "
                    f"contains a tool id"
                )

            translated.append(entry)

        # write to kafka
        self.journal.write_additions(obj_type, translated)
