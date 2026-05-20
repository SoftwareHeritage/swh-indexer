# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Any, Dict

from swh.indexer.exception import ReportableError
from swh.indexer.indexer import BaseIndexer
from swh.journal.client import JournalClientBase


class IndexerJournalClient(JournalClientBase):
    indexer: BaseIndexer

    def __init__(self, indexer, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # The indexer collaborator which does indexation computation out of objects read
        # from kafka
        self.indexer = indexer

    def process_one_object(self, decoded_object, decoded_object_type, raw_message):
        """Implementation is in charge of calling the run method of the indexer and trap
        any error to report it within the error_reporter.

        """
        if decoded_object_type in self.indexer.object_types:
            try:
                # Make the indexer index the object
                self.indexer.run([decoded_object])
            except ReportableError as exc:
                # If any reportable error is raised, let it be reported to the error
                # reporter if any, otherwise propagate the error to the main process as
                # usual
                if self.error_reporter is None:
                    raise

                self.log_error_report(
                    decoded_object,
                    decoded_object_type,
                    raw_message,
                    exc,
                    str(self.indexer.run),
                )

    def log_error_report(
        self,
        object_d: Dict,
        object_type: str,
        msg,
        exc: Exception,
        operation: str,
    ) -> None:
        """Method called when issue must be reported without failing the index process.

        This always logs the issue in sentry. This can also optionally logs the issue in
        another reporter (when said reporter is declared in configuration).

        Args:
            object_d: The object dict which created a problem
            object_type: its associated object_type
            msg: The raw kafka message read from the topic
            exc: The exception raised and caught (it contains the id of the object)
            operation: The operation method called which raised the issue

        Returns:
            None

        """
        from msgpack import dumps
        import sentry_sdk

        from swh.model.hashutil import hash_to_hex

        # ReportedException's first argument is a string describing the issue
        # The second argument is actually the id of the object in error
        obj_id = exc.args[1]

        obj_id_str = hash_to_hex(obj_id)
        error_context: Dict[str, Any] = {
            "obj_id": obj_id_str,
            "object_type": object_type,
            "operation": operation,
            "exc": str(exc),
        }

        error_context["raw_message"] = {
            "key": msg.key(),  # bytes or None
            "value": msg.value(),  # bytes or None
            "partition": msg.partition(),  # int
            "topic": msg.topic(),  # str
            "timestamp": msg.timestamp(),
        }

        # Report in sentry
        with sentry_sdk.push_scope() as scope:
            for k, v in error_context.items():
                scope.set_extra(k, v)
            sentry_sdk.capture_exception(exc)

        self.indexer.log.error(
            "Failed operation %(operation)s on %(obj_id)s exception: %(exc)s",
            error_context,
        )

        oid = f"{object_type}:{obj_id_str}"

        msg = dumps(error_context)
        assert self.error_reporter is not None
        self.error_reporter(oid, msg)
