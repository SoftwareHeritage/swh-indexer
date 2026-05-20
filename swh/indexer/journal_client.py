# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Dict

from swh.indexer.exception import ReportableException
from swh.indexer.indexer import BaseIndexer
from swh.journal.client import JournalClientBase


class IndexerJournalClient(JournalClientBase):
    indexer: BaseIndexer

    def __init__(self, indexer, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # The indexer collaborator which does indexation computation out of objects read
        # from kafka
        self.indexer = indexer
        # Let's share the same error reporter so internal error raised can be managed
        # within the indexer
        indexer.error_reporter = self.error_reporter

    def process_one_object(self, decoded_object, decoded_object_type, raw_message):
        """Implementation is in charge of calling the run method of the indexer and trap
        any error to report it within the error_reporter.

        """
        if decoded_object_type in self.indexer.object_types:
            try:
                # Make the indexer index the object
                self.indexer.run([decoded_object])
            except ReportableException as exc:
                # If any reportable exception is raised, let it be reported to the error
                # reporter if any
                if self.error_reporter is None:
                    raise

                self.log_error_report(
                    decoded_object,
                    decoded_object_type,
                    raw_message,
                    exc,
                    self.indexer.run,
                )

    def log_error_report(
        self,
        object_d: Dict,
        object_type: str,
        raw_message,
        exc: Exception,
        operation: str,
    ) -> None:
        """Method called when issue must be reported without failing the index process.

        This always logs the issue in sentry. This can also optionally logs the issue in
        another reporter (when said reporter is declared in configuration).

        Args:
            obj_id: The object identifier which raised a problem.
            exc: The exception raised and caught
            operation: The operation where it was triggered
            information: A dictionary of extra information to summarize the context

        Returns:
            None

        """
        import sentry_sdk

        from swh.model.hashutil import hash_to_hex

        # ReportedException's first argument is a string describing the issue
        # The second argument is actually the id of the object in error
        obj_id = exc.args[1]

        obj_id_str = hash_to_hex(obj_id)
        error_context = {
            "obj_id": obj_id_str,
            "operation": operation,
            "exc": str(exc),
            "raw_message": raw_message,
        }
        for k, v in object_d.items():
            if isinstance(v, bytes):
                value = hash_to_hex(v)
            else:
                value = value
            error_context[k] = value

        # Report in sentry
        with sentry_sdk.push_scope() as scope:
            for k, v in error_context.items():
                scope.set_extra(k, v)
            sentry_sdk.capture_exception(exc)

        self.indexer.log.error(
            "Failed operation %(operation)s on %(obj_id)s exception: %(exc)s",
            error_context,
        )

        from msgpack import dumps

        oid = f"swh:1:{object_type}:{obj_id_str}"
        msg = dumps(error_context)
        assert self.error_reporter is not None
        self.error_reporter(oid, msg)
