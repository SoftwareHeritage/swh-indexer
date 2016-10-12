# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import click
import subprocess

from swh.core import hashutil
from swh.scheduler.celery_backend.config import app

from .indexer import BaseIndexer, DiskIndexer


def compute_mimetype_encoding(path):
    """Determine mimetype and encoding from file at path.

    Args:
        path: filepath to determine the mime type

    Returns:
        A dict with mimetype and encoding key and corresponding values.

    """
    cmd = ['file', '--mime', path]
    properties = subprocess.check_output(cmd)
    if properties:
        res = properties.split(b': ')[1].strip().split(b'; ')
        mimetype = res[0]
        encoding = res[1].split(b'=')[1]
        return {
            'mimetype': mimetype,
            'encoding': encoding
        }


class ContentMimetypeIndexer(BaseIndexer, DiskIndexer):
    """Indexer in charge of:
    - filtering out content already indexed
    - reading content from objstorage per the content's id (sha1)
    - computing {mimetype, encoding} from that content
    - store result in storage

    """
    ADDITIONAL_CONFIG = {
        'workdir': ('str', '/tmp/swh/indexer.mimetype'),
        'destination_queue': (
            'str', 'swh.indexer.tasks.SWHOrchestratorTextContentsTask')
    }

    CONFIG_BASE_FILENAME = 'indexer/mimetype'

    def __init__(self):
        super().__init__()
        self.working_directory = self.config['workdir']
        destination_queue = self.config['destination_queue']
        self.task_destination = app.tasks[destination_queue]

    def filter_contents(self, sha1s):
        """Filter out known sha1s and return only missing ones.

        """
        yield from self.storage.content_mimetype_missing(sha1s)

    def index_content(self, sha1, content):
        """Index sha1s' content and store result.

        Args:
            sha1 (bytes): content's identifier
            content (bytes): raw content in bytes

        Returns:
            A dict, representing a content_mimetype, with keys:
              - id (bytes): content's identifier (sha1)
              - mimetype (bytes): mimetype in bytes
              - encoding (bytes): encoding in bytes

        """
        filename = hashutil.hash_to_hex(sha1)
        content_path = self.write_to_temp(
            filename=filename,
            data=content)

        properties = compute_mimetype_encoding(content_path)
        properties.update({
            'id': sha1,
        })

        self.cleanup(content_path)
        return properties

    def persist_index_computations(self, results):
        """Persist the results in storage.

        Args:

            results ([dict]): list of content_mimetype, dict with the
            following keys:
              - id (bytes): content's identifier (sha1)
              - mimetype (bytes): mimetype in bytes
              - encoding (bytes): encoding in bytes

        """
        self.storage.content_mimetype_add(results)

    def _filter_text(self, results):
        """Filter sha1 whose raw content is text.

        """
        for result in results:
            if b'text/' in result['mimetype']:
                yield result['id']

    def next_step(self, results):
        """When the computations is done, we'd like to send over only text
        contents to the text content orchestrator.

        Args:
            results ([dict]): List of content_mimetype results, dict
            with the following keys:
              - id (bytes): content's identifier (sha1)
              - mimetype (bytes): mimetype in bytes
              - encoding (bytes): encoding in bytes

        """
        self.task_destination.delay(list(self._filter_text(results)))


@click.command()
@click.option('--path', help="Path to execute index on")
def main(path):
    print(compute_mimetype_encoding(path))


if __name__ == '__main__':
    main()
