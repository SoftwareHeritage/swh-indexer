# Copyright (C) 2016-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import click

from subprocess import Popen, PIPE
from swh.scheduler import utils

from .indexer import BaseIndexer


def compute_mimetype_encoding(raw_content):
    """Determine mimetype and encoding from the raw content.

    Args:
        raw_content (bytes): content's raw data

    Returns:
        A dict with mimetype and encoding key and corresponding values.

    """
    with Popen(['file', '--mime', '-'], stdin=PIPE,
               stdout=PIPE, stderr=PIPE) as p:
        properties, _ = p.communicate(raw_content)

        if properties:
            res = properties.split(b': ')[1].strip().split(b'; ')
            mimetype = res[0]
            encoding = res[1].split(b'=')[1]
            return {
                'mimetype': mimetype,
                'encoding': encoding
            }


class ContentMimetypeIndexer(BaseIndexer):
    """Indexer in charge of:
    - filtering out content already indexed
    - reading content from objstorage per the content's id (sha1)
    - computing {mimetype, encoding} from that content
    - store result in storage

    """
    ADDITIONAL_CONFIG = {
        # chained queue message, e.g:
        # swh.indexer.tasks.SWHOrchestratorTextContentsTask
        'destination_queue': ('str', None),
        'tool': ('dict', {
            'name': 'file',
            'version': '5.22'
        }),
    }

    CONFIG_BASE_FILENAME = 'indexer/mimetype'

    def __init__(self):
        super().__init__()
        destination_queue = self.config.get('destination_queue')
        if destination_queue:
            self.task_destination = utils.get_task(destination_queue)
        else:
            self.task_destination = None
        self.tool_name = self.config['tool']['name']
        self.tool_version = self.config['tool']['version']

    def filter_contents(self, sha1s):
        """Filter out known sha1s and return only missing ones.

        """
        yield from self.storage.content_mimetype_missing((
            {
                'id': sha1,
                'tool_name': self.tool_name,
                'tool_version': self.tool_version
            } for sha1 in sha1s
        ))

    def index_content(self, sha1, raw_content):
        """Index sha1s' content and store result.

        Args:
            sha1 (bytes): content's identifier
            raw_content (bytes): raw content in bytes

        Returns:
            A dict, representing a content_mimetype, with keys:
              - id (bytes): content's identifier (sha1)
              - mimetype (bytes): mimetype in bytes
              - encoding (bytes): encoding in bytes

        """
        properties = compute_mimetype_encoding(raw_content)
        properties.update({
            'id': sha1,
            'tool_name': self.tool_name,
            'tool_version': self.tool_version,
        })

        return properties

    def persist_index_computations(self, results, policy_update):
        """Persist the results in storage.

        Args:
            results ([dict]): list of content_mimetype, dict with the
            following keys:
              - id (bytes): content's identifier (sha1)
              - mimetype (bytes): mimetype in bytes
              - encoding (bytes): encoding in bytes
            policy_update ([str]): either 'update-dups' or 'ignore-dups' to
            respectively update duplicates or ignore them

        """
        self.storage.content_mimetype_add(
            results, conflict_update=(policy_update == 'update-dups'))

    def _filter_text(self, results):
        """Filter sha1 whose raw content is text.

        """
        for result in results:
            if b'binary' in result['encoding']:
                continue
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
        if self.task_destination:
            self.task_destination.delay(list(self._filter_text(results)))


@click.command()
@click.option('--path', help="Path to execute index on")
def main(path):
    with open(path, 'rb') as f:
        raw_content = f.read()

    print(compute_mimetype_encoding(raw_content))


if __name__ == '__main__':
    main()
