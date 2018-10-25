# Copyright (C) 2016-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import click
import magic

from swh.model import hashutil
from swh.scheduler import utils

from .indexer import ContentIndexer


def compute_mimetype_encoding(raw_content):
    """Determine mimetype and encoding from the raw content.

    Args:
        raw_content (bytes): content's raw data

    Returns:
        A dict with mimetype and encoding key and corresponding values
        (as bytes).

    """
    r = magic.detect_from_content(raw_content)
    return {
        'mimetype': r.mime_type.encode('utf-8'),
        'encoding': r.encoding.encode('utf-8'),
    }


class ContentMimetypeIndexer(ContentIndexer):
    """Indexer in charge of:

    - filtering out content already indexed
    - reading content from objstorage per the content's id (sha1)
    - computing {mimetype, encoding} from that content
    - store result in storage

    """
    ADDITIONAL_CONFIG = {
        'destination_task': ('str', None),
        'tools': ('dict', {
            'name': 'file',
            'version': '1:5.30-1+deb9u1',
            'configuration': {
                "type": "library",
                "debian-package": "python3-magic"
            },
        }),
    }

    CONFIG_BASE_FILENAME = 'indexer/mimetype'

    def prepare(self):
        super().prepare()
        destination_task = self.config.get('destination_task')
        if destination_task:
            self.destination_task = utils.get_task(destination_task)
        else:
            self.destination_task = None
        self.tool = self.tools[0]

    def filter(self, ids):
        """Filter out known sha1s and return only missing ones.

        """
        yield from self.idx_storage.content_mimetype_missing((
            {
                'id': sha1,
                'indexer_configuration_id': self.tool['id'],
            } for sha1 in ids
        ))

    def index(self, id, data):
        """Index sha1s' content and store result.

        Args:
            id (bytes): content's identifier
            data (bytes): raw content in bytes

        Returns:
            A dict, representing a content_mimetype, with keys:

              - id (bytes): content's identifier (sha1)
              - mimetype (bytes): mimetype in bytes
              - encoding (bytes): encoding in bytes

        """
        try:
            properties = compute_mimetype_encoding(data)
            properties.update({
                'id': id,
                'indexer_configuration_id': self.tool['id'],
                })
        except TypeError:
            self.log.error('Detecting mimetype error for id %s' % (
                hashutil.hash_to_hex(id), ))
            return None

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
        self.idx_storage.content_mimetype_add(
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
        if self.destination_task:
            self.destination_task.delay(list(self._filter_text(results)))


@click.command()
@click.option('--path', help="Path to execute index on")
def main(path):
    with open(path, 'rb') as f:
        raw_content = f.read()

    print(compute_mimetype_encoding(raw_content))


if __name__ == '__main__':
    main()
