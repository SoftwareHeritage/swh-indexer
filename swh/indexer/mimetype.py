# Copyright (C) 2016-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import click
import magic

from swh.model import hashutil
from swh.scheduler import get_scheduler

from .indexer import ContentIndexer, ContentRangeIndexer


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


class MixinMimetypeIndexer:
    """Mixin mimetype indexer.

    See :class:`ContentMimetypeIndexer` and :class:`MimetypeRangeIndexer`

    """
    ADDITIONAL_CONFIG = {
        'scheduler': ('dict', {
            'cls': 'remote',
            'args': {
                'url': 'http://localhost:5008',
            },
        }),
        'tools': ('dict', {
            'name': 'file',
            'version': '1:5.30-1+deb9u1',
            'configuration': {
                "type": "library",
                "debian-package": "python3-magic"
            },
        }),
        'write_batch_size': ('int', 1000),
    }

    CONFIG_BASE_FILENAME = 'indexer/mimetype'

    def prepare(self):
        super().prepare()
        self.scheduler = get_scheduler(**self.config['scheduler'])
        self.tool = self.tools[0]

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


class ContentMimetypeIndexer(MixinMimetypeIndexer, ContentIndexer):
    """Mimetype Indexer working on list of content identifiers.

    It:
    - (optionally) filters out content already indexed (cf. :callable:`filter`)
    - reads content from objstorage per the content's id (sha1)
    - computes {mimetype, encoding} from that content
    - stores result in storage

    FIXME:
    - 1. Rename redundant ContentMimetypeIndexer to MimetypeIndexer
    - 2. Do we keep it afterwards? ~> i think this can be used with the journal

    """
    def filter(self, ids):
        """Filter out known sha1s and return only missing ones.

        """
        yield from self.idx_storage.content_mimetype_missing((
            {
                'id': sha1,
                'indexer_configuration_id': self.tool['id'],
            } for sha1 in ids
        ))


class MimetypeRangeIndexer(MixinMimetypeIndexer, ContentRangeIndexer):
    """Mimetype Range Indexer working on range of content identifiers.

    It:
    - (optionally) filters out content already indexed (cf :callable:`range`)
    - reads content from objstorage per the content's id (sha1)
    - computes {mimetype, encoding} from that content
    - stores result in storage

    """
    def indexed_contents_in_range(self, start, end):
        """Retrieve indexed content id within range [start, end].

        Args
            **start** (bytes): Starting bound from range identifier
            **end** (bytes): End range identifier

        Yields:
            Content identifier (bytes) present in the range [start, end]

        """
        while start:
            result = self.idx_storage.content_mimetype_get_range(
                start, end, self.tool['id'])
            contents = result['ids']
            for _id in contents:
                yield _id
            start = result['next']


@click.command()
@click.option('--path', help="Path to execute index on")
def main(path):
    with open(path, 'rb') as f:
        raw_content = f.read()

    print(compute_mimetype_encoding(raw_content))


if __name__ == '__main__':
    main()
