# Copyright (C) 2016-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import magic

from .indexer import ContentIndexer, ContentRangeIndexer

if not hasattr(magic.Magic, 'from_buffer'):
    raise ImportError(
        'Expected "import magic" to import python-magic, but file_magic '
        'was imported instead.')


def compute_mimetype_encoding(raw_content):
    """Determine mimetype and encoding from the raw content.

    Args:
        raw_content (bytes): content's raw data

    Returns:
        dict: mimetype and encoding key and corresponding values
        (as bytes).

    """
    m = magic.Magic(mime=True, mime_encoding=True)
    res = m.from_buffer(raw_content)
    (mimetype, encoding) = res.split('; charset=')
    return {
        'mimetype': mimetype,
        'encoding': encoding,
    }


class MixinMimetypeIndexer:
    """Mixin mimetype indexer.

    See :class:`MimetypeIndexer` and :class:`MimetypeRangeIndexer`

    """
    ADDITIONAL_CONFIG = {
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

    def index(self, id, data):
        """Index sha1s' content and store result.

        Args:
            id (bytes): content's identifier
            data (bytes): raw content in bytes

        Returns:
            dict: content's mimetype; dict keys being

            - **id** (bytes): content's identifier (sha1)
            - **mimetype** (bytes): mimetype in bytes
            - **encoding** (bytes): encoding in bytes

        """
        properties = compute_mimetype_encoding(data)
        properties.update({
            'id': id,
            'indexer_configuration_id': self.tool['id'],
            })
        return properties

    def persist_index_computations(self, results, policy_update):
        """Persist the results in storage.

        Args:
            results ([dict]): list of content's mimetype dicts
              (see :meth:`.index`)

            policy_update ([str]): either 'update-dups' or 'ignore-dups' to
               respectively update duplicates or ignore them

        """
        self.idx_storage.content_mimetype_add(
            results, conflict_update=(policy_update == 'update-dups'))


class MimetypeIndexer(MixinMimetypeIndexer, ContentIndexer):
    """Mimetype Indexer working on list of content identifiers.

    It:

    - (optionally) filters out content already indexed (cf.
      :meth:`.filter`)
    - reads content from objstorage per the content's id (sha1)
    - computes {mimetype, encoding} from that content
    - stores result in storage

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

    - (optionally) filters out content already indexed (cf
      :meth:`.indexed_contents_in_range`)
    - reads content from objstorage per the content's id (sha1)
    - computes {mimetype, encoding} from that content
    - stores result in storage

    """
    def indexed_contents_in_range(self, start, end):
        """Retrieve indexed content id within range [start, end].

        Args:
            start (bytes): Starting bound from range identifier
            end (bytes): End range identifier

        Returns:
            dict: a dict with keys:

            - **ids** [bytes]: iterable of content ids within the range.
            - **next** (Optional[bytes]): The next range of sha1 starts at
              this sha1 if any

        """
        return self.idx_storage.content_mimetype_get_range(
            start, end, self.tool['id'])
