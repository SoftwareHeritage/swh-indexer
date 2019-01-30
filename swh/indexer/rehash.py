# Copyright (C) 2017-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import itertools

from collections import defaultdict

from swh.core import utils
from swh.core.config import SWHConfig
from swh.model import hashutil
from swh.objstorage import get_objstorage
from swh.objstorage.exc import ObjNotFoundError
from swh.storage import get_storage


class RecomputeChecksums(SWHConfig):
    """Class in charge of (re)computing content's hashes.

    Hashes to compute are defined across 2 configuration options:

    compute_checksums ([str])
      list of hash algorithms that
      py:func:`swh.model.hashutil.MultiHash.from_data` function should
      be able to deal with. For variable-length checksums, a desired
      checksum length should also be provided. Their format is
      <algorithm's name>:<variable-length> e.g: blake2:512

    recompute_checksums (bool)
      a boolean to notify that we also want to recompute potential existing
      hashes specified in compute_checksums.  Default to False.

    """
    DEFAULT_CONFIG = {
        # The storage to read from or update metadata to
        'storage': ('dict', {
            'cls': 'remote',
            'args': {
              'url': 'http://localhost:5002/'
            },
        }),
        # The objstorage to read contents' data from
        'objstorage': ('dict', {
            'cls': 'pathslicing',
            'args': {
                'root': '/srv/softwareheritage/objects',
                'slicing': '0:2/2:4/4:6',
            },
        }),
        # the set of checksums that should be computed.
        # Examples: 'sha1_git', 'blake2b512', 'blake2s256'
        'compute_checksums': (
            'list[str]', []),
        # whether checksums that already exist in the DB should be
        # recomputed/updated or left untouched
        'recompute_checksums': ('bool', False),
        # Number of contents to retrieve blobs at the same time
        'batch_size_retrieve_content': ('int', 10),
        # Number of contents to update at the same time
        'batch_size_update': ('int', 100),
    }

    CONFIG_BASE_FILENAME = 'indexer/rehash'

    def __init__(self):
        self.config = self.parse_config_file()
        self.storage = get_storage(**self.config['storage'])
        self.objstorage = get_objstorage(**self.config['objstorage'])
        self.compute_checksums = self.config['compute_checksums']
        self.recompute_checksums = self.config[
            'recompute_checksums']
        self.batch_size_retrieve_content = self.config[
            'batch_size_retrieve_content']
        self.batch_size_update = self.config[
            'batch_size_update']
        self.log = logging.getLogger('swh.indexer.rehash')

        if not self.compute_checksums:
            raise ValueError('Checksums list should not be empty.')

    def _read_content_ids(self, contents):
        """Read the content identifiers from the contents.

        """
        for c in contents:
            h = c['sha1']
            if isinstance(h, str):
                h = hashutil.hash_to_bytes(h)

            yield h

    def get_new_contents_metadata(self, all_contents):
        """Retrieve raw contents and compute new checksums on the
           contents. Unknown or corrupted contents are skipped.

        Args:
            all_contents ([dict]): List of contents as dictionary with
              the necessary primary keys
            checksum_algorithms ([str]): List of checksums to compute

        Yields:
            tuple: tuple of (content to update, list of checksums computed)

        """
        content_ids = self._read_content_ids(all_contents)
        for contents in utils.grouper(content_ids,
                                      self.batch_size_retrieve_content):
            contents_iter = itertools.tee(contents, 2)
            try:
                content_metadata = self.storage.content_get_metadata(
                    [s for s in contents_iter[0]])
            except Exception:
                self.log.exception(
                    'Problem when reading contents metadata.')
                continue

            for content in content_metadata:
                # Recompute checksums provided in compute_checksums options
                if self.recompute_checksums:
                    checksums_to_compute = list(self.compute_checksums)
                else:
                    # Compute checksums provided in compute_checksums
                    # options not already defined for that content
                    checksums_to_compute = [h for h in self.compute_checksums
                                            if not content.get(h)]

                if not checksums_to_compute:  # Nothing to recompute
                    continue

                try:
                    raw_content = self.objstorage.get(content['sha1'])
                except ObjNotFoundError:
                    self.log.warning('Content %s not found in objstorage!' %
                                     content['sha1'])
                    continue

                content_hashes = hashutil.MultiHash.from_data(
                    raw_content, hash_names=checksums_to_compute).digest()
                content.update(content_hashes)
                yield content, checksums_to_compute

    def run(self, contents):
        """Given a list of content:

          - (re)compute a given set of checksums on contents available in our
            object storage
          - update those contents with the new metadata

          Args:
              contents (dict): contents as dictionary with necessary keys.
                  key present in such dictionary should be the ones defined in
                  the 'primary_key' option.

        """
        for data in utils.grouper(
                self.get_new_contents_metadata(contents),
                self.batch_size_update):

            groups = defaultdict(list)
            for content, keys_to_update in data:
                keys = ','.join(keys_to_update)
                groups[keys].append(content)

            for keys_to_update, contents in groups.items():
                keys = keys_to_update.split(',')
                try:
                    self.storage.content_update(contents,
                                                keys=keys)
                except Exception:
                    self.log.exception('Problem during update.')
                    continue
