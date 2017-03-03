# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from swh.model import hashutil
from swh.core import utils
from swh.core.config import SWHConfig
from swh.storage import get_storage


class RecomputeChecksums(SWHConfig):
    """Class in charge of (re)computing blob's hashes.

    Corrupted contents won't be updated.

    Hashes to compute are defined across 2 configuration options:

    - primary_key ([str]): List of keys composing the primary key of a
      content in the storage db.

    - compute_new_checksums ([str]): list of hash algorithms that
      swh.model.hashutil.hashdata function should be able to deal
      with.

    - recompute_existing_checksums (bool): a boolean to notify that we
      also want to recompute existing hash (as defined in
      swh.model.hashutil.ALGORITHMS). As an important design detail,
      there is currently one limitation about sha1. Since it's a
      primary key on content, this cannot be updated so the
      computations are skipped for that one.

    """
    DEFAULT_CONFIG = {
        'storage': ('dict', {
            'cls': 'remote',
            'args': {
              'url': 'http://localhost:5002/'
            },
        }),
        # the set of checksums that should be computed. For
        # variable-length checksums a desired checksum length should also
        # be provided.
        'compute_new_checksums': (
            'list[str]', ['sha3:224', 'blake2:512']),
        # whether checksums that already exist in the DB should be
        # recomputed/updated or left untouched
        'recompute_existing_checksums': ('bool', 'False'),
        # primary key used for content. This will serve to check the
        # data is not corrupted. The content sent should reflect the
        # keys defined here.
        'primary_key': ('list[str]', ['sha1']),
        # Number of contents to retrieve blobs at the same time
        'batch_size_retrieve_content': ('int', 10),
        # Number of contents to update at the same time
        'batch_size_update': ('int', 100)
    }

    CONFIG_BASE_FILENAME = 'storage/recompute'

    def __init__(self):
        self.config = self.parse_config_file()
        self.storage = get_storage(**self.config['storage'])
        self.compute_new_checksums = self.config['compute-new-checkums']
        self.recompute_existing_checksums = set(self.config[
            'recompute_existing_checksums'])
        self.primary_key = set(self.config['primary_key'])
        self.batch_size_retrieve_content = self.config[
            'batch_size_retrieve_content']
        self.batch_size_update = self.config[
            'batch_size_update']

        for key in self.primary_key:
            if key not in hashutil.ALGORITHMS:
                raise ValueError('Primary key should be in %s' %
                                 hashutil.ALGORITHMS)

    def get_new_contents_metadata(self, all_contents, checksum_algorithms):
        """Retrieve raw contents and compute new checksums on the
           contents. Unknown or corrupted contents are skipped.

        Args:
            ids ([dict]): Content with the necessary keys (cf. primary_key
                        option)
            checksum_algorithms ([str]): List of checksums to compute

        """
        for contents in utils.grouper(all_contents,
                                      self.batch_size_retrieve_content):
            # Retrieve the raw data
            contents = self.storage.content_get(
                (c['sha1'] for c in contents))

            for content in contents:
                raw_content = content['data']

                updated_content = hashutil.hashdata(
                    raw_content, algo=checksum_algorithms)

                # Check the invariant primary key
                for key in self.primary_key:
                    old_value = content[key]
                    new_value = updated_content[key]
                    if old_value != new_value:
                        self.log.error(
                            "Corrupted content! Old %s %s and new one %s don't"
                            " match." % (key, old_value, new_value))
                    continue

                yield updated_content

    def run(self, contents):
        """Given a list of content (dict):
            - (re)compute a given set of checksums on contents
              available in our object storage
            - update those contents with the new metadata

            Args:
                - ids ([dict]): content identifier as dictionary. The
                  key present in such dictionary should be the ones
                  defined in the 'primary_key' option.

        """
        # Determine checksums to compute
        checksum_algorithms = self.compute_new_checksums
        if self.recompute_existing_checksums:
            checksum_algorithms = checksum_algorithms + set(
                hashutil.ALGORITHMS)

        # Whatever the choice on checksums to recompute, we cannot
        # update the 'composite' primary key so removing it from the
        # columns to update
        keys_to_update = list(checksum_algorithms - self.primary_key)

        for contents in utils.grouper(
                self.get_new_contents_metadata(self, contents),
                self.batch_size_update):
            self.storage.content_update(list(contents),
                                        keys=keys_to_update)
