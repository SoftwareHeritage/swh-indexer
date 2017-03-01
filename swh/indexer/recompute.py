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

    Hashes to compute are defined across 2 configuration options:

    - compute-new-checksums ([str]): list of hash algorithms that
      swh.model.hashutil.hashdata function should be able to deal
      with.

    - recompute-existing-checksums (bool): a boolean to notify that we
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
        'compute-new-checksums': (
            'list[str]', ['sha3:224', 'blake2:512']),
        # whether checksums that already exist in the DB should be
        # recomputed/updated or left untouched
        'recompute-existing-checksums': ('bool', 'False'),
        'batch': ('int', 100)
    }

    CONFIG_BASE_FILENAME = 'storage/recompute'

    def __init__(self):
        self.config = self.parse_config_file()
        self.storage = get_storage(**self.config['storage'])
        self.batch = self.config['batch']
        self.compute_new_checksums = self.config['compute-new-checkums']
        self.recompute_existing_checksums = set(self.config[
            'recompute_existing_checksums'])

    def run(self, ids):
        """Given a list of ids, (re)compute a given set of checksums on
            contents available in our object storage, and update the
            content table accordingly.

            Args:
               ids ([bytes]): content identifier

        """
        # Determine what checksums to compute
        checksums_algorithms = self.compute_new_checksums
        if self.recompute_existing_checksums:
            checksums_algorithms = checksums_algorithms + set(
                hashutil.ALGORITHMS)

        # Whatever the choice here, we cannot do a thing about sha1
        # since it's the primary key, so removing it from the columns
        # to update
        keys_to_update = list(checksums_algorithms - set(['sha1']))

        for content_ids in utils.grouper(ids, self.batch):
            contents = self.storage.content_get_metadata(content_ids)

            updated_contents = []
            for content in contents:
                raw_contents = list(self.storage.content_get([content['id']]))
                if not raw_contents:  # unknown content
                    continue

                raw_content = raw_contents[0]['data']
                updated_content = hashutil.hashdata(
                    raw_content, algo=checksums_algorithms)

                if updated_content['sha1'] != content['sha1']:
                    self.log.error(
                        "Corrupted content! Old sha1 %s and new one %s don't"
                        " match." % (content['sha1'], updated_content['sha1']))
                    continue

                updated_contents.append(updated_content)

            self.storage.content_update(updated_contents, keys=keys_to_update)
