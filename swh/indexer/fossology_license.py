# Copyright (C) 2016-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import subprocess

from swh.model import hashutil

from .indexer import ContentIndexer, ContentRangeIndexer, write_to_temp


def compute_license(path, log=None):
    """Determine license from file at path.

    Args:
        path: filepath to determine the license

    Returns:
        dict: A dict with the following keys:

        - licenses ([str]): associated detected licenses to path
        - path (bytes): content filepath

    """
    try:
        properties = subprocess.check_output(['nomossa', path],
                                             universal_newlines=True)
        if properties:
            res = properties.rstrip().split(' contains license(s) ')
            licenses = res[1].split(',')
        else:
            licenses = []

        return {
            'licenses': licenses,
            'path': path,
        }
    except subprocess.CalledProcessError:
        if log:
            from os import path as __path
            log.exception('Problem during license detection for sha1 %s' %
                          __path.basename(path))
        return {
            'licenses': [],
            'path': path,
        }


class MixinFossologyLicenseIndexer:
    """Mixin fossology license indexer.

    See :class:`FossologyLicenseIndexer` and
    :class:`FossologyLicenseRangeIndexer`

    """
    ADDITIONAL_CONFIG = {
        'workdir': ('str', '/tmp/swh/indexer.fossology.license'),
        'tools': ('dict', {
            'name': 'nomos',
            'version': '3.1.0rc2-31-ga2cbb8c',
            'configuration': {
                'command_line': 'nomossa <filepath>',
            },
        }),
        'write_batch_size': ('int', 1000),
    }

    CONFIG_BASE_FILENAME = 'indexer/fossology_license'

    def prepare(self):
        super().prepare()
        self.working_directory = self.config['workdir']

    def index(self, id, data):
        """Index sha1s' content and store result.

        Args:
            id (bytes): content's identifier
            raw_content (bytes): associated raw content to content id

        Returns:
            dict: A dict, representing a content_license, with keys:

            - id (bytes): content's identifier (sha1)
            - license (bytes): license in bytes
            - path (bytes): path
            - indexer_configuration_id (int): tool used to compute the output

        """
        assert isinstance(id, bytes)
        with write_to_temp(
                filename=hashutil.hash_to_hex(id),  # use the id as pathname
                data=data,
                working_directory=self.working_directory) as content_path:
            properties = compute_license(path=content_path, log=self.log)
            properties.update({
                'id': id,
                'indexer_configuration_id': self.tool['id'],
            })
        return properties

    def persist_index_computations(self, results, policy_update):
        """Persist the results in storage.

        Args:
            results ([dict]): list of content_license, dict with the
              following keys:

              - id (bytes): content's identifier (sha1)
              - license (bytes): license in bytes
              - path (bytes): path

            policy_update ([str]): either 'update-dups' or 'ignore-dups' to
              respectively update duplicates or ignore them

        """
        self.idx_storage.content_fossology_license_add(
            results, conflict_update=(policy_update == 'update-dups'))


class FossologyLicenseIndexer(
        MixinFossologyLicenseIndexer, ContentIndexer):
    """Indexer in charge of:

    - filtering out content already indexed
    - reading content from objstorage per the content's id (sha1)
    - computing {license, encoding} from that content
    - store result in storage

    """
    def filter(self, ids):
        """Filter out known sha1s and return only missing ones.

        """
        yield from self.idx_storage.content_fossology_license_missing((
            {
                'id': sha1,
                'indexer_configuration_id': self.tool['id'],
            } for sha1 in ids
        ))


class FossologyLicenseRangeIndexer(
        MixinFossologyLicenseIndexer, ContentRangeIndexer):
    """FossologyLicense Range Indexer working on range of content identifiers.

    - filters out the non textual content
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
        return self.idx_storage.content_fossology_license_get_range(
                start, end, self.tool['id'])
