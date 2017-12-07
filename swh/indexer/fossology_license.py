# Copyright (C) 2016-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import click
import subprocess

from swh.model import hashutil

from .indexer import ContentIndexer, DiskIndexer


def compute_license(path, log=None):
    """Determine license from file at path.

    Args:
        path: filepath to determine the license

    Returns:
        A dict with the following keys:
        - licenses ([str]): associated detected licenses to path
        - path (bytes): content filepath
        - tool (str): tool used to compute the output

    """
    try:
        properties = subprocess.check_output(['nomossa', path],
                                             universal_newlines=True)
        if properties:
            res = properties.rstrip().split(' contains license(s) ')
            licenses = res[1].split(',')

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


class ContentFossologyLicenseIndexer(ContentIndexer, DiskIndexer):
    """Indexer in charge of:
    - filtering out content already indexed
    - reading content from objstorage per the content's id (sha1)
    - computing {license, encoding} from that content
    - store result in storage

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
    }

    CONFIG_BASE_FILENAME = 'indexer/fossology_license'

    def prepare(self):
        super().prepare()
        self.working_directory = self.config['workdir']
        self.tool = self.tools[0]

    def filter(self, ids):
        """Filter out known sha1s and return only missing ones.

        """
        yield from self.idx_storage.content_fossology_license_missing((
            {
                'id': sha1,
                'indexer_configuration_id': self.tool['id'],
            } for sha1 in ids
        ))

    def index(self, id, data):
        """Index sha1s' content and store result.

        Args:
            sha1 (bytes): content's identifier
            raw_content (bytes): raw content in bytes

        Returns:
            A dict, representing a content_license, with keys:
              - id (bytes): content's identifier (sha1)
              - license (bytes): license in bytes
              - path (bytes): path

        """
        filename = hashutil.hash_to_hex(id)
        content_path = self.write_to_temp(
            filename=filename,
            data=data)

        try:
            properties = compute_license(path=content_path, log=self.log)
            properties.update({
                'id': id,
                'indexer_configuration_id': self.tool['id'],
            })
        finally:
            self.cleanup(content_path)

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


@click.command(help='Compute license for path using tool')
@click.option('--tool', default='nomossa', help="Path to tool")
@click.option('--path', required=1, help="Path to execute index on")
def main(tool, path):
    print(compute_license(tool, path))


if __name__ == '__main__':
    main()
