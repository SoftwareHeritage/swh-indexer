# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import click
import subprocess

from swh.core import hashutil

from .indexer import BaseIndexer, DiskIndexer


def compute_license(tool, path):
    """Determine license from file at path.

    Args:
        path: filepath to determine the license

    Returns:
        A dict with the following keys:
        - licenses ([str]): associated detected licenses to path
        - path (bytes): content filepath
        - tool (str): tool used to compute the output

    """
    properties = subprocess.check_output([tool, path],
                                         universal_newlines=False)
    if properties:
        res = properties.rstrip().split(' contains license(s) ')
        licenses = res[1].split(',')

        return {
            'licenses': licenses,
            'path': path,
        }


class ContentFossologyLicenseIndexer(BaseIndexer, DiskIndexer):
    """Indexer in charge of:
    - filtering out content already indexed
    - reading content from objstorage per the content's id (sha1)
    - computing {license, encoding} from that content
    - store result in storage

    """
    ADDITIONAL_CONFIG = {
        'workdir': ('str', '/tmp/swh/indexer.fossology.license'),
        'tool': ('dict', {
            'cli': '/usr/local/bin/nomossa',
            'name': 'nomos',
            'version': '3.1.0rc2-31-ga2cbb8c'
        }),
    }

    CONFIG_BASE_FILENAME = 'indexer/fossology_license'

    def __init__(self):
        super().__init__()
        self.working_directory = self.config['workdir']
        self.tool = self.config['tool']['cli']
        self.tool_name = self.config['tool']['name']
        self.tool_version = self.config['tool']['version']

    def filter_contents(self, sha1s):
        """Filter out known sha1s and return only missing ones.

        """
        yield from self.storage.content_fossology_license_missing(sha1s)

    def index_content(self, sha1, content):
        """Index sha1s' content and store result.

        Args:
            sha1 (bytes): content's identifier
            content (bytes): raw content in bytes

        Returns:
            A dict, representing a content_license, with keys:
              - id (bytes): content's identifier (sha1)
              - license (bytes): license in bytes
              - path (bytes): path

        """
        filename = hashutil.hash_to_hex(sha1)
        content_path = self.write_to_temp(
            filename=filename,
            data=content)

        properties = compute_license(self.tool, path=content_path)
        properties.update({
            'id': sha1,
            'tool_name': self.config['tool_name'],
            'tool_version': self.config['tool_version']
        })

        self.log.info('Licenses: %s' % properties['licenses'])

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
        wrong_licenses = self.storage.content_fossology_license_add(
            results, conflict_update=(policy_update == 'update-dups'))

        if wrong_licenses:
            for l in wrong_licenses:
                self.log.warn('Content %s has some unknown licenses: %s' % (
                    hashutil.hash_to_hex(l['id']),
                    ','.join((name.decode('utf-8') for name in l['licenses'])))
                )


@click.command(help='Compute license for path using tool')
@click.option('--tool', default='nomossa', help="Path to tool")
@click.option('--path', required=1, help="Path to execute index on")
def main(tool, path):
    print(compute_license(tool, path))


if __name__ == '__main__':
    main()
