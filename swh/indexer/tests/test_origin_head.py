# Copyright (C) 2017-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from swh.indexer.origin_head import OriginHeadIndexer
from swh.indexer.tests.utils import (
    BASE_TEST_CONFIG, fill_storage
)

ORIGIN_HEAD_CONFIG = {
    **BASE_TEST_CONFIG,
    'tools': {
        'name': 'origin-metadata',
        'version': '0.0.1',
        'configuration': {},
    },
    'tasks': {
        'revision_intrinsic_metadata': None,
        'origin_intrinsic_metadata': None,
    }
}


class OriginHeadTestIndexer(OriginHeadIndexer):
    """Specific indexer whose configuration is enough to satisfy the
       indexing tests.
    """
    def parse_config_file(self, *args, **kwargs):
        return ORIGIN_HEAD_CONFIG

    def persist_index_computations(self, results, policy_update):
        self.results = results


class OriginHead(unittest.TestCase):
    def setUp(self):
        self.indexer = OriginHeadTestIndexer()
        self.indexer.catch_exceptions = False
        fill_storage(self.indexer.storage)

    def test_git(self):
        self.indexer.run(
                ['https://github.com/SoftwareHeritage/swh-storage'])
        self.assertEqual(self.indexer.results, [{
            'revision_id': b'8K\x12\x00d\x03\xcc\xe4]bS\xe3\x8f{'
                           b'\xd7}\xac\xefrm',
            'origin_url': 'https://github.com/SoftwareHeritage/swh-storage'}])

    def test_git_partial_snapshot(self):
        """Checks partial snapshots are ignored."""
        origin_url = 'https://github.com/SoftwareHeritage/swh-core'
        self.indexer.storage.origin_add_one({
            'type': 'git',
            'url': origin_url,
        })
        visit = self.indexer.storage.origin_visit_add(
            origin_url, '2019-02-27')
        self.indexer.storage.snapshot_add([{
            'id': b'foo',
            'branches': {
                b'foo': None,
                b'HEAD': {
                    'target_type': 'alias',
                    'target': b'foo',
                }
            }
        }])
        self.indexer.storage.origin_visit_update(
            origin_url, visit['visit'], status='partial', snapshot=b'foo')
        self.indexer.run([origin_url])
        self.assertEqual(self.indexer.results, [])

    def test_vcs_missing_snapshot(self):
        self.indexer.storage.origin_add([{
            'type': 'git',
            'url': 'https://github.com/SoftwareHeritage/swh-indexer',
        }])
        self.indexer.run(
                ['https://github.com/SoftwareHeritage/swh-indexer'])
        self.assertEqual(self.indexer.results, [])

    def test_pypi_missing_branch(self):
        origin_url = 'https://pypi.org/project/abcdef/'
        self.indexer.storage.origin_add_one({
            'type': 'pypi',
            'url': origin_url,
        })
        visit = self.indexer.storage.origin_visit_add(
            origin_url, '2019-02-27')
        self.indexer.storage.snapshot_add([{
            'id': b'foo',
            'branches': {
                b'foo': None,
                b'HEAD': {
                    'target_type': 'alias',
                    'target': b'foo',
                }
            }
        }])
        self.indexer.storage.origin_visit_update(
            origin_url, visit['visit'], status='full', snapshot=b'foo')
        self.indexer.run(['https://pypi.org/project/abcdef/'])
        self.assertEqual(self.indexer.results, [])

    def test_ftp(self):
        self.indexer.run(
                ['rsync://ftp.gnu.org/gnu/3dldf'])
        self.assertEqual(self.indexer.results, [{
            'revision_id': b'\x8e\xa9\x8e/\xea}\x9feF\xf4\x9f\xfd\xee'
                           b'\xcc\x1a\xb4`\x8c\x8by',
            'origin_url': 'rsync://ftp.gnu.org/gnu/3dldf'}])

    def test_ftp_missing_snapshot(self):
        self.indexer.storage.origin_add([{
            'type': 'ftp',
            'url': 'rsync://ftp.gnu.org/gnu/foobar',
        }])
        self.indexer.run(
                ['rsync://ftp.gnu.org/gnu/foobar'])
        self.assertEqual(self.indexer.results, [])

    def test_deposit(self):
        self.indexer.run(
                ['https://forge.softwareheritage.org/source/'
                 'jesuisgpl/'])
        self.assertEqual(self.indexer.results, [{
            'revision_id': b'\xe7n\xa4\x9c\x9f\xfb\xb7\xf76\x11\x08{'
                           b'\xa6\xe9\x99\xb1\x9e]q\xeb',
            'origin_url': 'https://forge.softwareheritage.org/source/'
                          'jesuisgpl/'}])

    def test_deposit_missing_snapshot(self):
        self.indexer.storage.origin_add([{
            'type': 'deposit',
            'url': 'https://forge.softwareheritage.org/source/foobar',
        }])
        self.indexer.run(
                ['https://forge.softwareheritage.org/source/foobar'])
        self.assertEqual(self.indexer.results, [])

    def test_pypi(self):
        self.indexer.run(
                ['https://pypi.org/project/limnoria/'])
        self.assertEqual(self.indexer.results, [{
            'revision_id': b'\x83\xb9\xb6\xc7\x05\xb1%\xd0\xfem\xd8k'
                           b'A\x10\x9d\xc5\xfa2\xf8t',
            'origin_url': 'https://pypi.org/project/limnoria/'}])

    def test_svn(self):
        self.indexer.run(
                ['http://0-512-md.googlecode.com/svn/'])
        self.assertEqual(self.indexer.results, [{
            'revision_id': b'\xe4?r\xe1,\x88\xab\xec\xe7\x9a\x87\xb8'
                           b'\xc9\xad#.\x1bw=\x18',
            'origin_url': 'http://0-512-md.googlecode.com/svn/'}])
