# Copyright (C) 2017-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from swh.indexer.origin_head import OriginHeadIndexer
from swh.indexer.tests.test_utils import (
    BASE_TEST_CONFIG, fill_storage
)


class OriginHeadTestIndexer(OriginHeadIndexer):
    """Specific indexer whose configuration is enough to satisfy the
       indexing tests.
    """
    def parse_config_file(self, *args, **kwargs):
        return {
            **BASE_TEST_CONFIG,
            'tools': {
                'name': 'origin-metadata',
                'version': '0.0.1',
                'configuration': {},
            },
            'tasks': {
                'revision_metadata': None,
                'origin_intrinsic_metadata': None,
            }
        }

    def persist_index_computations(self, results, policy_update):
        self.results = results


class OriginHead(unittest.TestCase):
    def setUp(self):
        self.indexer = OriginHeadTestIndexer()
        fill_storage(self.indexer.storage)

    def _get_origin_id(self, type_, url):
        origin = self.indexer.storage.origin_get({
            'type': type_, 'url': url})
        return origin['id']

    def test_git(self):
        self.indexer.run(
                ['git+https://github.com/SoftwareHeritage/swh-storage'])
        origin_id = self._get_origin_id(
            'git', 'https://github.com/SoftwareHeritage/swh-storage')
        self.assertEqual(self.indexer.results, [{
            'revision_id': b'8K\x12\x00d\x03\xcc\xe4]bS\xe3\x8f{'
                           b'\xd7}\xac\xefrm',
            'origin_id': origin_id}])

    def test_ftp(self):
        self.indexer.run(
                ['ftp+rsync://ftp.gnu.org/gnu/3dldf'])
        origin_id = self._get_origin_id(
                'ftp', 'rsync://ftp.gnu.org/gnu/3dldf')
        self.assertEqual(self.indexer.results, [{
            'revision_id': b'\x8e\xa9\x8e/\xea}\x9feF\xf4\x9f\xfd\xee'
                           b'\xcc\x1a\xb4`\x8c\x8by',
            'origin_id': origin_id}])

    def test_deposit(self):
        self.indexer.run(
                ['deposit+https://forge.softwareheritage.org/source/'
                 'jesuisgpl/'])
        origin_id = self._get_origin_id(
            'deposit', 'https://forge.softwareheritage.org/source/jesuisgpl/')
        self.assertEqual(self.indexer.results, [{
            'revision_id': b'\xe7n\xa4\x9c\x9f\xfb\xb7\xf76\x11\x08{'
                           b'\xa6\xe9\x99\xb1\x9e]q\xeb',
            'origin_id': origin_id}])

    def test_pypi(self):
        self.indexer.run(
                ['pypi+https://pypi.org/project/limnoria/'])
        origin_id = self._get_origin_id(
            'pypi', 'https://pypi.org/project/limnoria/')
        self.assertEqual(self.indexer.results, [{
            'revision_id': b'\x83\xb9\xb6\xc7\x05\xb1%\xd0\xfem\xd8k'
                           b'A\x10\x9d\xc5\xfa2\xf8t',
            'origin_id': origin_id}])

    def test_svn(self):
        self.indexer.run(
                ['svn+http://0-512-md.googlecode.com/svn/'])
        origin_id = self._get_origin_id(
            'svn', 'http://0-512-md.googlecode.com/svn/')
        self.assertEqual(self.indexer.results, [{
            'revision_id': b'\xe4?r\xe1,\x88\xab\xec\xe7\x9a\x87\xb8'
                           b'\xc9\xad#.\x1bw=\x18',
            'origin_id': origin_id}])
