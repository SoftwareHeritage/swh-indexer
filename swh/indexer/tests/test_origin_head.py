# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest
import logging
from nose.tools import istest

from swh.indexer.origin_head import OriginHeadIndexer
from swh.indexer.tests.test_utils import MockIndexerStorage

ORIGINS = [
        {
            'id': 52189575,
            'lister': None,
            'project': None,
            'type': 'git',
            'url': 'https://github.com/SoftwareHeritage/swh-storage'},
        {
            'id': 4423668,
            'lister': None,
            'project': None,
            'type': 'ftp',
            'url': 'rsync://ftp.gnu.org/gnu/3dldf'},
        {
            'id': 77775770,
            'lister': None,
            'project': None,
            'type': 'deposit',
            'url': 'https://forge.softwareheritage.org/source/jesuisgpl/'},
        {
            'id': 85072327,
            'lister': None,
            'project': None,
            'type': 'pypi',
            'url': 'https://pypi.org/project/limnoria/'},
        {
            'id': 49908349,
            'lister': None,
            'project': None,
            'type': 'svn',
            'url': 'http://0-512-md.googlecode.com/svn/'},
        ]

SNAPSHOTS = {
        52189575: {
            'branches': {
                b'refs/heads/add-revision-origin-cache': {
                    'target': b'L[\xce\x1c\x88\x8eF\t\xf1"\x19\x1e\xfb\xc0'
                              b's\xe7/\xe9l\x1e',
                    'target_type': 'revision'},
                b'HEAD': {
                    'target': b'8K\x12\x00d\x03\xcc\xe4]bS\xe3\x8f{\xd7}'
                              b'\xac\xefrm',
                    'target_type': 'revision'},
                b'refs/tags/v0.0.103': {
                    'target': b'\xb6"Im{\xfdLb\xb0\x94N\xea\x96m\x13x\x88+'
                              b'\x0f\xdd',
                    'target_type': 'release'},
                }},
        4423668: {
            'branches': {
                b'3DLDF-1.1.4.tar.gz': {
                    'target': b'dJ\xfb\x1c\x91\xf4\x82B%]6\xa2\x90|\xd3\xfc'
                              b'"G\x99\x11',
                    'target_type': 'revision'},
                b'3DLDF-2.0.2.tar.gz': {
                    'target': b'\xb6\x0e\xe7\x9e9\xac\xaa\x19\x9e='
                              b'\xd1\xc5\x00\\\xc6\xfc\xe0\xa6\xb4V',
                    'target_type': 'revision'},
                b'3DLDF-2.0.3-examples.tar.gz': {
                    'target': b'!H\x19\xc0\xee\x82-\x12F1\xbd\x97'
                              b'\xfe\xadZ\x80\x80\xc1\x83\xff',
                    'target_type': 'revision'},
                b'3DLDF-2.0.3.tar.gz': {
                    'target': b'\x8e\xa9\x8e/\xea}\x9feF\xf4\x9f\xfd\xee'
                              b'\xcc\x1a\xb4`\x8c\x8by',
                    'target_type': 'revision'},
                b'3DLDF-2.0.tar.gz': {
                    'target': b'F6*\xff(?\x19a\xef\xb6\xc2\x1fv$S\xe3G'
                              b'\xd3\xd1m',
                    b'target_type': 'revision'}
                }},
        77775770: {
            'branches': {
                b'master': {
                    'target': b'\xe7n\xa4\x9c\x9f\xfb\xb7\xf76\x11\x08{'
                              b'\xa6\xe9\x99\xb1\x9e]q\xeb',
                    'target_type': 'revision'}
            },
            'id': b"h\xc0\xd2a\x04\xd4~'\x8d\xd6\xbe\x07\xeda\xfa\xfbV"
                  b"\x1d\r "},
        85072327: {
            'branches': {
                b'HEAD': {
                    'target': b'releases/2018.09.09',
                    'target_type': 'alias'},
                b'releases/2018.09.01': {
                    'target': b'<\xee1(\xe8\x8d_\xc1\xc9\xa6rT\xf1\x1d'
                              b'\xbb\xdfF\xfdw\xcf',
                    'target_type': 'revision'},
                b'releases/2018.09.09': {
                    'target': b'\x83\xb9\xb6\xc7\x05\xb1%\xd0\xfem\xd8k'
                              b'A\x10\x9d\xc5\xfa2\xf8t',
                    'target_type': 'revision'}},
            'id': b'{\xda\x8e\x84\x7fX\xff\x92\x80^\x93V\x18\xa3\xfay'
                  b'\x12\x9e\xd6\xb3'},
        49908349: {
                'branches': {
                    b'master': {
                        'target': b'\xe4?r\xe1,\x88\xab\xec\xe7\x9a\x87\xb8'
                                  b'\xc9\xad#.\x1bw=\x18',
                        'target_type': 'revision'}},
                'id': b'\xa1\xa2\x8c\n\xb3\x87\xa8\xf9\xe0a\x8c\xb7'
                      b'\x05\xea\xb8\x1f\xc4H\xf4s'},
        }


class MockStorage:
    def origin_get(self, id_):
        for origin in ORIGINS:
            if origin['type'] == id_['type'] and origin['url'] == id_['url']:
                return origin
        assert False, id_

    def snapshot_get_latest(self, origin_id):
        if origin_id in SNAPSHOTS:
            return SNAPSHOTS[origin_id]
        else:
            assert False, origin_id


class TestOriginHeadIndexer(OriginHeadIndexer):
    """Specific indexer whose configuration is enough to satisfy the
       indexing tests.
    """
    def prepare(self):
        self.config = {
            'tools': {
                'name': 'origin-metadata',
                'version': '0.0.1',
                'configuration': {},
            },
        }
        self.storage = MockStorage()
        self.idx_storage = MockIndexerStorage()
        self.log = logging.getLogger('swh.indexer')
        self.objstorage = None
        self.tools = self.register_tools(self.config['tools'])
        self.tool = self.tools[0]
        self.results = None

    def persist_index_computations(self, results, policy_update):
        self.results = results


class OriginHead(unittest.TestCase):
    @istest
    def test_git(self):
        indexer = TestOriginHeadIndexer()
        indexer.run(
                ['git+https://github.com/SoftwareHeritage/swh-storage'],
                'update-dups', parse_ids=True)
        self.assertEqual(indexer.results, [{
            'revision_id': b'8K\x12\x00d\x03\xcc\xe4]bS\xe3\x8f{'
                           b'\xd7}\xac\xefrm',
            'origin_id': 52189575}])

    @istest
    def test_ftp(self):
        indexer = TestOriginHeadIndexer()
        indexer.run(
                ['ftp+rsync://ftp.gnu.org/gnu/3dldf'],
                'update-dups', parse_ids=True)
        self.assertEqual(indexer.results, [{
            'revision_id': b'\x8e\xa9\x8e/\xea}\x9feF\xf4\x9f\xfd\xee'
                           b'\xcc\x1a\xb4`\x8c\x8by',
            'origin_id': 4423668}])

    @istest
    def test_deposit(self):
        indexer = TestOriginHeadIndexer()
        indexer.run(
                ['deposit+https://forge.softwareheritage.org/source/'
                 'jesuisgpl/'],
                'update-dups', parse_ids=True)
        self.assertEqual(indexer.results, [{
            'revision_id': b'\xe7n\xa4\x9c\x9f\xfb\xb7\xf76\x11\x08{'
                           b'\xa6\xe9\x99\xb1\x9e]q\xeb',
            'origin_id': 77775770}])

    @istest
    def test_pypi(self):
        indexer = TestOriginHeadIndexer()
        indexer.run(
                ['pypi+https://pypi.org/project/limnoria/'],
                'update-dups', parse_ids=True)
        self.assertEqual(indexer.results, [{
            'revision_id': b'\x83\xb9\xb6\xc7\x05\xb1%\xd0\xfem\xd8k'
                           b'A\x10\x9d\xc5\xfa2\xf8t',
            'origin_id': 85072327}])

    @istest
    def test_svn(self):
        indexer = TestOriginHeadIndexer()
        indexer.run(
                ['svn+http://0-512-md.googlecode.com/svn/'],
                'update-dups', parse_ids=True)
        self.assertEqual(indexer.results, [{
            'revision_id': b'\xe4?r\xe1,\x88\xab\xec\xe7\x9a\x87\xb8'
                           b'\xc9\xad#.\x1bw=\x18',
            'origin_id': 49908349}])
