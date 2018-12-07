# Copyright (C) 2017-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import hashlib
import random

from swh.model import hashutil
from swh.model.hashutil import hash_to_bytes, hash_to_hex

from swh.indexer.storage import INDEXER_CFG_KEY

BASE_TEST_CONFIG = {
    'storage': {
        'cls': 'memory',
        'args': {
        },
    },
    'objstorage': {
        'cls': 'memory',
        'args': {
        },
    },
    INDEXER_CFG_KEY: {
        'cls': 'memory',
        'args': {
        },
    },
}

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
        {
            'id': 54974445,
            'lister': None,
            'project': None,
            'type': 'git',
            'url': 'https://github.com/librariesio/yarn-parser'},
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
        54974445: {
                'branches': {
                    b'HEAD': {
                        'target': hash_to_bytes(
                            '8dbb6aeb036e7fd80664eb8bfd1507881af1ba9f'),
                        'target_type': 'revision'}}}
        }


REVISIONS = [{
    'id': hash_to_bytes('8dbb6aeb036e7fd80664eb8bfd1507881af1ba9f'),
    'committer': {
        'id': 26,
        'name': b'Andrew Nesbitt',
        'fullname': b'Andrew Nesbitt <andrewnez@gmail.com>',
        'email': b'andrewnez@gmail.com'
    },
    'synthetic': False,
    'date': {
        'negative_utc': False,
        'timestamp': {
            'seconds': 1487596456,
            'microseconds': 0
        },
        'offset': 0
    },
    'directory': b'10'
}]

DIRECTORY_ID = b'10'

DIRECTORY = [{
    'sha1_git': b'abc',
    'name': b'index.js',
    'target': b'abc',
    'length': 897,
    'status': 'visible',
    'type': 'file',
    'perms': 33188,
    'sha1': b'bcd'
    },
    {
    'sha1_git': b'aab',
    'name': b'package.json',
    'target': b'aab',
    'length': 712,
    'status': 'visible',
    'type': 'file',
    'perms': 33188,
    'sha1': b'cde'
    },
    {
    'target': b'11',
    'type': 'dir',
    'length': None,
    'name': b'.github',
    'sha1': None,
    'perms': 16384,
    'sha1_git': None,
    'status': None,
    'sha256': None
    }
]

SHA1_TO_LICENSES = {
    '01c9379dfc33803963d07c1ccc748d3fe4c96bb5': ['GPL'],
    '02fb2c89e14f7fab46701478c83779c7beb7b069': ['Apache2.0'],
    '103bc087db1d26afc3a0283f38663d081e9b01e6': ['MIT'],
    '688a5ef812c53907562fe379d4b3851e69c7cb15': ['AGPL'],
    'da39a3ee5e6b4b0d3255bfef95601890afd80709': [],
}


SHA1_TO_CTAGS = {
    '01c9379dfc33803963d07c1ccc748d3fe4c96bb5': [{
        'name': 'foo',
        'kind': 'str',
        'line': 10,
        'lang': 'bar',
    }],
    'd4c647f0fc257591cc9ba1722484229780d1c607': [{
        'name': 'let',
        'kind': 'int',
        'line': 100,
        'lang': 'haskell',
    }],
    '688a5ef812c53907562fe379d4b3851e69c7cb15': [{
        'name': 'symbol',
        'kind': 'float',
        'line': 99,
        'lang': 'python',
    }],
}


OBJ_STORAGE_DATA = {
    '01c9379dfc33803963d07c1ccc748d3fe4c96bb5': b'this is some text',
    '688a5ef812c53907562fe379d4b3851e69c7cb15': b'another text',
    '8986af901dd2043044ce8f0d8fc039153641cf17': b'yet another text',
    '02fb2c89e14f7fab46701478c83779c7beb7b069': b"""
    import unittest
    import logging
    from swh.indexer.mimetype import MimetypeIndexer
    from swh.indexer.tests.test_utils import MockObjStorage

    class MockStorage():
        def content_mimetype_add(self, mimetypes):
            self.state = mimetypes
            self.conflict_update = conflict_update

        def indexer_configuration_add(self, tools):
            return [{
                'id': 10,
            }]
    """,
    '103bc087db1d26afc3a0283f38663d081e9b01e6': b"""
        #ifndef __AVL__
        #define __AVL__

        typedef struct _avl_tree avl_tree;

        typedef struct _data_t {
          int content;
        } data_t;
    """,
    '93666f74f1cf635c8c8ac118879da6ec5623c410': b"""
    (should 'pygments (recognize 'lisp 'easily))

    """,
    '26a9f72a7c87cc9205725cfd879f514ff4f3d8d5': b"""
    {
        "name": "test_metadata",
        "version": "0.0.1",
        "description": "Simple package.json test for indexer",
        "repository": {
          "type": "git",
          "url": "https://github.com/moranegg/metadata_test"
      }
    }
    """,
    'd4c647f0fc257591cc9ba1722484229780d1c607': b"""
    {
      "version": "5.0.3",
      "name": "npm",
      "description": "a package manager for JavaScript",
      "keywords": [
        "install",
        "modules",
        "package manager",
        "package.json"
      ],
      "preferGlobal": true,
      "config": {
        "publishtest": false
      },
      "homepage": "https://docs.npmjs.com/",
      "author": "Isaac Z. Schlueter <i@izs.me> (http://blog.izs.me)",
      "repository": {
        "type": "git",
        "url": "https://github.com/npm/npm"
      },
      "bugs": {
        "url": "https://github.com/npm/npm/issues"
      },
      "dependencies": {
        "JSONStream": "~1.3.1",
        "abbrev": "~1.1.0",
        "ansi-regex": "~2.1.1",
        "ansicolors": "~0.3.2",
        "ansistyles": "~0.1.3"
      },
      "devDependencies": {
        "tacks": "~1.2.6",
        "tap": "~10.3.2"
      },
      "license": "Artistic-2.0"
    }

    """,
    'a7ab314d8a11d2c93e3dcf528ca294e7b431c449': b"""
    """,
    'da39a3ee5e6b4b0d3255bfef95601890afd80709': b'',
    '636465': b"""
    {
      "name": "yarn-parser",
      "version": "1.0.0",
      "description": "Tiny web service for parsing yarn.lock files",
      "main": "index.js",
      "scripts": {
        "start": "node index.js",
        "test": "mocha"
      },
      "engines": {
        "node": "9.8.0"
      },
      "repository": {
        "type": "git",
        "url": "git+https://github.com/librariesio/yarn-parser.git"
      },
      "keywords": [
        "yarn",
        "parse",
        "lock",
        "dependencies"
      ],
      "author": "Andrew Nesbitt",
      "license": "AGPL-3.0",
      "bugs": {
        "url": "https://github.com/librariesio/yarn-parser/issues"
      },
      "homepage": "https://github.com/librariesio/yarn-parser#readme",
      "dependencies": {
        "@yarnpkg/lockfile": "^1.0.0",
        "body-parser": "^1.15.2",
        "express": "^4.14.0"
      },
      "devDependencies": {
        "chai": "^4.1.2",
        "mocha": "^5.2.0",
        "request": "^2.87.0",
        "test": "^0.6.0"
      }
    }
"""
}

CONTENT_METADATA = [{
    'tool': {
        'configuration': {
            'type': 'local',
            'context': 'NpmMapping'
            },
        'version': '0.0.1',
        'id': 6,
        'name': 'swh-metadata-translator'
    },
    'id': b'cde',
    'translated_metadata': {
        '@context': 'https://doi.org/10.5063/schema/codemeta-2.0',
        'type': 'SoftwareSourceCode',
        'codemeta:issueTracker':
            'https://github.com/librariesio/yarn-parser/issues',
        'version': '1.0.0',
        'name': 'yarn-parser',
        'schema:author': 'Andrew Nesbitt',
        'url':
            'https://github.com/librariesio/yarn-parser#readme',
        'processorRequirements': {'node': '7.5'},
        'license': 'AGPL-3.0',
        'keywords': ['yarn', 'parse', 'lock', 'dependencies'],
        'schema:codeRepository':
            'git+https://github.com/librariesio/yarn-parser.git',
        'description':
            'Tiny web service for parsing yarn.lock files',
        }
}]


def fill_obj_storage(obj_storage):
    """Add some content in an object storage."""
    for (obj_id, content) in OBJ_STORAGE_DATA.items():
        obj_storage.add(content, obj_id=hash_to_bytes(obj_id))


def fill_storage(storage):
    for origin in ORIGINS:
        origin = origin.copy()
        del origin['id']
        storage.origin_add_one(origin)
    for (orig_pseudo_id, snap) in SNAPSHOTS.items():
        for orig in ORIGINS:
            if orig_pseudo_id == orig['id']:
                origin_id = storage.origin_get(
                    {'type': orig['type'], 'url': orig['url']})['id']
                break
        else:
            assert False
        visit = storage.origin_visit_add(origin_id, datetime.datetime.now())
        snap_id = snap.get('id') or \
            bytes([random.randint(0, 255) for _ in range(32)])
        storage.snapshot_add(origin_id, visit['visit'], {
            'id': snap_id,
            'branches': snap['branches']
        })
    storage.revision_add(REVISIONS)
    storage.directory_add([{
        'id': DIRECTORY_ID,
        'entries': DIRECTORY,
    }])
    for (obj_id, content) in OBJ_STORAGE_DATA.items():
        # TODO: use MultiHash
        if hasattr(hashlib, 'blake2s'):
            blake2s256 = hashlib.blake2s(content, digest_size=32).digest()
        else:
            # fallback for Python <3.6
            blake2s256 = bytes([random.randint(0, 255) for _ in range(32)])
        storage.content_add([{
            'data': content,
            'length': len(content),
            'status': 'visible',
            'sha1': hash_to_bytes(obj_id),
            'sha1_git': hash_to_bytes(obj_id),
            'sha256': hashlib.sha256(content).digest(),
            'blake2s256': blake2s256
        }])


class CommonIndexerNoTool:
    """Mixin to wronly initialize content indexer"""
    def prepare(self):
        super().prepare()
        self.tools = None


class CommonIndexerWithErrorsTest:
    """Test indexer configuration checks.

    """
    Indexer = None
    RangeIndexer = None

    def test_wrong_unknown_configuration_tool(self):
        """Indexer with unknown configuration tool fails check"""
        with self.assertRaisesRegex(ValueError, 'Tools None is unknown'):
            print('indexer: %s' % self.Indexer)
            self.Indexer()

    def test_wrong_unknown_configuration_tool_range(self):
        """Range Indexer with unknown configuration tool fails check"""
        if self.RangeIndexer is not None:
            with self.assertRaisesRegex(ValueError, 'Tools None is unknown'):
                self.RangeIndexer()


class CommonContentIndexerTest:
    def get_indexer_results(self, ids):
        """Override this for indexers that don't have a mock storage."""
        return self.indexer.idx_storage.state

    def assert_results_ok(self, sha1s, expected_results=None):
        sha1s = [sha1 if isinstance(sha1, bytes) else hash_to_bytes(sha1)
                 for sha1 in sha1s]
        actual_results = self.get_indexer_results(sha1s)

        if expected_results is None:
            expected_results = self.expected_results

        for indexed_data in actual_results:
            _id = indexed_data['id']
            self.assertEqual(indexed_data, expected_results[_id])
            _tool_id = indexed_data['indexer_configuration_id']
            self.assertEqual(_tool_id, self.indexer.tool['id'])

    def test_index(self):
        """Known sha1 have their data indexed

        """
        sha1s = [self.id0, self.id1, self.id2]

        # when
        self.indexer.run(sha1s, policy_update='update-dups')

        self.assert_results_ok(sha1s)

        # 2nd pass
        self.indexer.run(sha1s, policy_update='ignore-dups')

        self.assert_results_ok(sha1s)

    def test_index_one_unknown_sha1(self):
        """Unknown sha1 are not indexed"""
        sha1s = [self.id1,
                 '799a5ef812c53907562fe379d4b3851e69c7cb15',  # unknown
                 '800a5ef812c53907562fe379d4b3851e69c7cb15']  # unknown

        # when
        self.indexer.run(sha1s, policy_update='update-dups')

        # then
        expected_results = {
            k: v for k, v in self.expected_results.items() if k in sha1s
        }

        self.assert_results_ok(sha1s, expected_results)


class CommonContentIndexerRangeTest:
    """Allows to factorize tests on range indexer.

    """
    def setUp(self):
        self.contents = sorted(OBJ_STORAGE_DATA)

    def assert_results_ok(self, start, end, actual_results,
                          expected_results=None):
        if expected_results is None:
            expected_results = self.expected_results

        actual_results = list(actual_results)
        for indexed_data in actual_results:
            _id = indexed_data['id']
            assert isinstance(_id, bytes)
            indexed_data = indexed_data.copy()
            indexed_data['id'] = hash_to_hex(indexed_data['id'])
            self.assertEqual(indexed_data, expected_results[hash_to_hex(_id)])
            self.assertTrue(start <= _id <= end)
            _tool_id = indexed_data['indexer_configuration_id']
            self.assertEqual(_tool_id, self.indexer.tool['id'])

    def test__index_contents(self):
        """Indexing contents without existing data results in indexed data

        """
        _start, _end = [self.contents[0], self.contents[2]]  # output hex ids
        start, end = map(hashutil.hash_to_bytes, (_start, _end))
        # given
        actual_results = list(self.indexer._index_contents(
            start, end, indexed={}))

        self.assert_results_ok(start, end, actual_results)

    def test__index_contents_with_indexed_data(self):
        """Indexing contents with existing data results in less indexed data

        """
        _start, _end = [self.contents[0], self.contents[2]]  # output hex ids
        start, end = map(hashutil.hash_to_bytes, (_start, _end))
        data_indexed = [self.id0, self.id2]

        # given
        actual_results = self.indexer._index_contents(
            start, end, indexed=set(map(hash_to_bytes, data_indexed)))

        # craft the expected results
        expected_results = self.expected_results.copy()
        for already_indexed_key in data_indexed:
            expected_results.pop(already_indexed_key)

        self.assert_results_ok(
            start, end, actual_results, expected_results)

    def test_generate_content_get(self):
        """Optimal indexing should result in indexed data

        """
        _start, _end = [self.contents[0], self.contents[2]]  # output hex ids
        start, end = map(hashutil.hash_to_bytes, (_start, _end))

        # given
        actual_results = self.indexer.run(start, end)

        # then
        self.assertTrue(actual_results)

    def test_generate_content_get_input_as_bytes(self):
        """Optimal indexing should result in indexed data

        Input are in bytes here.

        """
        _start, _end = [self.contents[0], self.contents[2]]  # output hex ids
        start, end = map(hashutil.hash_to_bytes, (_start, _end))

        # given
        actual_results = self.indexer.run(  # checks the bytes input this time
            start, end, skip_existing=False)
        # no already indexed data so same result as prior test

        # then
        self.assertTrue(actual_results)

    def test_generate_content_get_no_result(self):
        """No result indexed returns False"""
        _start, _end = ['0000000000000000000000000000000000000000',
                        '0000000000000000000000000000000000000001']
        start, end = map(hashutil.hash_to_bytes, (_start, _end))
        # given
        actual_results = self.indexer.run(
            start, end, incremental=False)

        # then
        self.assertFalse(actual_results)


class NoDiskIndexer:
    """Mixin to override the DiskIndexer behavior avoiding side-effects in
       tests.

    """

    def write_to_temp(self, filename, data):  # noop
        return filename

    def cleanup(self, content_path):  # noop
        return None
