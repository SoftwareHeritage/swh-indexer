# Copyright (C) 2017-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import abc
import datetime
import functools
import random
import unittest

from hypothesis import strategies

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
            'type': 'git',
            'url': 'https://github.com/SoftwareHeritage/swh-storage'},
        {
            'type': 'ftp',
            'url': 'rsync://ftp.gnu.org/gnu/3dldf'},
        {
            'type': 'deposit',
            'url': 'https://forge.softwareheritage.org/source/jesuisgpl/'},
        {
            'type': 'pypi',
            'url': 'https://pypi.org/project/limnoria/'},
        {
            'type': 'svn',
            'url': 'http://0-512-md.googlecode.com/svn/'},
        {
            'type': 'git',
            'url': 'https://github.com/librariesio/yarn-parser'},
        {
            'type': 'git',
            'url': 'https://github.com/librariesio/yarn-parser.git'},
        ]

SNAPSHOTS = [
    {
        'origin': 'https://github.com/SoftwareHeritage/swh-storage',
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
    {
        'origin': 'rsync://ftp.gnu.org/gnu/3dldf',
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
                'target_type': 'revision'}
            }},
    {
        'origin': 'https://forge.softwareheritage.org/source/jesuisgpl/',
        'branches': {
            b'master': {
                'target': b'\xe7n\xa4\x9c\x9f\xfb\xb7\xf76\x11\x08{'
                          b'\xa6\xe9\x99\xb1\x9e]q\xeb',
                'target_type': 'revision'}
        },
        'id': b"h\xc0\xd2a\x04\xd4~'\x8d\xd6\xbe\x07\xeda\xfa\xfbV"
              b"\x1d\r "},
    {
        'origin': 'https://pypi.org/project/limnoria/',
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
    {
        'origin': 'http://0-512-md.googlecode.com/svn/',
        'branches': {
            b'master': {
                'target': b'\xe4?r\xe1,\x88\xab\xec\xe7\x9a\x87\xb8'
                          b'\xc9\xad#.\x1bw=\x18',
                'target_type': 'revision'}},
        'id': b'\xa1\xa2\x8c\n\xb3\x87\xa8\xf9\xe0a\x8c\xb7'
              b'\x05\xea\xb8\x1f\xc4H\xf4s'},
    {
        'origin': 'https://github.com/librariesio/yarn-parser',
        'branches': {
            b'HEAD': {
                'target': hash_to_bytes(
                    '8dbb6aeb036e7fd80664eb8bfd1507881af1ba9f'),
                'target_type': 'revision'}}},
    {
        'origin': 'https://github.com/librariesio/yarn-parser.git',
        'branches': {
            b'HEAD': {
                'target': hash_to_bytes(
                    '8dbb6aeb036e7fd80664eb8bfd1507881af1ba9f'),
                'target_type': 'revision'}}},
]


REVISIONS = [{
    'id': hash_to_bytes('8dbb6aeb036e7fd80664eb8bfd1507881af1ba9f'),
    'message': 'Improve search functionality',
    'author': {
        'name': b'Andrew Nesbitt',
        'fullname': b'Andrew Nesbitt <andrewnez@gmail.com>',
        'email': b'andrewnez@gmail.com'
    },
    'committer': {
        'name': b'Andrew Nesbitt',
        'fullname': b'Andrew Nesbitt <andrewnez@gmail.com>',
        'email': b'andrewnez@gmail.com'
    },
    'committer_date': {
        'negative_utc': None,
        'offset': 120,
        'timestamp': {
            'microseconds': 0,
            'seconds': 1380883849
        }
    },
    'type': 'git',
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

DIRECTORY_ENTRIES = [{
    'name': b'index.js',
    'type': 'file',
    'target': b'abc',
    'perms': 33188,
    },
    {
    'name': b'package.json',
    'type': 'file',
    'target': b'cde',
    'perms': 33188,
    },
    {
    'name': b'.github',
    'type': 'dir',
    'target': b'11',
    'perms': 16384,
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
    # 626364
    hash_to_hex(b'bcd'): b'unimportant content for bcd',
    # 636465
    hash_to_hex(b'cde'): b"""
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

YARN_PARSER_METADATA = {
    '@context': 'https://doi.org/10.5063/schema/codemeta-2.0',
    'url':
        'https://github.com/librariesio/yarn-parser#readme',
    'codeRepository':
        'git+git+https://github.com/librariesio/yarn-parser.git',
    'author': [{
        'type': 'Person',
        'name': 'Andrew Nesbitt'
    }],
    'license': 'https://spdx.org/licenses/AGPL-3.0',
    'version': '1.0.0',
    'description':
        'Tiny web service for parsing yarn.lock files',
    'issueTracker':
        'https://github.com/librariesio/yarn-parser/issues',
    'name': 'yarn-parser',
    'keywords': ['yarn', 'parse', 'lock', 'dependencies'],
    'type': 'SoftwareSourceCode',
}


json_dict_keys = strategies.one_of(
    strategies.characters(),
    *map(strategies.just, ['type', 'url', 'name', 'email', '@id',
                           '@context', 'repository', 'license',
                           'repositories', 'licenses'
                           ]),
)
"""Hypothesis strategy that generates strings, with an emphasis on those
that are often used as dictionary keys in metadata files."""


generic_json_document = strategies.recursive(
    strategies.none() | strategies.booleans() | strategies.floats() |
    strategies.characters(),
    lambda children: (
        strategies.lists(children, 1) |
        strategies.dictionaries(json_dict_keys, children, min_size=1)
    )
)
"""Hypothesis strategy that generates possible values for values of JSON
metadata files."""


def json_document_strategy(keys=None):
    """Generates an hypothesis strategy that generates metadata files
    for a JSON-based format that uses the given keys."""
    if keys is None:
        keys = strategies.characters()
    else:
        keys = strategies.one_of(map(strategies.just, keys))

    return strategies.dictionaries(keys, generic_json_document, min_size=1)


def _tree_to_xml(root, xmlns, data):
    def encode(s):
        "Skips unpaired surrogates generated by json_document_strategy"
        return s.encode('utf8', 'replace')

    def to_xml(data, indent=b' '):
        if data is None:
            return b''
        elif isinstance(data, (bool, str, int, float)):
            return indent + encode(str(data))
        elif isinstance(data, list):
            return b'\n'.join(to_xml(v, indent=indent) for v in data)
        elif isinstance(data, dict):
            lines = []
            for (key, value) in data.items():
                lines.append(indent + encode('<{}>'.format(key)))
                lines.append(to_xml(value, indent=indent+b' '))
                lines.append(indent + encode('</{}>'.format(key)))
            return b'\n'.join(lines)
        else:
            raise TypeError(data)

    return b'\n'.join([
        '<{} xmlns="{}">'.format(root, xmlns).encode(),
        to_xml(data),
        '</{}>'.format(root).encode(),
    ])


class TreeToXmlTest(unittest.TestCase):
    def test_leaves(self):
        self.assertEqual(
            _tree_to_xml('root', 'http://example.com', None),
            b'<root xmlns="http://example.com">\n\n</root>'
        )
        self.assertEqual(
            _tree_to_xml('root', 'http://example.com', True),
            b'<root xmlns="http://example.com">\n True\n</root>'
        )
        self.assertEqual(
            _tree_to_xml('root', 'http://example.com', 'abc'),
            b'<root xmlns="http://example.com">\n abc\n</root>'
        )
        self.assertEqual(
            _tree_to_xml('root', 'http://example.com', 42),
            b'<root xmlns="http://example.com">\n 42\n</root>'
        )
        self.assertEqual(
            _tree_to_xml('root', 'http://example.com', 3.14),
            b'<root xmlns="http://example.com">\n 3.14\n</root>'
        )

    def test_dict(self):
        self.assertIn(
            _tree_to_xml('root', 'http://example.com', {
                'foo': 'bar',
                'baz': 'qux'
            }),
            [
                b'<root xmlns="http://example.com">\n'
                b' <foo>\n  bar\n </foo>\n'
                b' <baz>\n  qux\n </baz>\n'
                b'</root>',
                b'<root xmlns="http://example.com">\n'
                b' <baz>\n  qux\n </baz>\n'
                b' <foo>\n  bar\n </foo>\n'
                b'</root>'
            ]
        )

    def test_list(self):
        self.assertEqual(
            _tree_to_xml('root', 'http://example.com', [
                {'foo': 'bar'},
                {'foo': 'baz'},
            ]),
            b'<root xmlns="http://example.com">\n'
            b' <foo>\n  bar\n </foo>\n'
            b' <foo>\n  baz\n </foo>\n'
            b'</root>'
        )


def xml_document_strategy(keys, root, xmlns):
    """Generates an hypothesis strategy that generates metadata files
    for an XML format that uses the given keys."""

    return strategies.builds(
        functools.partial(_tree_to_xml, root, xmlns),
        json_document_strategy(keys))


def filter_dict(d, keys):
    'return a copy of the dict with keys deleted'
    if not isinstance(keys, (list, tuple)):
        keys = (keys, )
    return dict((k, v) for (k, v) in d.items() if k not in keys)


def fill_obj_storage(obj_storage):
    """Add some content in an object storage."""
    for (obj_id, content) in OBJ_STORAGE_DATA.items():
        obj_storage.add(content, obj_id=hash_to_bytes(obj_id))


def fill_storage(storage):
    for origin in ORIGINS:
        storage.origin_add_one(origin)
    for snap in SNAPSHOTS:
        origin_url = snap['origin']
        visit = storage.origin_visit_add(origin_url, datetime.datetime.now())
        snap_id = snap.get('id') or \
            bytes([random.randint(0, 255) for _ in range(32)])
        storage.snapshot_add([{
            'id': snap_id,
            'branches': snap['branches']
        }])
        storage.origin_visit_update(
            origin_url, visit['visit'], status='full', snapshot=snap_id)
    storage.revision_add(REVISIONS)

    contents = []
    for (obj_id, content) in OBJ_STORAGE_DATA.items():
        content_hashes = hashutil.MultiHash.from_data(content).digest()
        contents.append({
            'data': content,
            'length': len(content),
            'status': 'visible',
            'sha1': hash_to_bytes(obj_id),
            'sha1_git': hash_to_bytes(obj_id),
            'sha256': content_hashes['sha256'],
            'blake2s256': content_hashes['blake2s256']
        })
    storage.content_add(contents)
    storage.directory_add([{
        'id': DIRECTORY_ID,
        'entries': DIRECTORY_ENTRIES,
    }])


class CommonContentIndexerTest(metaclass=abc.ABCMeta):
    legacy_get_format = False
    """True if and only if the tested indexer uses the legacy format.
    see: https://forge.softwareheritage.org/T1433

    """
    def get_indexer_results(self, ids):
        """Override this for indexers that don't have a mock storage."""
        return self.indexer.idx_storage.state

    def assert_legacy_results_ok(self, sha1s, expected_results=None):
        # XXX old format, remove this when all endpoints are
        #     updated to the new one
        #     see: https://forge.softwareheritage.org/T1433
        sha1s = [sha1 if isinstance(sha1, bytes) else hash_to_bytes(sha1)
                 for sha1 in sha1s]
        actual_results = list(self.get_indexer_results(sha1s))

        if expected_results is None:
            expected_results = self.expected_results

        self.assertEqual(len(expected_results), len(actual_results),
                         (expected_results, actual_results))
        for indexed_data in actual_results:
            _id = indexed_data['id']
            expected_data = expected_results[hashutil.hash_to_hex(_id)].copy()
            expected_data['id'] = _id
            self.assertEqual(indexed_data, expected_data)

    def assert_results_ok(self, sha1s, expected_results=None):
        if self.legacy_get_format:
            self.assert_legacy_results_ok(sha1s, expected_results)
            return

        sha1s = [sha1 if isinstance(sha1, bytes) else hash_to_bytes(sha1)
                 for sha1 in sha1s]
        actual_results = list(self.get_indexer_results(sha1s))

        if expected_results is None:
            expected_results = self.expected_results

        self.assertEqual(len(expected_results), len(actual_results),
                         (expected_results, actual_results))
        for indexed_data in actual_results:
            (_id, indexed_data) = list(indexed_data.items())[0]
            expected_data = expected_results[hashutil.hash_to_hex(_id)].copy()
            expected_data = [expected_data]
            self.assertEqual(indexed_data, expected_data)

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
