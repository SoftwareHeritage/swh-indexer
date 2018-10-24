# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from swh.objstorage.exc import ObjNotFoundError

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
                        'target': b'8dbb6aeb036e7fd80664eb8bfd1507881af1ba9f',
                        'target_type': 'revision'}}}
        }


class MockObjStorage:
    """Mock an swh-objstorage objstorage with predefined contents.

    """
    data = {}

    def __init__(self):
        self.data = {
            '01c9379dfc33803963d07c1ccc748d3fe4c96bb5': b'this is some text',
            '688a5ef812c53907562fe379d4b3851e69c7cb15': b'another text',
            '8986af901dd2043044ce8f0d8fc039153641cf17': b'yet another text',
            '02fb2c89e14f7fab46701478c83779c7beb7b069': b"""
            import unittest
            import logging
            from swh.indexer.mimetype import ContentMimetypeIndexer
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
        }

    def __iter__(self):
        yield from self.data.keys()

    def __contains__(self, sha1):
        return self.data.get(sha1) is not None

    def get(self, sha1):
        raw_content = self.data.get(sha1)
        if raw_content is None:
            raise ObjNotFoundError(sha1)
        return raw_content


class MockIndexerStorage():
    """Mock an swh-indexer storage.

    """
    added_data = []

    def indexer_configuration_add(self, tools):
        tool = tools[0]
        if tool['tool_name'] == 'swh-metadata-translator':
            return [{
                'id': 30,
                'tool_name': 'swh-metadata-translator',
                'tool_version': '0.0.1',
                'tool_configuration': {
                    'type': 'local',
                    'context': 'NpmMapping'
                },
            }]
        elif tool['tool_name'] == 'swh-metadata-detector':
            return [{
                'id': 7,
                'tool_name': 'swh-metadata-detector',
                'tool_version': '0.0.1',
                'tool_configuration': {
                    'type': 'local',
                    'context': 'NpmMapping'
                },
            }]
        elif tool['tool_name'] == 'origin-metadata':
            return [{
                'id': 8,
                'tool_name': 'origin-metadata',
                'tool_version': '0.0.1',
                'tool_configuration': {},
            }]
        else:
            assert False, 'Unknown tool {tool_name}'.format(**tool)

    def content_metadata_missing(self, sha1s):
        yield from []

    def content_metadata_add(self, metadata, conflict_update=None):
        self.added_data.append(
                ('content_metadata', conflict_update, metadata))

    def revision_metadata_add(self, metadata, conflict_update=None):
        self.added_data.append(
                ('revision_metadata', conflict_update, metadata))

    def origin_intrinsic_metadata_add(self, metadata, conflict_update=None):
        self.added_data.append(
                ('origin_intrinsic_metadata', conflict_update, metadata))

    def content_metadata_get(self, sha1s):
        return [{
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
                'issueTracker': {
                    'url': 'https://github.com/librariesio/yarn-parser/issues'
                },
                'version': '1.0.0',
                'name': 'yarn-parser',
                'author': 'Andrew Nesbitt',
                'url': 'https://github.com/librariesio/yarn-parser#readme',
                'processorRequirements': {'node': '7.5'},
                'other': {
                    'scripts': {
                                    'start': 'node index.js'
                    },
                    'main': 'index.js'
                },
                'license': 'AGPL-3.0',
                'keywords': ['yarn', 'parse', 'lock', 'dependencies'],
                'codeRepository': {
                    'type': 'git',
                    'url': 'git+https://github.com/librariesio/yarn-parser.git'
                },
                'description': 'Tiny web service for parsing yarn.lock files',
                'softwareRequirements': {
                    'yarn': '^0.21.0',
                    'express': '^4.14.0',
                    'body-parser': '^1.15.2'}
                }
        }]


class MockStorage():
    """Mock a real swh-storage storage to simplify reading indexers'
    outputs.

    """
    def origin_get(self, id_):
        for origin in ORIGINS:
            for (k, v) in id_.items():
                if origin[k] != v:
                    break
            else:
                # This block is run iff we didn't break, ie. if all supplied
                # parts of the id are set to the expected value.
                return origin
        assert False, id_

    def snapshot_get_latest(self, origin_id):
        if origin_id in SNAPSHOTS:
            return SNAPSHOTS[origin_id]
        else:
            assert False, origin_id

    def revision_get(self, revisions):
        return [{
            'id': b'8dbb6aeb036e7fd80664eb8bfd1507881af1ba9f',
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

    def directory_ls(self, directory, recursive=False, cur=None):
        # with directory: b'\x9d',
        return [{
                'sha1_git': b'abc',
                'name': b'index.js',
                'target': b'abc',
                'length': 897,
                'status': 'visible',
                'type': 'file',
                'perms': 33188,
                'dir_id': b'10',
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
                'dir_id': b'10',
                'sha1': b'cde'
                },
                {
                'dir_id': b'10',
                'target': b'11',
                'type': 'dir',
                'length': None,
                'name': b'.github',
                'sha1': None,
                'perms': 16384,
                'sha1_git': None,
                'status': None,
                'sha256': None
                }]
