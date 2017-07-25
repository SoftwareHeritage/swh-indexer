
# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.objstorage.exc import ObjNotFoundError


class MockStorageWrongConfiguration():
    def indexer_configuration_get(self, tool):
        return None


class MockObjStorage():
    """Mock objstorage with predefined contents.

    """
    def __init__(self):
        self.data = {
            '01c9379dfc33803963d07c1ccc748d3fe4c96bb50': b'this is some text',
            '688a5ef812c53907562fe379d4b3851e69c7cb15': b'another text',
            '8986af901dd2043044ce8f0d8fc039153641cf17': b'yet another text',
            '02fb2c89e14f7fab46701478c83779c7beb7b069': b"""
            import unittest
            import logging
            from nose.tools import istest
            from swh.indexer.mimetype import ContentMimetypeIndexer
            from swh.indexer.tests.test_utils import MockObjStorage

            class MockStorage():
                def content_mimetype_add(self, mimetypes):
                    self.state = mimetypes
                    self.conflict_update = conflict_update

                def indexer_configuration_get(self, tool):
                    return {
                        'id': 10,
                    }
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
            """
        }

    def get(self, sha1):
        raw_content = self.data.get(sha1)
        if not raw_content:
            raise ObjNotFoundError()
        return raw_content


class MockStorage():
    """Mock storage to simplify reading indexers' outputs.
    """
    def content_metadata_add(self, metadata, conflict_update=None):
        self.state = metadata
        self.conflict_update = conflict_update

    def revision_metadata_add(self, metadata, conflict_update=None):
        self.state = metadata
        self.conflict_update = conflict_update

    def indexer_configuration_get(self, tool):
        if tool['tool_name'] == 'swh-metadata-translator':
            return {
                'id': 30,
                'tool_name': 'swh-metadata-translator',
                'tool_version': '0.0.1',
                'tool_configuration': {
                    'type': 'local',
                    'context': 'npm'
                },
            }
        elif tool['tool_name'] == 'swh-metadata-detector':
            return {
                'id': 7,
                'tool_name': 'swh-metadata-detector',
                'tool_version': '0.0.1',
                'tool_configuration': {
                    'type': 'local',
                    'context': 'npm'
                },
            }

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

    def content_metadata_get(self, sha1s):
        return [{
            'tool': {
                'configuration': {
                    'type': 'local',
                    'context': 'npm'
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
