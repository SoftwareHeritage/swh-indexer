
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

            """

        }

    def get(self, sha1):
        raw_content = self.data.get(sha1)
        if not raw_content:
            raise ObjNotFoundError()
        return raw_content
