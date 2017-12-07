# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest
import logging
from nose.tools import istest

from swh.indexer.metadata_dictionary import compute_metadata
from swh.indexer.metadata_detector import detect_metadata
from swh.indexer.metadata import ContentMetadataIndexer
from swh.indexer.metadata import RevisionMetadataIndexer
from swh.indexer.tests.test_utils import MockObjStorage, MockStorage
from swh.indexer.tests.test_utils import MockIndexerStorage


class TestContentMetadataIndexer(ContentMetadataIndexer):
    """Specific Metadata whose configuration is enough to satisfy the
       indexing tests.
    """
    def prepare(self):
        self.config.update({
            'rescheduling_task': None,
        })
        self.idx_storage = MockIndexerStorage()
        self.log = logging.getLogger('swh.indexer')
        self.objstorage = MockObjStorage()
        self.task_destination = None
        self.rescheduling_task = self.config['rescheduling_task']
        self.tools = self.register_tools(self.config['tools'])
        self.tool = self.tools[0]
        self.results = []


class TestRevisionMetadataIndexer(RevisionMetadataIndexer):
    """Specific indexer whose configuration is enough to satisfy the
       indexing tests.
    """
    def prepare(self):
        self.config = {
            'rescheduling_task': None,
            'storage': {
                'cls': 'remote',
                'args': {
                    'url': 'http://localhost:9999',
                }
            },
            'tools': {
                'name': 'swh-metadata-detector',
                'version': '0.0.1',
                'configuration': {
                    'type': 'local',
                    'context': 'npm'
                }
            }
        }
        self.storage = MockStorage()
        self.idx_storage = MockIndexerStorage()
        self.log = logging.getLogger('swh.indexer')
        self.objstorage = MockObjStorage()
        self.task_destination = None
        self.rescheduling_task = self.config['rescheduling_task']
        self.tools = self.register_tools(self.config['tools'])
        self.tool = self.tools[0]
        self.results = []


class Metadata(unittest.TestCase):
    """
    Tests metadata_mock_tool tool for Metadata detection
    """
    def setUp(self):
        """
        shows the entire diff in the results
        """
        self.maxDiff = None
        self.content_tool = {
            'name': 'swh-metadata-translator',
            'version': '0.0.1',
            'configuration': {
                'type': 'local',
                'context': 'npm'
            }
        }

    @istest
    def test_compute_metadata_none(self):
        """
        testing content empty content is empty
        should return None
        """
        # given
        content = b""
        context = "npm"

        # None if no metadata was found or an error occurred
        declared_metadata = None
        # when
        result = compute_metadata(context, content)
        # then
        self.assertEqual(declared_metadata, result)

    @istest
    def test_compute_metadata_npm(self):
        """
        testing only computation of metadata with hard_mapping_npm
        """
        # given
        content = b"""
            {
                "name": "test_metadata",
                "version": "0.0.1",
                "description": "Simple package.json test for indexer",
                  "repository": {
                    "type": "git",
                    "url": "https://github.com/moranegg/metadata_test"
                }
            }
        """
        declared_metadata = {
            'name': 'test_metadata',
            'version': '0.0.1',
            'description': 'Simple package.json test for indexer',
            'codeRepository': {
                'type': 'git',
                'url': 'https://github.com/moranegg/metadata_test'
              },
            'other': {}
        }

        # when
        result = compute_metadata("npm", content)
        # then
        self.assertEqual(declared_metadata, result)

    @istest
    def test_index_content_metadata_npm(self):
        """
        testing NPM with package.json
        - one sha1 uses a file that can't be translated to metadata and
          should return None in the translated metadata
        """
        # given
        sha1s = ['26a9f72a7c87cc9205725cfd879f514ff4f3d8d5',
                 'd4c647f0fc257591cc9ba1722484229780d1c607',
                 '02fb2c89e14f7fab46701478c83779c7beb7b069']
        # this metadata indexer computes only metadata for package.json
        # in npm context with a hard mapping
        metadata_indexer = TestContentMetadataIndexer(
            tool=self.content_tool, config={})

        # when
        metadata_indexer.run(sha1s, policy_update='ignore-dups')
        results = metadata_indexer.idx_storage.state

        expected_results = [{
            'indexer_configuration_id': 30,
            'translated_metadata': {
                'other': {},
                'codeRepository': {
                    'type': 'git',
                    'url': 'https://github.com/moranegg/metadata_test'
                },
                'description': 'Simple package.json test for indexer',
                'name': 'test_metadata',
                'version': '0.0.1'
            },
            'id': '26a9f72a7c87cc9205725cfd879f514ff4f3d8d5'
            }, {
            'indexer_configuration_id': 30,
            'translated_metadata': {
                'softwareRequirements': {
                        'JSONStream': '~1.3.1',
                        'abbrev': '~1.1.0',
                        'ansi-regex': '~2.1.1',
                        'ansicolors': '~0.3.2',
                        'ansistyles': '~0.1.3'
                },
                'issueTracker': {
                    'url': 'https://github.com/npm/npm/issues'
                },
                'author':
                    'Isaac Z. Schlueter <i@izs.me> (http://blog.izs.me)',
                'codeRepository': {
                    'type': 'git',
                    'url': 'https://github.com/npm/npm'
                },
                'description': 'a package manager for JavaScript',
                'softwareSuggestions': {
                        'tacks': '~1.2.6',
                        'tap': '~10.3.2'
                },
                'license': 'Artistic-2.0',
                'version': '5.0.3',
                'other': {
                    'preferGlobal': True,
                    'config': {
                        'publishtest': False
                    }
                },
                'name': 'npm',
                'keywords': [
                    'install',
                    'modules',
                    'package manager',
                    'package.json'
                ],
                'url': 'https://docs.npmjs.com/'
            },
            'id': 'd4c647f0fc257591cc9ba1722484229780d1c607'
            }, {
            'indexer_configuration_id': 30,
            'translated_metadata': None,
            'id': '02fb2c89e14f7fab46701478c83779c7beb7b069'
        }]

        # The assertion bellow returns False sometimes because of nested lists
        self.assertEqual(expected_results, results)

    @istest
    def test_detect_metadata_package_json(self):
        # given
        df = [{
                'sha1_git': b'abc',
                'name': b'index.js',
                'target': b'abc',
                'length': 897,
                'status': 'visible',
                'type': 'file',
                'perms': 33188,
                'dir_id': b'dir_a',
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
                'dir_id': b'dir_a',
                'sha1': b'cde'
        }]
        # when
        results = detect_metadata(df)

        expected_results = {
            'npm': [
                b'cde'
            ]
        }
        # then
        self.assertEqual(expected_results, results)

    @istest
    def test_revision_metadata_indexer(self):
        metadata_indexer = TestRevisionMetadataIndexer()

        sha1_gits = [
            b'8dbb6aeb036e7fd80664eb8bfd1507881af1ba9f',
        ]
        metadata_indexer.run(sha1_gits, 'update-dups')

        results = metadata_indexer.idx_storage.state

        expected_results = [{
            'id': b'8dbb6aeb036e7fd80664eb8bfd1507881af1ba9f',
            'translated_metadata': {
                'identifier': None,
                'maintainer': None,
                'url': [
                    'https://github.com/librariesio/yarn-parser#readme'
                ],
                'codeRepository': [{
                    'type': 'git',
                    'url': 'git+https://github.com/librariesio/yarn-parser.git'
                }],
                'author': ['Andrew Nesbitt'],
                'license': ['AGPL-3.0'],
                'version': ['1.0.0'],
                'description': [
                    'Tiny web service for parsing yarn.lock files'
                ],
                'relatedLink': None,
                'developmentStatus': None,
                'operatingSystem': None,
                'issueTracker': [{
                    'url': 'https://github.com/librariesio/yarn-parser/issues'
                }],
                'softwareRequirements': [{
                    'express': '^4.14.0',
                    'yarn': '^0.21.0',
                    'body-parser': '^1.15.2'
                }],
                'name': ['yarn-parser'],
                'keywords': [['yarn', 'parse', 'lock', 'dependencies']],
                'type': None,
                'email': None
            },
            'indexer_configuration_id': 7
        }]
        # then
        self.assertEqual(expected_results, results)
