# Copyright (C) 2017-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest
import logging

from swh.indexer.metadata_dictionary import CROSSWALK_TABLE, MAPPINGS
from swh.indexer.metadata_detector import detect_metadata
from swh.indexer.metadata_detector import extract_minimal_metadata_dict
from swh.indexer.metadata import ContentMetadataIndexer
from swh.indexer.metadata import RevisionMetadataIndexer
from swh.indexer.tests.test_utils import MockObjStorage, MockStorage
from swh.indexer.tests.test_utils import MockIndexerStorage


class ContentMetadataTestIndexer(ContentMetadataIndexer):
    """Specific Metadata whose configuration is enough to satisfy the
       indexing tests.
    """
    def prepare(self):
        self.idx_storage = MockIndexerStorage()
        self.log = logging.getLogger('swh.indexer')
        self.objstorage = MockObjStorage()
        self.destination_task = None
        self.tools = self.register_tools(self.config['tools'])
        self.tool = self.tools[0]
        self.results = []


class RevisionMetadataTestIndexer(RevisionMetadataIndexer):
    """Specific indexer whose configuration is enough to satisfy the
       indexing tests.
    """

    ContentMetadataIndexer = ContentMetadataTestIndexer

    def prepare(self):
        self.config = {
            'storage': {
                'cls': 'remote',
                'args': {
                    'url': 'http://localhost:9999',
                }
            },
            'tools': {
                'name': 'swh-metadata-detector',
                'version': '0.0.2',
                'configuration': {
                    'type': 'local',
                    'context': 'NpmMapping'
                }
            }
        }
        self.storage = MockStorage()
        self.idx_storage = MockIndexerStorage()
        self.log = logging.getLogger('swh.indexer')
        self.objstorage = MockObjStorage()
        self.destination_task = None
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
            'version': '0.0.2',
            'configuration': {
                'type': 'local',
                'context': 'NpmMapping'
            }
        }
        MockIndexerStorage.added_data = []

    def test_crosstable(self):
        self.assertEqual(CROSSWALK_TABLE['NodeJS'], {
            'repository': 'https://codemeta.github.io/terms/codeRepository',
            'os': 'https://codemeta.github.io/terms/operatingSystem',
            'cpu': 'https://codemeta.github.io/terms/processorRequirements',
            'engines':
                'https://codemeta.github.io/terms/processorRequirements',
            'author': 'https://codemeta.github.io/terms/author',
            'author.email': 'https://codemeta.github.io/terms/email',
            'author.name': 'https://codemeta.github.io/terms/name',
            'contributor': 'https://codemeta.github.io/terms/contributor',
            'keywords': 'https://codemeta.github.io/terms/keywords',
            'license': 'https://codemeta.github.io/terms/license',
            'version': 'https://codemeta.github.io/terms/version',
            'description': 'https://codemeta.github.io/terms/description',
            'name': 'https://codemeta.github.io/terms/name',
            'bugs': 'https://codemeta.github.io/terms/issueTracker',
            'homepage': 'https://codemeta.github.io/terms/url'
        })

    def test_compute_metadata_none(self):
        """
        testing content empty content is empty
        should return None
        """
        # given
        content = b""

        # None if no metadata was found or an error occurred
        declared_metadata = None
        # when
        result = MAPPINGS["NpmMapping"].translate(content)
        # then
        self.assertEqual(declared_metadata, result)

    def test_compute_metadata_npm(self):
        """
        testing only computation of metadata with hard_mapping_npm
        """
        # given
        content = b"""
            {
                "name": "test_metadata",
                "version": "0.0.2",
                "description": "Simple package.json test for indexer",
                  "repository": {
                    "type": "git",
                    "url": "https://github.com/moranegg/metadata_test"
                },
                "author": {
                    "email": "moranegg@example.com",
                    "name": "Morane G"
                }
            }
        """
        declared_metadata = {
            '@context': 'https://doi.org/10.5063/schema/codemeta-2.0',
            'codemeta:name': 'test_metadata',
            'codemeta:version': '0.0.2',
            'codemeta:description': 'Simple package.json test for indexer',
            'codemeta:codeRepository':
                'git+https://github.com/moranegg/metadata_test',
            'codemeta:author': {
                'type': 'codemeta:Person',
                'codemeta:name': 'Morane G',
                'codemeta:email': 'moranegg@example.com',
            },
        }

        # when
        result = MAPPINGS["NpmMapping"].translate(content)
        # then
        self.assertEqual(declared_metadata, result)

    def test_extract_minimal_metadata_dict(self):
        """
        Test the creation of a coherent minimal metadata set
        """
        # given
        metadata_list = [{
            '@context': 'https://doi.org/10.5063/schema/codemeta-2.0',
            'codemeta:name': 'test_1',
            'codemeta:version': '0.0.2',
            'codemeta:description': 'Simple package.json test for indexer',
            'codemeta:codeRepository':
                'git+https://github.com/moranegg/metadata_test',
        }, {
            '@context': 'https://doi.org/10.5063/schema/codemeta-2.0',
            'codemeta:name': 'test_0_1',
            'codemeta:version': '0.0.2',
            'codemeta:description': 'Simple package.json test for indexer',
            'codemeta:codeRepository':
                'git+https://github.com/moranegg/metadata_test'
        }, {
            '@context': 'https://doi.org/10.5063/schema/codemeta-2.0',
            'codemeta:name': 'test_metadata',
            'codemeta:version': '0.0.2',
            'codemeta:author': 'moranegg',
        }]

        # when
        results = extract_minimal_metadata_dict(metadata_list)

        # then
        expected_results = {
            '@context': 'https://doi.org/10.5063/schema/codemeta-2.0',
            "codemeta:version": '0.0.2',
            "codemeta:description": 'Simple package.json test for indexer',
            "codemeta:name": ['test_1', 'test_0_1', 'test_metadata'],
            "codemeta:author": 'moranegg',
            "codemeta:codeRepository":
                'git+https://github.com/moranegg/metadata_test',
        }
        self.assertEqual(expected_results, results)

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
        metadata_indexer = ContentMetadataTestIndexer(
            tool=self.content_tool, config={})

        # when
        metadata_indexer.run(sha1s, policy_update='ignore-dups')
        results = metadata_indexer.idx_storage.added_data

        expected_results = [('content_metadata', False, [{
            'indexer_configuration_id': 30,
            'translated_metadata': {
                '@context': 'https://doi.org/10.5063/schema/codemeta-2.0',
                'codemeta:codeRepository':
                    'git+https://github.com/moranegg/metadata_test',
                'codemeta:description': 'Simple package.json test for indexer',
                'codemeta:name': 'test_metadata',
                'codemeta:version': '0.0.1'
            },
            'id': '26a9f72a7c87cc9205725cfd879f514ff4f3d8d5'
            }, {
            'indexer_configuration_id': 30,
            'translated_metadata': {
                '@context': 'https://doi.org/10.5063/schema/codemeta-2.0',
                'codemeta:issueTracker':
                    'https://github.com/npm/npm/issues',
                'codemeta:author': {
                    'type': 'codemeta:Person',
                    'codemeta:name': 'Isaac Z. Schlueter',
                    'codemeta:email': 'i@izs.me',
                    'codemeta:url': 'http://blog.izs.me',
                },
                'codemeta:codeRepository':
                    'git+https://github.com/npm/npm',
                'codemeta:description': 'a package manager for JavaScript',
                'codemeta:license': 'Artistic-2.0',
                'codemeta:version': '5.0.3',
                'codemeta:name': 'npm',
                'codemeta:keywords': [
                    'install',
                    'modules',
                    'package manager',
                    'package.json'
                ],
                'codemeta:url': 'https://docs.npmjs.com/'
            },
            'id': 'd4c647f0fc257591cc9ba1722484229780d1c607'
            }, {
            'indexer_configuration_id': 30,
            'translated_metadata': None,
            'id': '02fb2c89e14f7fab46701478c83779c7beb7b069'
        }])]

        # The assertion below returns False sometimes because of nested lists
        self.assertEqual(expected_results, results)

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
            'NpmMapping': [
                b'cde'
            ]
        }
        # then
        self.assertEqual(expected_results, results)

    def test_revision_metadata_indexer(self):
        metadata_indexer = RevisionMetadataTestIndexer()

        sha1_gits = [
            b'8dbb6aeb036e7fd80664eb8bfd1507881af1ba9f',
        ]
        metadata_indexer.run(sha1_gits, 'update-dups')

        results = metadata_indexer.idx_storage.added_data

        expected_results = [('revision_metadata', True, [{
            'id': '8dbb6aeb036e7fd80664eb8bfd1507881af1ba9f',
            'translated_metadata': {
                '@context': 'https://doi.org/10.5063/schema/codemeta-2.0',
                'codemeta:url':
                    'https://github.com/librariesio/yarn-parser#readme',
                'codemeta:codeRepository':
                    'git+https://github.com/librariesio/yarn-parser.git',
                'codemeta:author': 'Andrew Nesbitt',
                'codemeta:license': 'AGPL-3.0',
                'codemeta:version': '1.0.0',
                'codemeta:description':
                    'Tiny web service for parsing yarn.lock files',
                'codemeta:issueTracker':
                    'https://github.com/librariesio/yarn-parser/issues',
                'codemeta:name': 'yarn-parser',
                'codemeta:keywords': ['yarn', 'parse', 'lock', 'dependencies'],
            },
            'indexer_configuration_id': 7
        }])]
        # then
        self.assertEqual(expected_results, results)
