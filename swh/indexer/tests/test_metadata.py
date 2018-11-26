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

from swh.model.hashutil import hash_to_bytes


class ContentMetadataTestIndexer(ContentMetadataIndexer):
    """Specific Metadata whose configuration is enough to satisfy the
       indexing tests.
    """
    def prepare(self):
        self.idx_storage = MockIndexerStorage()
        self.log = logging.getLogger('swh.indexer')
        self.objstorage = MockObjStorage()
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
            'storage': {},
            'objstorage': {},
            'indexer_storage': {},
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
        self.tools = self.register_tools(self.config['tools'])
        self.tool = self.tools[0]


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
            'repository': 'http://schema.org/codeRepository',
            'os': 'http://schema.org/operatingSystem',
            'cpu': 'http://schema.org/processorRequirements',
            'engines':
                'http://schema.org/processorRequirements',
            'author': 'http://schema.org/author',
            'author.email': 'http://schema.org/email',
            'author.name': 'http://schema.org/name',
            'contributor': 'http://schema.org/contributor',
            'keywords': 'http://schema.org/keywords',
            'license': 'http://schema.org/license',
            'version': 'http://schema.org/version',
            'description': 'http://schema.org/description',
            'name': 'http://schema.org/name',
            'bugs': 'https://codemeta.github.io/terms/issueTracker',
            'homepage': 'http://schema.org/url'
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
            'type': 'SoftwareSourceCode',
            'name': 'test_metadata',
            'version': '0.0.2',
            'description': 'Simple package.json test for indexer',
            'schema:codeRepository':
                'git+https://github.com/moranegg/metadata_test',
            'schema:author': {
                'type': 'Person',
                'name': 'Morane G',
                'email': 'moranegg@example.com',
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
            'name': 'test_1',
            'version': '0.0.2',
            'description': 'Simple package.json test for indexer',
            'schema:codeRepository':
                'git+https://github.com/moranegg/metadata_test',
        }, {
            '@context': 'https://doi.org/10.5063/schema/codemeta-2.0',
            'name': 'test_0_1',
            'version': '0.0.2',
            'description': 'Simple package.json test for indexer',
            'schema:codeRepository':
                'git+https://github.com/moranegg/metadata_test'
        }, {
            '@context': 'https://doi.org/10.5063/schema/codemeta-2.0',
            'name': 'test_metadata',
            'version': '0.0.2',
            'schema:author': 'moranegg',
        }]

        # when
        results = extract_minimal_metadata_dict(metadata_list)

        # then
        expected_results = {
            '@context': 'https://doi.org/10.5063/schema/codemeta-2.0',
            "version": '0.0.2',
            "description": 'Simple package.json test for indexer',
            "name": ['test_1', 'test_0_1', 'test_metadata'],
            "schema:author": 'moranegg',
            "schema:codeRepository":
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
                'type': 'SoftwareSourceCode',
                'schema:codeRepository':
                    'git+https://github.com/moranegg/metadata_test',
                'description': 'Simple package.json test for indexer',
                'name': 'test_metadata',
                'version': '0.0.1'
            },
            'id': '26a9f72a7c87cc9205725cfd879f514ff4f3d8d5'
            }, {
            'indexer_configuration_id': 30,
            'translated_metadata': {
                '@context': 'https://doi.org/10.5063/schema/codemeta-2.0',
                'type': 'SoftwareSourceCode',
                'codemeta:issueTracker':
                    'https://github.com/npm/npm/issues',
                'schema:author': {
                    'type': 'Person',
                    'name': 'Isaac Z. Schlueter',
                    'email': 'i@izs.me',
                    'schema:url': 'http://blog.izs.me',
                },
                'schema:codeRepository':
                    'git+https://github.com/npm/npm',
                'description': 'a package manager for JavaScript',
                'schema:license': 'Artistic-2.0',
                'version': '5.0.3',
                'name': 'npm',
                'keywords': [
                    'install',
                    'modules',
                    'package manager',
                    'package.json'
                ],
                'schema:url': 'https://docs.npmjs.com/'
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

    def test_compute_metadata_valid_codemeta(self):
        raw_content = (
            b"""{
            "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
            "@type": "SoftwareSourceCode",
            "identifier": "CodeMeta",
            "description": "CodeMeta is a concept vocabulary that can be used to standardize the exchange of software metadata across repositories and organizations.",
            "name": "CodeMeta: Minimal metadata schemas for science software and code, in JSON-LD",
            "codeRepository": "https://github.com/codemeta/codemeta",
            "issueTracker": "https://github.com/codemeta/codemeta/issues",
            "license": "https://spdx.org/licenses/Apache-2.0",
            "version": "2.0",
            "author": [
              {
                "@type": "Person",
                "givenName": "Carl",
                "familyName": "Boettiger",
                "email": "cboettig@gmail.com",
                "@id": "http://orcid.org/0000-0002-1642-628X"
              },
              {
                "@type": "Person",
                "givenName": "Matthew B.",
                "familyName": "Jones",
                "email": "jones@nceas.ucsb.edu",
                "@id": "http://orcid.org/0000-0003-0077-4738"
              }
            ],
            "maintainer": {
              "@type": "Person",
              "givenName": "Carl",
              "familyName": "Boettiger",
              "email": "cboettig@gmail.com",
              "@id": "http://orcid.org/0000-0002-1642-628X"
            },
            "contIntegration": "https://travis-ci.org/codemeta/codemeta",
            "developmentStatus": "active",
            "downloadUrl": "https://github.com/codemeta/codemeta/archive/2.0.zip",
            "funder": {
                "@id": "https://doi.org/10.13039/100000001",
                "@type": "Organization",
                "name": "National Science Foundation"
            },
            "funding":"1549758; Codemeta: A Rosetta Stone for Metadata in Scientific Software",
            "keywords": [
              "metadata",
              "software"
            ],
            "version":"2.0",
            "dateCreated":"2017-06-05",
            "datePublished":"2017-06-05",
            "programmingLanguage": "JSON-LD"
          }""") # noqa
        expected_result = {
            "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
            "type": "SoftwareSourceCode",
            "identifier": "CodeMeta",
            "description":
                "CodeMeta is a concept vocabulary that can "
                "be used to standardize the exchange of software metadata "
                "across repositories and organizations.",
            "name":
                "CodeMeta: Minimal metadata schemas for science "
                "software and code, in JSON-LD",
            "codeRepository": "https://github.com/codemeta/codemeta",
            "issueTracker": "https://github.com/codemeta/codemeta/issues",
            "license": "https://spdx.org/licenses/Apache-2.0",
            "version": "2.0",
            "author": [
              {
                "type": "Person",
                "givenName": "Carl",
                "familyName": "Boettiger",
                "email": "cboettig@gmail.com",
                "id": "http://orcid.org/0000-0002-1642-628X"
              },
              {
                "type": "Person",
                "givenName": "Matthew B.",
                "familyName": "Jones",
                "email": "jones@nceas.ucsb.edu",
                "id": "http://orcid.org/0000-0003-0077-4738"
              }
            ],
            "maintainer": {
              "type": "Person",
              "givenName": "Carl",
              "familyName": "Boettiger",
              "email": "cboettig@gmail.com",
              "id": "http://orcid.org/0000-0002-1642-628X"
            },
            "contIntegration": "https://travis-ci.org/codemeta/codemeta",
            "developmentStatus": "active",
            "downloadUrl":
                "https://github.com/codemeta/codemeta/archive/2.0.zip",
            "funder": {
                "id": "https://doi.org/10.13039/100000001",
                "type": "Organization",
                "name": "National Science Foundation"
            },
            "funding": "1549758; Codemeta: A Rosetta Stone for Metadata "
                "in Scientific Software",
            "keywords": [
              "metadata",
              "software"
            ],
            "version": "2.0",
            "dateCreated": "2017-06-05",
            "datePublished": "2017-06-05",
            "programmingLanguage": "JSON-LD"
          }
        result = MAPPINGS["CodemetaMapping"].translate(raw_content)
        self.assertEqual(result, expected_result)

    def test_compute_metadata_maven(self):
        raw_content = b"""
        <project>
          <name>Maven Default Project</name>
          <modelVersion>4.0.0</modelVersion>
          <groupId>com.mycompany.app</groupId>
          <artifactId>my-app</artifactId>
          <version>1.2.3</version>
          <repositories>
            <repository>
              <id>central</id>
              <name>Maven Repository Switchboard</name>
              <layout>default</layout>
              <url>http://repo1.maven.org/maven2</url>
              <snapshots>
                <enabled>false</enabled>
              </snapshots>
            </repository>
          </repositories>
        </project>"""
        result = MAPPINGS["MavenMapping"].translate(raw_content)
        self.assertEqual(result, {
            '@context': 'https://doi.org/10.5063/schema/codemeta-2.0',
            'type': 'SoftwareSourceCode',
            'name': 'Maven Default Project',
            'schema:identifier': 'com.mycompany.app',
            'version': '1.2.3',
            'schema:codeRepository':
                'http://repo1.maven.org/maven2/com/mycompany/app/my-app',
            })

    def test_revision_metadata_indexer(self):
        metadata_indexer = RevisionMetadataTestIndexer()

        sha1_gits = [
            hash_to_bytes('8dbb6aeb036e7fd80664eb8bfd1507881af1ba9f'),
        ]
        metadata_indexer.run(sha1_gits, 'update-dups')

        results = metadata_indexer.idx_storage.added_data

        expected_results = [('revision_metadata', True, [{
            'id': hash_to_bytes('8dbb6aeb036e7fd80664eb8bfd1507881af1ba9f'),
            'translated_metadata': {
                '@context': 'https://doi.org/10.5063/schema/codemeta-2.0',
                'url':
                    'https://github.com/librariesio/yarn-parser#readme',
                'schema:codeRepository':
                    'git+https://github.com/librariesio/yarn-parser.git',
                'schema:author': 'Andrew Nesbitt',
                'license': 'AGPL-3.0',
                'version': '1.0.0',
                'description':
                    'Tiny web service for parsing yarn.lock files',
                'codemeta:issueTracker':
                    'https://github.com/librariesio/yarn-parser/issues',
                'name': 'yarn-parser',
                'keywords': ['yarn', 'parse', 'lock', 'dependencies'],
            },
            'indexer_configuration_id': 7
        }])]
        # then
        self.assertEqual(expected_results, results)
