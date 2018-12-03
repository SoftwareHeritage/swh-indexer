# Copyright (C) 2017-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from swh.model.hashutil import hash_to_bytes

from swh.indexer.metadata_dictionary import CROSSWALK_TABLE, MAPPINGS
from swh.indexer.metadata_detector import (
    detect_metadata, extract_minimal_metadata_dict
)
from swh.indexer.metadata import (
    ContentMetadataIndexer, RevisionMetadataIndexer
)

from .test_utils import (
    BASE_TEST_CONFIG, fill_obj_storage, fill_storage
)


TRANSLATOR_TOOL = {
    'name': 'swh-metadata-translator',
    'version': '0.0.2',
    'configuration': {
        'type': 'local',
        'context': 'NpmMapping'
    }
}


class ContentMetadataTestIndexer(ContentMetadataIndexer):
    """Specific Metadata whose configuration is enough to satisfy the
       indexing tests.
    """
    def parse_config_file(self, *args, **kwargs):
        assert False, 'should not be called; the rev indexer configures it.'


class RevisionMetadataTestIndexer(RevisionMetadataIndexer):
    """Specific indexer whose configuration is enough to satisfy the
       indexing tests.
    """

    ContentMetadataIndexer = ContentMetadataTestIndexer

    def parse_config_file(self, *args, **kwargs):
        return {
            **BASE_TEST_CONFIG,
            'tools': TRANSLATOR_TOOL,
        }


class Metadata(unittest.TestCase):
    """
    Tests metadata_mock_tool tool for Metadata detection
    """
    def setUp(self):
        """
        shows the entire diff in the results
        """
        self.maxDiff = None

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
            'codeRepository':
                'git+https://github.com/moranegg/metadata_test',
            'author': [{
                'type': 'Person',
                'name': 'Morane G',
                'email': 'moranegg@example.com',
            }],
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
            'codeRepository':
                'git+https://github.com/moranegg/metadata_test',
        }, {
            '@context': 'https://doi.org/10.5063/schema/codemeta-2.0',
            'name': 'test_0_1',
            'version': '0.0.2',
            'description': 'Simple package.json test for indexer',
            'codeRepository':
                'git+https://github.com/moranegg/metadata_test'
        }, {
            '@context': 'https://doi.org/10.5063/schema/codemeta-2.0',
            'name': 'test_metadata',
            'version': '0.0.2',
            'author': 'moranegg',
        }]

        # when
        results = extract_minimal_metadata_dict(metadata_list)

        # then
        expected_results = {
            '@context': 'https://doi.org/10.5063/schema/codemeta-2.0',
            "version": '0.0.2',
            "description": 'Simple package.json test for indexer',
            "name": ['test_1', 'test_0_1', 'test_metadata'],
            "author": ['moranegg'],
            "codeRepository":
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
        sha1s = [
            hash_to_bytes('26a9f72a7c87cc9205725cfd879f514ff4f3d8d5'),
            hash_to_bytes('d4c647f0fc257591cc9ba1722484229780d1c607'),
            hash_to_bytes('02fb2c89e14f7fab46701478c83779c7beb7b069'),
        ]
        # this metadata indexer computes only metadata for package.json
        # in npm context with a hard mapping
        metadata_indexer = ContentMetadataTestIndexer(
            tool=TRANSLATOR_TOOL, config=BASE_TEST_CONFIG.copy())
        fill_obj_storage(metadata_indexer.objstorage)
        fill_storage(metadata_indexer.storage)

        # when
        metadata_indexer.run(sha1s, policy_update='ignore-dups')
        results = list(metadata_indexer.idx_storage.content_metadata_get(
            sha1s))

        expected_results = [{
            'translated_metadata': {
                '@context': 'https://doi.org/10.5063/schema/codemeta-2.0',
                'type': 'SoftwareSourceCode',
                'codeRepository':
                    'git+https://github.com/moranegg/metadata_test',
                'description': 'Simple package.json test for indexer',
                'name': 'test_metadata',
                'version': '0.0.1'
            },
            'id': hash_to_bytes('26a9f72a7c87cc9205725cfd879f514ff4f3d8d5')
            }, {
            'translated_metadata': {
                '@context': 'https://doi.org/10.5063/schema/codemeta-2.0',
                'type': 'SoftwareSourceCode',
                'issueTracker':
                    'https://github.com/npm/npm/issues',
                'author': [{
                    'type': 'Person',
                    'name': 'Isaac Z. Schlueter',
                    'email': 'i@izs.me',
                    'url': 'http://blog.izs.me',
                }],
                'codeRepository':
                    'git+https://github.com/npm/npm',
                'description': 'a package manager for JavaScript',
                'license': 'https://spdx.org/licenses/Artistic-2.0',
                'version': '5.0.3',
                'name': 'npm',
                'keywords': [
                    'install',
                    'modules',
                    'package manager',
                    'package.json'
                ],
                'url': 'https://docs.npmjs.com/'
            },
            'id': hash_to_bytes('d4c647f0fc257591cc9ba1722484229780d1c607')
            }, {
            'translated_metadata': None,
            'id': hash_to_bytes('02fb2c89e14f7fab46701478c83779c7beb7b069')
        }]

        for result in results:
            del result['tool']

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
          <licenses>
            <license>
              <name>Apache License, Version 2.0</name>
              <url>https://www.apache.org/licenses/LICENSE-2.0.txt</url>
              <distribution>repo</distribution>
              <comments>A business-friendly OSS license</comments>
            </license>
          </licenses>
        </project>"""
        result = MAPPINGS["MavenMapping"].translate(raw_content)
        self.assertEqual(result, {
            '@context': 'https://doi.org/10.5063/schema/codemeta-2.0',
            'type': 'SoftwareSourceCode',
            'name': 'Maven Default Project',
            'identifier': 'com.mycompany.app',
            'version': '1.2.3',
            'license': 'https://www.apache.org/licenses/LICENSE-2.0.txt',
            'codeRepository':
                'http://repo1.maven.org/maven2/com/mycompany/app/my-app',
        })

    def test_compute_metadata_maven_minimal(self):
        raw_content = b"""
        <project>
          <name>Maven Default Project</name>
          <modelVersion>4.0.0</modelVersion>
          <groupId>com.mycompany.app</groupId>
          <artifactId>my-app</artifactId>
          <version>1.2.3</version>
        </project>"""
        result = MAPPINGS["MavenMapping"].translate(raw_content)
        self.assertEqual(result, {
            '@context': 'https://doi.org/10.5063/schema/codemeta-2.0',
            'type': 'SoftwareSourceCode',
            'name': 'Maven Default Project',
            'identifier': 'com.mycompany.app',
            'version': '1.2.3',
            'codeRepository':
            'https://repo.maven.apache.org/maven2/com/mycompany/app/my-app',
            'license': [],
        })

    def test_compute_metadata_maven_multiple(self):
        '''Tests when there are multiple code repos and licenses.'''
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
            <repository>
              <id>example</id>
              <name>Example Maven Repo</name>
              <layout>default</layout>
              <url>http://example.org/maven2</url>
            </repository>
          </repositories>
          <licenses>
            <license>
              <name>Apache License, Version 2.0</name>
              <url>https://www.apache.org/licenses/LICENSE-2.0.txt</url>
              <distribution>repo</distribution>
              <comments>A business-friendly OSS license</comments>
            </license>
            <license>
              <name>MIT license</name>
              <url>https://opensource.org/licenses/MIT</url>
            </license>
          </licenses>
        </project>"""
        result = MAPPINGS["MavenMapping"].translate(raw_content)
        self.assertEqual(result, {
            '@context': 'https://doi.org/10.5063/schema/codemeta-2.0',
            'type': 'SoftwareSourceCode',
            'name': 'Maven Default Project',
            'identifier': 'com.mycompany.app',
            'version': '1.2.3',
            'license': [
                'https://www.apache.org/licenses/LICENSE-2.0.txt',
                'https://opensource.org/licenses/MIT',
            ],
            'codeRepository': [
                'http://repo1.maven.org/maven2/com/mycompany/app/my-app',
                'http://example.org/maven2/com/mycompany/app/my-app',
            ]
        })

    def test_revision_metadata_indexer(self):
        metadata_indexer = RevisionMetadataTestIndexer()
        fill_obj_storage(metadata_indexer.objstorage)
        fill_storage(metadata_indexer.storage)

        tool = metadata_indexer.idx_storage.indexer_configuration_get(
            {'tool_'+k: v for (k, v) in TRANSLATOR_TOOL.items()})
        assert tool is not None

        metadata_indexer.idx_storage.content_metadata_add([{
            'indexer_configuration_id': tool['id'],
            'id': b'cde',
            'translated_metadata': {
                '@context': 'https://doi.org/10.5063/schema/codemeta-2.0',
                'type': 'SoftwareSourceCode',
                'issueTracker':
                    'https://github.com/librariesio/yarn-parser/issues',
                'version': '1.0.0',
                'name': 'yarn-parser',
                'author': ['Andrew Nesbitt'],
                'url':
                    'https://github.com/librariesio/yarn-parser#readme',
                'processorRequirements': {'node': '7.5'},
                'license': 'AGPL-3.0',
                'keywords': ['yarn', 'parse', 'lock', 'dependencies'],
                'codeRepository':
                    'git+https://github.com/librariesio/yarn-parser.git',
                'description':
                    'Tiny web service for parsing yarn.lock files',
                }
        }])

        sha1_gits = [
            hash_to_bytes('8dbb6aeb036e7fd80664eb8bfd1507881af1ba9f'),
        ]
        metadata_indexer.run(sha1_gits, 'update-dups')

        results = list(metadata_indexer.idx_storage.revision_metadata_get(
            sha1_gits))

        expected_results = [{
            'id': hash_to_bytes('8dbb6aeb036e7fd80664eb8bfd1507881af1ba9f'),
            'tool': TRANSLATOR_TOOL,
            'translated_metadata': {
                '@context': 'https://doi.org/10.5063/schema/codemeta-2.0',
                'url':
                    'https://github.com/librariesio/yarn-parser#readme',
                'codeRepository':
                    'git+https://github.com/librariesio/yarn-parser.git',
                'author': ['Andrew Nesbitt'],
                'license': 'AGPL-3.0',
                'version': '1.0.0',
                'description':
                    'Tiny web service for parsing yarn.lock files',
                'issueTracker':
                    'https://github.com/librariesio/yarn-parser/issues',
                'name': 'yarn-parser',
                'keywords': ['yarn', 'parse', 'lock', 'dependencies'],
            },
        }]

        for result in results:
            del result['tool']['id']

        # then
        self.assertEqual(expected_results, results)
