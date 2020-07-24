# Copyright (C) 2017-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json
import unittest

from hypothesis import given, strategies, settings, HealthCheck

from swh.model.hashutil import hash_to_bytes
from swh.model.model import Directory, DirectoryEntry, Revision

from swh.indexer.codemeta import CODEMETA_TERMS
from swh.indexer.metadata_dictionary import MAPPINGS
from swh.indexer.metadata_dictionary.maven import MavenMapping
from swh.indexer.metadata_dictionary.npm import NpmMapping
from swh.indexer.metadata_dictionary.ruby import GemspecMapping
from swh.indexer.metadata_detector import detect_metadata
from swh.indexer.metadata import ContentMetadataIndexer, RevisionMetadataIndexer

from swh.indexer.tests.utils import REVISION, DIRECTORY2

from .utils import (
    BASE_TEST_CONFIG,
    fill_obj_storage,
    fill_storage,
    YARN_PARSER_METADATA,
    json_document_strategy,
    xml_document_strategy,
)


TRANSLATOR_TOOL = {
    "name": "swh-metadata-translator",
    "version": "0.0.2",
    "configuration": {"type": "local", "context": "NpmMapping"},
}


class ContentMetadataTestIndexer(ContentMetadataIndexer):
    """Specific Metadata whose configuration is enough to satisfy the
       indexing tests.
    """

    def parse_config_file(self, *args, **kwargs):
        assert False, "should not be called; the rev indexer configures it."


REVISION_METADATA_CONFIG = {
    **BASE_TEST_CONFIG,
    "tools": TRANSLATOR_TOOL,
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
        self.npm_mapping = MAPPINGS["NpmMapping"]()
        self.codemeta_mapping = MAPPINGS["CodemetaMapping"]()
        self.maven_mapping = MAPPINGS["MavenMapping"]()
        self.pkginfo_mapping = MAPPINGS["PythonPkginfoMapping"]()
        self.gemspec_mapping = MAPPINGS["GemspecMapping"]()

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
        result = self.npm_mapping.translate(content)
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
            "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
            "type": "SoftwareSourceCode",
            "name": "test_metadata",
            "version": "0.0.2",
            "description": "Simple package.json test for indexer",
            "codeRepository": "git+https://github.com/moranegg/metadata_test",
            "author": [
                {"type": "Person", "name": "Morane G", "email": "moranegg@example.com",}
            ],
        }

        # when
        result = self.npm_mapping.translate(content)
        # then
        self.assertEqual(declared_metadata, result)

    def test_index_content_metadata_npm(self):
        """
        testing NPM with package.json
        - one sha1 uses a file that can't be translated to metadata and
          should return None in the translated metadata
        """
        # given
        sha1s = [
            hash_to_bytes("26a9f72a7c87cc9205725cfd879f514ff4f3d8d5"),
            hash_to_bytes("d4c647f0fc257591cc9ba1722484229780d1c607"),
            hash_to_bytes("02fb2c89e14f7fab46701478c83779c7beb7b069"),
        ]
        # this metadata indexer computes only metadata for package.json
        # in npm context with a hard mapping
        config = BASE_TEST_CONFIG.copy()
        config["tools"] = [TRANSLATOR_TOOL]
        metadata_indexer = ContentMetadataTestIndexer(config=config)
        fill_obj_storage(metadata_indexer.objstorage)
        fill_storage(metadata_indexer.storage)

        # when
        metadata_indexer.run(sha1s, policy_update="ignore-dups")
        results = list(metadata_indexer.idx_storage.content_metadata_get(sha1s))

        expected_results = [
            {
                "metadata": {
                    "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
                    "type": "SoftwareSourceCode",
                    "codeRepository": "git+https://github.com/moranegg/metadata_test",
                    "description": "Simple package.json test for indexer",
                    "name": "test_metadata",
                    "version": "0.0.1",
                },
                "id": hash_to_bytes("26a9f72a7c87cc9205725cfd879f514ff4f3d8d5"),
            },
            {
                "metadata": {
                    "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
                    "type": "SoftwareSourceCode",
                    "issueTracker": "https://github.com/npm/npm/issues",
                    "author": [
                        {
                            "type": "Person",
                            "name": "Isaac Z. Schlueter",
                            "email": "i@izs.me",
                            "url": "http://blog.izs.me",
                        }
                    ],
                    "codeRepository": "git+https://github.com/npm/npm",
                    "description": "a package manager for JavaScript",
                    "license": "https://spdx.org/licenses/Artistic-2.0",
                    "version": "5.0.3",
                    "name": "npm",
                    "keywords": [
                        "install",
                        "modules",
                        "package manager",
                        "package.json",
                    ],
                    "url": "https://docs.npmjs.com/",
                },
                "id": hash_to_bytes("d4c647f0fc257591cc9ba1722484229780d1c607"),
            },
        ]

        for result in results:
            del result["tool"]

        # The assertion below returns False sometimes because of nested lists
        self.assertEqual(expected_results, results)

    def test_npm_bugs_normalization(self):
        # valid dictionary
        package_json = b"""{
            "name": "foo",
            "bugs": {
                "url": "https://github.com/owner/project/issues",
                "email": "foo@example.com"
            }
        }"""
        result = self.npm_mapping.translate(package_json)
        self.assertEqual(
            result,
            {
                "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
                "name": "foo",
                "issueTracker": "https://github.com/owner/project/issues",
                "type": "SoftwareSourceCode",
            },
        )

        # "invalid" dictionary
        package_json = b"""{
            "name": "foo",
            "bugs": {
                "email": "foo@example.com"
            }
        }"""
        result = self.npm_mapping.translate(package_json)
        self.assertEqual(
            result,
            {
                "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
                "name": "foo",
                "type": "SoftwareSourceCode",
            },
        )

        # string
        package_json = b"""{
            "name": "foo",
            "bugs": "https://github.com/owner/project/issues"
        }"""
        result = self.npm_mapping.translate(package_json)
        self.assertEqual(
            result,
            {
                "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
                "name": "foo",
                "issueTracker": "https://github.com/owner/project/issues",
                "type": "SoftwareSourceCode",
            },
        )

    def test_npm_repository_normalization(self):
        # normal
        package_json = b"""{
            "name": "foo",
            "repository": {
                "type" : "git",
                "url" : "https://github.com/npm/cli.git"
            }
        }"""
        result = self.npm_mapping.translate(package_json)
        self.assertEqual(
            result,
            {
                "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
                "name": "foo",
                "codeRepository": "git+https://github.com/npm/cli.git",
                "type": "SoftwareSourceCode",
            },
        )

        # missing url
        package_json = b"""{
            "name": "foo",
            "repository": {
                "type" : "git"
            }
        }"""
        result = self.npm_mapping.translate(package_json)
        self.assertEqual(
            result,
            {
                "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
                "name": "foo",
                "type": "SoftwareSourceCode",
            },
        )

        # github shortcut
        package_json = b"""{
            "name": "foo",
            "repository": "github:npm/cli"
        }"""
        result = self.npm_mapping.translate(package_json)
        expected_result = {
            "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
            "name": "foo",
            "codeRepository": "git+https://github.com/npm/cli.git",
            "type": "SoftwareSourceCode",
        }
        self.assertEqual(result, expected_result)

        # github shortshortcut
        package_json = b"""{
            "name": "foo",
            "repository": "npm/cli"
        }"""
        result = self.npm_mapping.translate(package_json)
        self.assertEqual(result, expected_result)

        # gitlab shortcut
        package_json = b"""{
            "name": "foo",
            "repository": "gitlab:user/repo"
        }"""
        result = self.npm_mapping.translate(package_json)
        self.assertEqual(
            result,
            {
                "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
                "name": "foo",
                "codeRepository": "git+https://gitlab.com/user/repo.git",
                "type": "SoftwareSourceCode",
            },
        )

    def test_detect_metadata_package_json(self):
        # given
        df = [
            {
                "sha1_git": b"abc",
                "name": b"index.js",
                "target": b"abc",
                "length": 897,
                "status": "visible",
                "type": "file",
                "perms": 33188,
                "dir_id": b"dir_a",
                "sha1": b"bcd",
            },
            {
                "sha1_git": b"aab",
                "name": b"package.json",
                "target": b"aab",
                "length": 712,
                "status": "visible",
                "type": "file",
                "perms": 33188,
                "dir_id": b"dir_a",
                "sha1": b"cde",
            },
        ]
        # when
        results = detect_metadata(df)

        expected_results = {"NpmMapping": [b"cde"]}
        # then
        self.assertEqual(expected_results, results)

    def test_compute_metadata_valid_codemeta(self):
        raw_content = b"""{
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
          }"""  # noqa
        expected_result = {
            "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
            "type": "SoftwareSourceCode",
            "identifier": "CodeMeta",
            "description": "CodeMeta is a concept vocabulary that can "
            "be used to standardize the exchange of software metadata "
            "across repositories and organizations.",
            "name": "CodeMeta: Minimal metadata schemas for science "
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
                    "id": "http://orcid.org/0000-0002-1642-628X",
                },
                {
                    "type": "Person",
                    "givenName": "Matthew B.",
                    "familyName": "Jones",
                    "email": "jones@nceas.ucsb.edu",
                    "id": "http://orcid.org/0000-0003-0077-4738",
                },
            ],
            "maintainer": {
                "type": "Person",
                "givenName": "Carl",
                "familyName": "Boettiger",
                "email": "cboettig@gmail.com",
                "id": "http://orcid.org/0000-0002-1642-628X",
            },
            "contIntegration": "https://travis-ci.org/codemeta/codemeta",
            "developmentStatus": "active",
            "downloadUrl": "https://github.com/codemeta/codemeta/archive/2.0.zip",
            "funder": {
                "id": "https://doi.org/10.13039/100000001",
                "type": "Organization",
                "name": "National Science Foundation",
            },
            "funding": "1549758; Codemeta: A Rosetta Stone for Metadata "
            "in Scientific Software",
            "keywords": ["metadata", "software"],
            "version": "2.0",
            "dateCreated": "2017-06-05",
            "datePublished": "2017-06-05",
            "programmingLanguage": "JSON-LD",
        }
        result = self.codemeta_mapping.translate(raw_content)
        self.assertEqual(result, expected_result)

    def test_compute_metadata_codemeta_alternate_context(self):
        raw_content = b"""{
            "@context": "https://raw.githubusercontent.com/codemeta/codemeta/master/codemeta.jsonld",
            "@type": "SoftwareSourceCode",
            "identifier": "CodeMeta"
        }"""  # noqa
        expected_result = {
            "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
            "type": "SoftwareSourceCode",
            "identifier": "CodeMeta",
        }
        result = self.codemeta_mapping.translate(raw_content)
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
        result = self.maven_mapping.translate(raw_content)
        self.assertEqual(
            result,
            {
                "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
                "type": "SoftwareSourceCode",
                "name": "Maven Default Project",
                "identifier": "com.mycompany.app",
                "version": "1.2.3",
                "license": "https://www.apache.org/licenses/LICENSE-2.0.txt",
                "codeRepository": (
                    "http://repo1.maven.org/maven2/com/mycompany/app/my-app"
                ),
            },
        )

    def test_compute_metadata_maven_empty(self):
        raw_content = b"""
        <project>
        </project>"""
        result = self.maven_mapping.translate(raw_content)
        self.assertEqual(
            result,
            {
                "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
                "type": "SoftwareSourceCode",
            },
        )

    def test_compute_metadata_maven_almost_empty(self):
        raw_content = b"""
        <project>
          <foo/>
        </project>"""
        result = self.maven_mapping.translate(raw_content)
        self.assertEqual(
            result,
            {
                "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
                "type": "SoftwareSourceCode",
            },
        )

    def test_compute_metadata_maven_invalid_xml(self):
        expected_warning = (
            "WARNING:swh.indexer.metadata_dictionary.maven.MavenMapping:"
            "Error parsing XML from foo"
        )

        raw_content = b"""
        <project>"""
        with self.assertLogs("swh.indexer.metadata_dictionary", level="WARNING") as cm:
            result = MAPPINGS["MavenMapping"]("foo").translate(raw_content)
            self.assertEqual(cm.output, [expected_warning])
        self.assertEqual(result, None)

        raw_content = b"""
        """
        with self.assertLogs("swh.indexer.metadata_dictionary", level="WARNING") as cm:
            result = MAPPINGS["MavenMapping"]("foo").translate(raw_content)
            self.assertEqual(cm.output, [expected_warning])
        self.assertEqual(result, None)

    def test_compute_metadata_maven_unknown_encoding(self):
        expected_warning = (
            "WARNING:swh.indexer.metadata_dictionary.maven.MavenMapping:"
            "Error detecting XML encoding from foo"
        )

        raw_content = b"""<?xml version="1.0" encoding="foo"?>
        <project>
        </project>"""
        with self.assertLogs("swh.indexer.metadata_dictionary", level="WARNING") as cm:
            result = MAPPINGS["MavenMapping"]("foo").translate(raw_content)
            self.assertEqual(cm.output, [expected_warning])
        self.assertEqual(result, None)

        raw_content = b"""<?xml version="1.0" encoding="UTF-7"?>
        <project>
        </project>"""
        with self.assertLogs("swh.indexer.metadata_dictionary", level="WARNING") as cm:
            result = MAPPINGS["MavenMapping"]("foo").translate(raw_content)
            self.assertEqual(cm.output, [expected_warning])
        self.assertEqual(result, None)

    def test_compute_metadata_maven_invalid_encoding(self):
        expected_warning = (
            "WARNING:swh.indexer.metadata_dictionary.maven.MavenMapping:"
            "Error unidecoding XML from foo"
        )

        raw_content = b"""<?xml version="1.0" encoding="UTF-8"?>
        <foo\xe5ct>
        </foo>"""
        with self.assertLogs("swh.indexer.metadata_dictionary", level="WARNING") as cm:
            result = MAPPINGS["MavenMapping"]("foo").translate(raw_content)
            self.assertEqual(cm.output, [expected_warning])
        self.assertEqual(result, None)

    def test_compute_metadata_maven_minimal(self):
        raw_content = b"""
        <project>
          <name>Maven Default Project</name>
          <modelVersion>4.0.0</modelVersion>
          <groupId>com.mycompany.app</groupId>
          <artifactId>my-app</artifactId>
          <version>1.2.3</version>
        </project>"""
        result = self.maven_mapping.translate(raw_content)
        self.assertEqual(
            result,
            {
                "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
                "type": "SoftwareSourceCode",
                "name": "Maven Default Project",
                "identifier": "com.mycompany.app",
                "version": "1.2.3",
                "codeRepository": (
                    "https://repo.maven.apache.org/maven2/com/mycompany/app/my-app"
                ),
            },
        )

    def test_compute_metadata_maven_empty_nodes(self):
        raw_content = b"""
        <project>
          <name>Maven Default Project</name>
          <modelVersion>4.0.0</modelVersion>
          <groupId>com.mycompany.app</groupId>
          <artifactId>my-app</artifactId>
          <version>1.2.3</version>
          <repositories>
          </repositories>
        </project>"""
        result = self.maven_mapping.translate(raw_content)
        self.assertEqual(
            result,
            {
                "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
                "type": "SoftwareSourceCode",
                "name": "Maven Default Project",
                "identifier": "com.mycompany.app",
                "version": "1.2.3",
                "codeRepository": (
                    "https://repo.maven.apache.org/maven2/com/mycompany/app/my-app"
                ),
            },
        )

        raw_content = b"""
        <project>
          <name>Maven Default Project</name>
          <modelVersion>4.0.0</modelVersion>
          <groupId>com.mycompany.app</groupId>
          <artifactId>my-app</artifactId>
          <version></version>
        </project>"""
        result = self.maven_mapping.translate(raw_content)
        self.assertEqual(
            result,
            {
                "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
                "type": "SoftwareSourceCode",
                "name": "Maven Default Project",
                "identifier": "com.mycompany.app",
                "codeRepository": (
                    "https://repo.maven.apache.org/maven2/com/mycompany/app/my-app"
                ),
            },
        )

        raw_content = b"""
        <project>
          <name></name>
          <modelVersion>4.0.0</modelVersion>
          <groupId>com.mycompany.app</groupId>
          <artifactId>my-app</artifactId>
          <version>1.2.3</version>
        </project>"""
        result = self.maven_mapping.translate(raw_content)
        self.assertEqual(
            result,
            {
                "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
                "type": "SoftwareSourceCode",
                "identifier": "com.mycompany.app",
                "version": "1.2.3",
                "codeRepository": (
                    "https://repo.maven.apache.org/maven2/com/mycompany/app/my-app"
                ),
            },
        )

        raw_content = b"""
        <project>
          <name>Maven Default Project</name>
          <modelVersion>4.0.0</modelVersion>
          <groupId>com.mycompany.app</groupId>
          <artifactId>my-app</artifactId>
          <version>1.2.3</version>
          <licenses>
          </licenses>
        </project>"""
        result = self.maven_mapping.translate(raw_content)
        self.assertEqual(
            result,
            {
                "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
                "type": "SoftwareSourceCode",
                "name": "Maven Default Project",
                "identifier": "com.mycompany.app",
                "version": "1.2.3",
                "codeRepository": (
                    "https://repo.maven.apache.org/maven2/com/mycompany/app/my-app"
                ),
            },
        )

        raw_content = b"""
        <project>
          <groupId></groupId>
          <version>1.2.3</version>
        </project>"""
        result = self.maven_mapping.translate(raw_content)
        self.assertEqual(
            result,
            {
                "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
                "type": "SoftwareSourceCode",
                "version": "1.2.3",
            },
        )

    def test_compute_metadata_maven_invalid_licenses(self):
        raw_content = b"""
        <project>
          <name>Maven Default Project</name>
          <modelVersion>4.0.0</modelVersion>
          <groupId>com.mycompany.app</groupId>
          <artifactId>my-app</artifactId>
          <version>1.2.3</version>
          <licenses>
            foo
          </licenses>
        </project>"""
        result = self.maven_mapping.translate(raw_content)
        self.assertEqual(
            result,
            {
                "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
                "type": "SoftwareSourceCode",
                "name": "Maven Default Project",
                "identifier": "com.mycompany.app",
                "version": "1.2.3",
                "codeRepository": (
                    "https://repo.maven.apache.org/maven2/com/mycompany/app/my-app"
                ),
            },
        )

    def test_compute_metadata_maven_multiple(self):
        """Tests when there are multiple code repos and licenses."""
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
        result = self.maven_mapping.translate(raw_content)
        self.assertEqual(
            result,
            {
                "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
                "type": "SoftwareSourceCode",
                "name": "Maven Default Project",
                "identifier": "com.mycompany.app",
                "version": "1.2.3",
                "license": [
                    "https://www.apache.org/licenses/LICENSE-2.0.txt",
                    "https://opensource.org/licenses/MIT",
                ],
                "codeRepository": [
                    "http://repo1.maven.org/maven2/com/mycompany/app/my-app",
                    "http://example.org/maven2/com/mycompany/app/my-app",
                ],
            },
        )

    def test_compute_metadata_pkginfo(self):
        raw_content = b"""\
Metadata-Version: 2.1
Name: swh.core
Version: 0.0.49
Summary: Software Heritage core utilities
Home-page: https://forge.softwareheritage.org/diffusion/DCORE/
Author: Software Heritage developers
Author-email: swh-devel@inria.fr
License: UNKNOWN
Project-URL: Bug Reports, https://forge.softwareheritage.org/maniphest
Project-URL: Funding, https://www.softwareheritage.org/donate
Project-URL: Source, https://forge.softwareheritage.org/source/swh-core
Description: swh-core
        ========
       \x20
        core library for swh's modules:
        - config parser
        - hash computations
        - serialization
        - logging mechanism
       \x20
Platform: UNKNOWN
Classifier: Programming Language :: Python :: 3
Classifier: Intended Audience :: Developers
Classifier: License :: OSI Approved :: GNU General Public License v3 (GPLv3)
Classifier: Operating System :: OS Independent
Classifier: Development Status :: 5 - Production/Stable
Description-Content-Type: text/markdown
Provides-Extra: testing
"""  # noqa
        result = self.pkginfo_mapping.translate(raw_content)
        self.assertCountEqual(
            result["description"],
            [
                "Software Heritage core utilities",  # note the comma here
                "swh-core\n"
                "========\n"
                "\n"
                "core library for swh's modules:\n"
                "- config parser\n"
                "- hash computations\n"
                "- serialization\n"
                "- logging mechanism\n"
                "",
            ],
            result,
        )
        del result["description"]
        self.assertEqual(
            result,
            {
                "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
                "type": "SoftwareSourceCode",
                "url": "https://forge.softwareheritage.org/diffusion/DCORE/",
                "name": "swh.core",
                "author": [
                    {
                        "type": "Person",
                        "name": "Software Heritage developers",
                        "email": "swh-devel@inria.fr",
                    }
                ],
                "version": "0.0.49",
            },
        )

    def test_compute_metadata_pkginfo_utf8(self):
        raw_content = b"""\
Metadata-Version: 1.1
Name: snowpyt
Description-Content-Type: UNKNOWN
Description: foo
        Hydrology N\xc2\xb083
"""  # noqa
        result = self.pkginfo_mapping.translate(raw_content)
        self.assertEqual(
            result,
            {
                "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
                "type": "SoftwareSourceCode",
                "name": "snowpyt",
                "description": "foo\nHydrology N°83",
            },
        )

    def test_compute_metadata_pkginfo_keywords(self):
        raw_content = b"""\
Metadata-Version: 2.1
Name: foo
Keywords: foo bar baz
"""  # noqa
        result = self.pkginfo_mapping.translate(raw_content)
        self.assertEqual(
            result,
            {
                "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
                "type": "SoftwareSourceCode",
                "name": "foo",
                "keywords": ["foo", "bar", "baz"],
            },
        )

    def test_compute_metadata_pkginfo_license(self):
        raw_content = b"""\
Metadata-Version: 2.1
Name: foo
License: MIT
"""  # noqa
        result = self.pkginfo_mapping.translate(raw_content)
        self.assertEqual(
            result,
            {
                "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
                "type": "SoftwareSourceCode",
                "name": "foo",
                "license": "MIT",
            },
        )

    def test_gemspec_base(self):
        raw_content = b"""
Gem::Specification.new do |s|
  s.name        = 'example'
  s.version     = '0.1.0'
  s.licenses    = ['MIT']
  s.summary     = "This is an example!"
  s.description = "Much longer explanation of the example!"
  s.authors     = ["Ruby Coder"]
  s.email       = 'rubycoder@example.com'
  s.files       = ["lib/example.rb"]
  s.homepage    = 'https://rubygems.org/gems/example'
  s.metadata    = { "source_code_uri" => "https://github.com/example/example" }
end"""
        result = self.gemspec_mapping.translate(raw_content)
        self.assertCountEqual(
            result.pop("description"),
            ["This is an example!", "Much longer explanation of the example!"],
        )
        self.assertEqual(
            result,
            {
                "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
                "type": "SoftwareSourceCode",
                "author": [{"type": "Person", "name": "Ruby Coder"}],
                "name": "example",
                "license": "https://spdx.org/licenses/MIT",
                "codeRepository": "https://rubygems.org/gems/example",
                "email": "rubycoder@example.com",
                "version": "0.1.0",
            },
        )

    def test_gemspec_two_author_fields(self):
        raw_content = b"""
Gem::Specification.new do |s|
  s.authors     = ["Ruby Coder1"]
  s.author      = "Ruby Coder2"
end"""
        result = self.gemspec_mapping.translate(raw_content)
        self.assertCountEqual(
            result.pop("author"),
            [
                {"type": "Person", "name": "Ruby Coder1"},
                {"type": "Person", "name": "Ruby Coder2"},
            ],
        )
        self.assertEqual(
            result,
            {
                "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
                "type": "SoftwareSourceCode",
            },
        )

    def test_gemspec_invalid_author(self):
        raw_content = b"""
Gem::Specification.new do |s|
  s.author      = ["Ruby Coder"]
end"""
        result = self.gemspec_mapping.translate(raw_content)
        self.assertEqual(
            result,
            {
                "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
                "type": "SoftwareSourceCode",
            },
        )
        raw_content = b"""
Gem::Specification.new do |s|
  s.author      = "Ruby Coder1",
end"""
        result = self.gemspec_mapping.translate(raw_content)
        self.assertEqual(
            result,
            {
                "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
                "type": "SoftwareSourceCode",
            },
        )
        raw_content = b"""
Gem::Specification.new do |s|
  s.authors     = ["Ruby Coder1", ["Ruby Coder2"]]
end"""
        result = self.gemspec_mapping.translate(raw_content)
        self.assertEqual(
            result,
            {
                "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
                "type": "SoftwareSourceCode",
                "author": [{"type": "Person", "name": "Ruby Coder1"}],
            },
        )

    def test_gemspec_alternative_header(self):
        raw_content = b"""
require './lib/version'

Gem::Specification.new { |s|
  s.name = 'rb-system-with-aliases'
  s.summary = 'execute system commands with aliases'
}
"""
        result = self.gemspec_mapping.translate(raw_content)
        self.assertEqual(
            result,
            {
                "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
                "type": "SoftwareSourceCode",
                "name": "rb-system-with-aliases",
                "description": "execute system commands with aliases",
            },
        )

    @settings(suppress_health_check=[HealthCheck.too_slow])
    @given(json_document_strategy(keys=list(NpmMapping.mapping)))
    def test_npm_adversarial(self, doc):
        raw = json.dumps(doc).encode()
        self.npm_mapping.translate(raw)

    @settings(suppress_health_check=[HealthCheck.too_slow])
    @given(json_document_strategy(keys=CODEMETA_TERMS))
    def test_codemeta_adversarial(self, doc):
        raw = json.dumps(doc).encode()
        self.codemeta_mapping.translate(raw)

    @settings(suppress_health_check=[HealthCheck.too_slow])
    @given(
        xml_document_strategy(
            keys=list(MavenMapping.mapping),
            root="project",
            xmlns="http://maven.apache.org/POM/4.0.0",
        )
    )
    def test_maven_adversarial(self, doc):
        self.maven_mapping.translate(doc)

    @settings(suppress_health_check=[HealthCheck.too_slow])
    @given(
        strategies.dictionaries(
            # keys
            strategies.one_of(
                strategies.text(), *map(strategies.just, GemspecMapping.mapping)
            ),
            # values
            strategies.recursive(
                strategies.characters(),
                lambda children: strategies.lists(children, min_size=1),
            ),
        )
    )
    def test_gemspec_adversarial(self, doc):
        parts = [b"Gem::Specification.new do |s|\n"]
        for (k, v) in doc.items():
            parts.append("  s.{} = {}\n".format(k, repr(v)).encode())
        parts.append(b"end\n")
        self.gemspec_mapping.translate(b"".join(parts))

    def test_revision_metadata_indexer(self):
        metadata_indexer = RevisionMetadataIndexer(config=REVISION_METADATA_CONFIG)
        fill_obj_storage(metadata_indexer.objstorage)
        fill_storage(metadata_indexer.storage)

        tool = metadata_indexer.idx_storage.indexer_configuration_get(
            {f"tool_{k}": v for (k, v) in TRANSLATOR_TOOL.items()}
        )
        assert tool is not None
        rev = REVISION
        assert rev.directory == DIRECTORY2.id

        metadata_indexer.idx_storage.content_metadata_add(
            [
                {
                    "indexer_configuration_id": tool["id"],
                    "id": DIRECTORY2.entries[0].target,
                    "metadata": YARN_PARSER_METADATA,
                }
            ]
        )

        metadata_indexer.run([rev.id], "update-dups")

        results = list(
            metadata_indexer.idx_storage.revision_intrinsic_metadata_get([REVISION.id])
        )

        expected_results = [
            {
                "id": rev.id,
                "tool": TRANSLATOR_TOOL,
                "metadata": YARN_PARSER_METADATA,
                "mappings": ["npm"],
            }
        ]

        for result in results:
            del result["tool"]["id"]

        # then
        self.assertEqual(results, expected_results)

    def test_revision_metadata_indexer_single_root_dir(self):
        metadata_indexer = RevisionMetadataIndexer(config=REVISION_METADATA_CONFIG)
        fill_obj_storage(metadata_indexer.objstorage)
        fill_storage(metadata_indexer.storage)

        # Add a parent directory, that is the only directory at the root
        # of the revision
        rev = REVISION
        assert rev.directory == DIRECTORY2.id

        directory = Directory(
            entries=(
                DirectoryEntry(
                    name=b"foobar-1.0.0", type="dir", target=rev.directory, perms=16384,
                ),
            ),
        )
        assert directory.id is not None
        metadata_indexer.storage.directory_add([directory])

        new_rev_dict = {**rev.to_dict(), "directory": directory.id}
        new_rev_dict.pop("id")
        new_rev = Revision.from_dict(new_rev_dict)
        metadata_indexer.storage.revision_add([new_rev])

        tool = metadata_indexer.idx_storage.indexer_configuration_get(
            {f"tool_{k}": v for (k, v) in TRANSLATOR_TOOL.items()}
        )
        assert tool is not None

        metadata_indexer.idx_storage.content_metadata_add(
            [
                {
                    "indexer_configuration_id": tool["id"],
                    "id": DIRECTORY2.entries[0].target,
                    "metadata": YARN_PARSER_METADATA,
                }
            ]
        )

        metadata_indexer.run([new_rev.id], "update-dups")

        results = list(
            metadata_indexer.idx_storage.revision_intrinsic_metadata_get([new_rev.id])
        )

        expected_results = [
            {
                "id": new_rev.id,
                "tool": TRANSLATOR_TOOL,
                "metadata": YARN_PARSER_METADATA,
                "mappings": ["npm"],
            }
        ]

        for result in results:
            del result["tool"]["id"]

        # then
        self.assertEqual(results, expected_results)
