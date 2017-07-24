# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest
import logging
from nose.tools import istest

from swh.indexer.metadata_dictionary import compute_metadata
from swh.indexer.metadata import ContentMetadataIndexer
from swh.indexer.tests.test_utils import MockObjStorage


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
                'name': 'swh-metadata-translator',
                'version': '0.1',
                'configuration': {
                    'type': 'local',
                    'context': 'npm'
                },
            }
        elif tool['tool_name'] == 'swh-metadata-detector':
            return {
                'id': 7,
                'name': 'swh-metadata-detector',
                'version': '0.1',
                'configuration': {
                    'type': 'local',
                    'context': 'npm'
                },
            }

    def revision_missing(self, revisions, cur=None):
        pass

    def revision_get(self, revisions):
        """Get all revisions from storage
           Args: an iterable of revision ids
           Returns: an iterable of revisions as dictionaries
                    (or None if the revision doesn't exist)
        """
        pass

    def directory_get(self,
                      directories,
                      cur=None):
        """Get information on directories.

        Args:
            - directories: an iterable of directory ids

        Returns:
            List of directories as dict with keys and associated values.

        """
        pass

    def directory_ls(self, directory, recursive=False, cur=None):
        """Get entries for one directory.

        Args:
            - directory: the directory to list entries from.
            - recursive: if flag on, this list recursively from this directory.

        Returns:
            List of entries for such directory.

        """
        pass


class TestMetadataIndexer(ContentMetadataIndexer):
    """Specific Metadata whose configuration is enough to satisfy the
       indexing tests.
    """
    def prepare(self):
        self.config = {
            'rescheduling_task': None,
            'tools':  {
                'name': 'swh-metadata-translator',
                'version': '0.0.1',
                'configuration': {
                    'type': 'local',
                    'context': 'npm'
                }
            }
        }
        self.storage = MockStorage()
        self.log = logging.getLogger('swh.indexer')
        self.objstorage = MockObjStorage()
        self.task_destination = None
        self.rescheduling_task = self.config['rescheduling_task']
        self.tools = self.retrieve_tools_information()
        self.results = []


# class TestRevisionMetadataIndexer(RevsionMetadataIndexer):
#     """Specific indexer whose configuration is enough to satisfy the
#        indexing tests.
#     """
#     def prepare(self):
#         self.config = {
#             'rescheduling_task': None,
#             'tools':  {
#                 'name': 'swh-metadata-detector',
#                 'version': '0.0.1',
#                 'configuration': {
#                     'type': 'local',
#                     'context': 'npm'
#                 }
#             }
#         }
#         self.storage = MockStorage()
#         self.log = logging.getLogger('swh.indexer')
#         self.objstorage = MockObjStorage()
#         self.task_destination = None
#         self.rescheduling_task = self.config['rescheduling_task']
#         self.tools = self.retrieve_tools_information()
#         self.results = []


class Metadata(unittest.TestCase):
    """
    Tests metadata_mock_tool tool for Metadata detection
    """
    def setUp(self):
        """
        shows the entire diff in the results
        """
        self.maxDiff = None

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
        metadata_indexer = TestMetadataIndexer()

        # when
        metadata_indexer.run(sha1s, policy_update='ignore-dups')
        results = metadata_indexer.storage.state

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
