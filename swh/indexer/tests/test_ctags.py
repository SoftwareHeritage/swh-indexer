# Copyright (C) 2017-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest
import logging
from swh.indexer.ctags import CtagsIndexer
from swh.indexer.tests.test_utils import (
    BasicMockIndexerStorage, MockObjStorage, CommonContentIndexerTest,
    CommonIndexerWithErrorsTest, CommonIndexerNoTool,
    SHA1_TO_CTAGS, NoDiskIndexer
)


class InjectCtagsIndexer:
    """Override ctags computations.

    """
    def compute_ctags(self, path, lang):
        """Inject fake ctags given path (sha1 identifier).

        """
        return {
            'lang': lang,
            **SHA1_TO_CTAGS.get(path)
        }


class CtagsIndexerTest(NoDiskIndexer, InjectCtagsIndexer, CtagsIndexer):
    """Specific language whose configuration is enough to satisfy the
       indexing tests.
    """
    def prepare(self):
        self.config = {
            'tools': {
                'name': 'universal-ctags',
                'version': '~git7859817b',
                'configuration': {
                    'command_line': '''ctags --fields=+lnz --sort=no '''
                                    ''' --links=no <filepath>''',
                    'max_content_size': 1000,
                },
            },
            'languages': {
                'python': 'python',
                'haskell': 'haskell',
                'bar': 'bar',
            }
        }
        self.idx_storage = BasicMockIndexerStorage()
        self.log = logging.getLogger('swh.indexer')
        self.objstorage = MockObjStorage()
        self.tool_config = self.config['tools']['configuration']
        self.max_content_size = self.tool_config['max_content_size']
        self.tools = self.register_tools(self.config['tools'])
        self.tool = self.tools[0]
        self.language_map = self.config['languages']


class TestCtagsIndexer(CommonContentIndexerTest, unittest.TestCase):
    """Ctags indexer test scenarios:

    - Known sha1s in the input list have their data indexed
    - Unknown sha1 in the input list are not indexed

    """
    def setUp(self):
        self.indexer = CtagsIndexerTest()

        # Prepare test input
        self.id0 = '01c9379dfc33803963d07c1ccc748d3fe4c96bb5'
        self.id1 = 'd4c647f0fc257591cc9ba1722484229780d1c607'
        self.id2 = '688a5ef812c53907562fe379d4b3851e69c7cb15'

        tool_id = self.indexer.tool['id']
        self.expected_results = {
            self.id0: {
                'id': self.id0,
                'indexer_configuration_id': tool_id,
                'ctags': SHA1_TO_CTAGS[self.id0],
            },
            self.id1: {
                'id': self.id1,
                'indexer_configuration_id': tool_id,
                'ctags': SHA1_TO_CTAGS[self.id1],
            },
            self.id2: {
                'id': self.id2,
                'indexer_configuration_id': tool_id,
                'ctags': SHA1_TO_CTAGS[self.id2],
            }
        }


class CtagsIndexerUnknownToolTestStorage(
        CommonIndexerNoTool, CtagsIndexerTest):
    """Fossology license indexer with wrong configuration"""


class TestCtagsIndexersErrors(
        CommonIndexerWithErrorsTest, unittest.TestCase):
    """Test the indexer raise the right errors when wrongly initialized"""
    Indexer = CtagsIndexerUnknownToolTestStorage
