# Copyright (C) 2017-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest
import logging
from swh.indexer import language
from swh.indexer.language import ContentLanguageIndexer
from swh.indexer.tests.test_utils import (
    BasicMockIndexerStorage, MockObjStorage, CommonContentIndexerTest,
    CommonIndexerWithErrorsTest, CommonIndexerNoTool
)


class LanguageTestIndexer(ContentLanguageIndexer):
    """Specific language whose configuration is enough to satisfy the
       indexing tests.
    """
    def prepare(self):
        self.config = {
            'tools':  {
                'name': 'pygments',
                'version': '2.0.1+dfsg-1.1+deb8u1',
                'configuration': {
                    'type': 'library',
                    'debian-package': 'python3-pygments',
                    'max_content_size': 10240,
                },
            }
        }
        self.idx_storage = BasicMockIndexerStorage()
        self.log = logging.getLogger('swh.indexer')
        self.objstorage = MockObjStorage()
        self.tool_config = self.config['tools']['configuration']
        self.max_content_size = self.tool_config['max_content_size']
        self.tools = self.register_tools(self.config['tools'])
        self.tool = self.tools[0]


class Language(unittest.TestCase):
    """Tests pygments tool for language detection

    """
    def test_compute_language_none(self):
        # given
        self.content = ""
        self.declared_language = {
            'lang': None
        }
        # when
        result = language.compute_language(self.content)
        # then
        self.assertEqual(self.declared_language, result)


class TestLanguageIndexer(CommonContentIndexerTest, unittest.TestCase):
    """Language indexer test scenarios:

    - Known sha1s in the input list have their data indexed
    - Unknown sha1 in the input list are not indexed

    """
    def setUp(self):
        self.indexer = LanguageTestIndexer()

        self.id0 = '02fb2c89e14f7fab46701478c83779c7beb7b069'
        self.id1 = '103bc087db1d26afc3a0283f38663d081e9b01e6'
        self.id2 = 'd4c647f0fc257591cc9ba1722484229780d1c607'
        tool_id = self.indexer.tool['id']

        self.expected_results = {
            self.id0: {
                'id': self.id0,
                'indexer_configuration_id': tool_id,
                'lang': 'python',
            },
            self.id1: {
                'id': self.id1,
                'indexer_configuration_id': tool_id,
                'lang': 'c'
            },
            self.id2: {
                'id': self.id2,
                'indexer_configuration_id': tool_id,
                'lang': 'text-only'
            }
        }


class LanguageIndexerUnknownToolTestStorage(
        CommonIndexerNoTool, LanguageTestIndexer):
    """Fossology license indexer with wrong configuration"""


class TestLanguageIndexersErrors(
        CommonIndexerWithErrorsTest, unittest.TestCase):
    """Test the indexer raise the right errors when wrongly initialized"""
    Indexer = LanguageIndexerUnknownToolTestStorage
