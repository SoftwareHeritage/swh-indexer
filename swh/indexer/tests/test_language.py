# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest
import logging
from swh.indexer import language
from swh.indexer.language import ContentLanguageIndexer
from swh.indexer.tests.test_utils import MockObjStorage


class _MockIndexerStorage():
    """Mock storage to simplify reading indexers' outputs.
    """
    def content_language_add(self, languages, conflict_update=None):
        self.state = languages
        self.conflict_update = conflict_update

    def indexer_configuration_add(self, tools):
        return [{
            'id': 20,
        }]


class TestLanguageIndexer(ContentLanguageIndexer):
    """Specific language whose configuration is enough to satisfy the
       indexing tests.
    """
    def prepare(self):
        self.config = {
            'destination_task': None,
            'rescheduling_task': None,
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
        self.idx_storage = _MockIndexerStorage()
        self.log = logging.getLogger('swh.indexer')
        self.objstorage = MockObjStorage()
        self.destination_task = None
        self.rescheduling_task = self.config['rescheduling_task']
        self.tool_config = self.config['tools']['configuration']
        self.max_content_size = self.tool_config['max_content_size']
        self.tools = self.register_tools(self.config['tools'])
        self.tool = self.tools[0]


class Language(unittest.TestCase):
    """
    Tests pygments tool for language detection
    """
    def setUp(self):
        self.maxDiff = None

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

    def test_index_content_language_python(self):
        # given
        # testing python
        sha1s = ['02fb2c89e14f7fab46701478c83779c7beb7b069']
        lang_indexer = TestLanguageIndexer()

        # when
        lang_indexer.run(sha1s, policy_update='ignore-dups')
        results = lang_indexer.idx_storage.state

        expected_results = [{
            'id': '02fb2c89e14f7fab46701478c83779c7beb7b069',
            'indexer_configuration_id': 20,
            'lang': 'python'
        }]
        # then
        self.assertEqual(expected_results, results)

    def test_index_content_language_c(self):
        # given
        # testing c
        sha1s = ['103bc087db1d26afc3a0283f38663d081e9b01e6']
        lang_indexer = TestLanguageIndexer()

        # when
        lang_indexer.run(sha1s, policy_update='ignore-dups')
        results = lang_indexer.idx_storage.state

        expected_results = [{
            'id': '103bc087db1d26afc3a0283f38663d081e9b01e6',
            'indexer_configuration_id': 20,
            'lang': 'c'
        }]

        # then
        self.assertEqual('c', results[0]['lang'])
        self.assertEqual(expected_results, results)
