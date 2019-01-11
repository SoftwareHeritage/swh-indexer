# Copyright (C) 2017-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json
import unittest
from unittest.mock import patch

import swh.indexer.ctags
from swh.indexer.ctags import (
    CtagsIndexer, run_ctags
)

from swh.indexer.tests.test_utils import (
    CommonContentIndexerTest,
    CommonIndexerWithErrorsTest, CommonIndexerNoTool,
    SHA1_TO_CTAGS, NoDiskIndexer, BASE_TEST_CONFIG,
    OBJ_STORAGE_DATA, fill_storage, fill_obj_storage
)


class BasicTest(unittest.TestCase):
    @patch('swh.indexer.ctags.subprocess')
    def test_run_ctags(self, mock_subprocess):
        """Computing licenses from a raw content should return results

        """
        output0 = """
{"name":"defun","kind":"function","line":1,"language":"scheme"}
{"name":"name","kind":"symbol","line":5,"language":"else"}"""
        output1 = """
{"name":"let","kind":"var","line":10,"language":"something"}"""

        expected_result0 = [
            {
                'name': 'defun',
                'kind': 'function',
                'line': 1,
                'lang': 'scheme'
            },
            {
                'name': 'name',
                'kind': 'symbol',
                'line': 5,
                'lang': 'else'
            }
        ]

        expected_result1 = [
            {
                'name': 'let',
                'kind': 'var',
                'line': 10,
                'lang': 'something'
            }
        ]
        for path, lang, intermediary_result, expected_result in [
                (b'some/path', 'lisp', output0, expected_result0),
                (b'some/path/2', 'markdown', output1, expected_result1)
        ]:
            mock_subprocess.check_output.return_value = intermediary_result
            actual_result = list(run_ctags(path, lang=lang))
            self.assertEqual(actual_result, expected_result)


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
    def parse_config_file(self, *args, **kwargs):
        return {
            **BASE_TEST_CONFIG,
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
            },
            'workdir': '/nowhere',
        }


class TestCtagsIndexer(CommonContentIndexerTest, unittest.TestCase):
    """Ctags indexer test scenarios:

    - Known sha1s in the input list have their data indexed
    - Unknown sha1 in the input list are not indexed

    """

    legacy_get_format = True

    def get_indexer_results(self, ids):
        yield from self.idx_storage.content_ctags_get(ids)

    def setUp(self):
        super().setUp()
        self.indexer = CtagsIndexerTest()
        self.idx_storage = self.indexer.idx_storage
        fill_storage(self.indexer.storage)
        fill_obj_storage(self.indexer.objstorage)

        # Prepare test input
        self.id0 = '01c9379dfc33803963d07c1ccc748d3fe4c96bb5'
        self.id1 = 'd4c647f0fc257591cc9ba1722484229780d1c607'
        self.id2 = '688a5ef812c53907562fe379d4b3851e69c7cb15'

        tool = {k.replace('tool_', ''): v
                for (k, v) in self.indexer.tool.items()}

        self.expected_results = {
            self.id0: {
                'id': self.id0,
                'tool': tool,
                **SHA1_TO_CTAGS[self.id0][0],
            },
            self.id1: {
                'id': self.id1,
                'tool': tool,
                **SHA1_TO_CTAGS[self.id1][0],
            },
            self.id2: {
                'id': self.id2,
                'tool': tool,
                **SHA1_TO_CTAGS[self.id2][0],
            }
        }

        self._set_mocks()

    def _set_mocks(self):
        def find_ctags_for_content(raw_content):
            for (sha1, ctags) in SHA1_TO_CTAGS.items():
                if OBJ_STORAGE_DATA[sha1] == raw_content:
                    return ctags
            else:
                raise ValueError(('%r not found in objstorage, can\'t mock '
                                  'its ctags.') % raw_content)

        def fake_language(raw_content, *args, **kwargs):
            ctags = find_ctags_for_content(raw_content)
            return {'lang': ctags[0]['lang']}
        self._real_compute_language = swh.indexer.ctags.compute_language
        swh.indexer.ctags.compute_language = fake_language

        def fake_check_output(cmd, *args, **kwargs):
            print(cmd)
            id_ = cmd[-1]  # when using NoDiskIndexer, path is replaced by id
            return '\n'.join(
                json.dumps({'language': ctag['lang'], **ctag})
                for ctag in SHA1_TO_CTAGS[id_])
        self._real_check_output = swh.indexer.ctags.subprocess.check_output
        swh.indexer.ctags.subprocess.check_output = fake_check_output

    def tearDown(self):
        swh.indexer.ctags.compute_language = self._real_compute_language
        swh.indexer.ctags.subprocess.check_output = self._real_check_output
        super().tearDown()


class CtagsIndexerUnknownToolTestStorage(
        CommonIndexerNoTool, CtagsIndexerTest):
    """Fossology license indexer with wrong configuration"""


class TestCtagsIndexersErrors(
        CommonIndexerWithErrorsTest, unittest.TestCase):
    """Test the indexer raise the right errors when wrongly initialized"""
    Indexer = CtagsIndexerUnknownToolTestStorage
