# Copyright (C) 2017-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest
import pytest

from swh.indexer import language
from swh.indexer.language import LanguageIndexer
from swh.indexer.tests.utils import (
    CommonContentIndexerTest,
    BASE_TEST_CONFIG, fill_storage, fill_obj_storage, filter_dict,
)


CONFIG = {
    **BASE_TEST_CONFIG,
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

    legacy_get_format = True

    def get_indexer_results(self, ids):
        yield from self.indexer.idx_storage.content_language_get(ids)

    def setUp(self):
        self.indexer = LanguageIndexer(config=CONFIG)
        fill_storage(self.indexer.storage)
        fill_obj_storage(self.indexer.objstorage)

        self.id0 = '02fb2c89e14f7fab46701478c83779c7beb7b069'
        self.id1 = '103bc087db1d26afc3a0283f38663d081e9b01e6'
        self.id2 = 'd4c647f0fc257591cc9ba1722484229780d1c607'

        tool = {k.replace('tool_', ''): v
                for (k, v) in self.indexer.tool.items()}

        self.expected_results = {
            self.id0: {
                'id': self.id0,
                'tool': tool,
                'lang': 'python',
            },
            self.id1: {
                'id': self.id1,
                'tool': tool,
                'lang': 'c'
            },
            self.id2: {
                'id': self.id2,
                'tool': tool,
                'lang': 'text-only'
            }
        }


def test_language_w_no_tool():
    with pytest.raises(ValueError):
        LanguageIndexer(config=filter_dict(CONFIG, 'tools'))
