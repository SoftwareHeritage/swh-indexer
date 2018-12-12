# Copyright (C) 2015-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import pytest
import unittest

from hypothesis import given

from swh.model.hashutil import hash_to_bytes

from swh.indexer.storage import get_indexer_storage
from swh.core.tests.db_testing import SingleDbTestFixture
from swh.indexer.tests.storage.generate_data_test import (
    gen_content_mimetypes, gen_content_fossology_licenses
)
from swh.indexer.tests.storage import SQL_DIR

TOOLS = [
    {
        'tool_name': 'universal-ctags',
        'tool_version': '~git7859817b',
        'tool_configuration': {
            "command_line": "ctags --fields=+lnz --sort=no --links=no "
                            "--output-format=json <filepath>"}
    },
    {
        'tool_name': 'swh-metadata-translator',
        'tool_version': '0.0.1',
        'tool_configuration': {"type": "local", "context": "NpmMapping"},
    },
    {
        'tool_name': 'swh-metadata-detector',
        'tool_version': '0.0.1',
        'tool_configuration': {
            "type": "local", "context": ["NpmMapping", "CodemetaMapping"]},
    },
    {
        'tool_name': 'file',
        'tool_version': '5.22',
        'tool_configuration': {"command_line": "file --mime <filepath>"},
    },
    {
        'tool_name': 'pygments',
        'tool_version': '2.0.1+dfsg-1.1+deb8u1',
        'tool_configuration': {
            "type": "library", "debian-package": "python3-pygments"},
    },
    {
        'tool_name': 'pygments',
        'tool_version': '2.0.1+dfsg-1.1+deb8u1',
        'tool_configuration': {
            "type": "library",
            "debian-package": "python3-pygments",
            "max_content_size": 10240
        },
    },
    {
        'tool_name': 'nomos',
        'tool_version': '3.1.0rc2-31-ga2cbb8c',
        'tool_configuration': {"command_line": "nomossa <filepath>"},
    }
]


@pytest.mark.db
class BasePgTestStorage(SingleDbTestFixture):
    """Base test class for most indexer tests.

    It adds support for Storage testing to the SingleDbTestFixture class.
    It will also build the database from the swh-indexed/sql/*.sql files.
    """

    TEST_DB_NAME = 'softwareheritage-test-indexer'
    TEST_DB_DUMP = os.path.join(SQL_DIR, '*.sql')

    def setUp(self):
        super().setUp()
        self.storage_config = {
            'cls': 'local',
            'args': {
                'db': 'dbname=%s' % self.TEST_DB_NAME,
            },
        }

    def tearDown(self):
        self.reset_storage_tables()
        self.storage = None
        super().tearDown()

    def reset_storage_tables(self):
        excluded = {'indexer_configuration'}
        self.reset_db_tables(self.TEST_DB_NAME, excluded=excluded)

        db = self.test_db[self.TEST_DB_NAME]
        db.conn.commit()


class CommonTestStorage:
    """Base class for Indexer Storage testing.

    """
    def setUp(self):
        super().setUp()
        self.storage = get_indexer_storage(**self.storage_config)
        tools = self.storage.indexer_configuration_add(TOOLS)
        self.tools = {}
        for tool in tools:
            tool_name = tool['tool_name']
            while tool_name in self.tools:
                tool_name += '_'
            self.tools[tool_name] = {
                'id': tool['id'],
                'name': tool['tool_name'],
                'version': tool['tool_version'],
                'configuration': tool['tool_configuration'],
            }

        self.sha1_1 = hash_to_bytes('34973274ccef6ab4dfaaf86599792fa9c3fe4689')
        self.sha1_2 = hash_to_bytes('61c2b3a30496d329e21af70dd2d7e097046d07b7')
        self.revision_id_1 = hash_to_bytes(
            '7026b7c1a2af56521e951c01ed20f255fa054238')
        self.revision_id_2 = hash_to_bytes(
            '7026b7c1a2af56521e9587659012345678904321')
        self.origin_id_1 = 54974445
        self.origin_id_2 = 44434342

    def test_check_config(self):
        self.assertTrue(self.storage.check_config(check_write=True))
        self.assertTrue(self.storage.check_config(check_write=False))

    def test_content_mimetype_missing(self):
        # given
        tool_id = self.tools['file']['id']

        mimetypes = [
            {
                'id': self.sha1_1,
                'indexer_configuration_id': tool_id,
            },
            {
                'id': self.sha1_2,
                'indexer_configuration_id': tool_id,
            }]

        # when
        actual_missing = self.storage.content_mimetype_missing(mimetypes)

        # then
        self.assertEqual(list(actual_missing), [
            self.sha1_1,
            self.sha1_2,
        ])

        # given
        self.storage.content_mimetype_add([{
            'id': self.sha1_2,
            'mimetype': 'text/plain',
            'encoding': 'utf-8',
            'indexer_configuration_id': tool_id,
        }])

        # when
        actual_missing = self.storage.content_mimetype_missing(mimetypes)

        # then
        self.assertEqual(list(actual_missing), [self.sha1_1])

    def test_content_mimetype_add__drop_duplicate(self):
        # given
        tool_id = self.tools['file']['id']

        mimetype_v1 = {
            'id': self.sha1_2,
            'mimetype': 'text/plain',
            'encoding': 'utf-8',
            'indexer_configuration_id': tool_id,
        }

        # given
        self.storage.content_mimetype_add([mimetype_v1])

        # when
        actual_mimetypes = list(self.storage.content_mimetype_get(
            [self.sha1_2]))

        # then
        expected_mimetypes_v1 = [{
            'id': self.sha1_2,
            'mimetype': 'text/plain',
            'encoding': 'utf-8',
            'tool': self.tools['file'],
        }]
        self.assertEqual(actual_mimetypes, expected_mimetypes_v1)

        # given
        mimetype_v2 = mimetype_v1.copy()
        mimetype_v2.update({
            'mimetype': 'text/html',
            'encoding': 'us-ascii',
        })

        self.storage.content_mimetype_add([mimetype_v2])

        actual_mimetypes = list(self.storage.content_mimetype_get(
            [self.sha1_2]))

        # mimetype did not change as the v2 was dropped.
        self.assertEqual(actual_mimetypes, expected_mimetypes_v1)

    def test_content_mimetype_add__update_in_place_duplicate(self):
        # given
        tool_id = self.tools['file']['id']

        mimetype_v1 = {
            'id': self.sha1_2,
            'mimetype': 'text/plain',
            'encoding': 'utf-8',
            'indexer_configuration_id': tool_id,
        }

        # given
        self.storage.content_mimetype_add([mimetype_v1])

        # when
        actual_mimetypes = list(self.storage.content_mimetype_get(
            [self.sha1_2]))

        expected_mimetypes_v1 = [{
            'id': self.sha1_2,
            'mimetype': 'text/plain',
            'encoding': 'utf-8',
            'tool': self.tools['file'],
        }]

        # then
        self.assertEqual(actual_mimetypes, expected_mimetypes_v1)

        # given
        mimetype_v2 = mimetype_v1.copy()
        mimetype_v2.update({
            'mimetype': 'text/html',
            'encoding': 'us-ascii',
        })

        self.storage.content_mimetype_add([mimetype_v2], conflict_update=True)

        actual_mimetypes = list(self.storage.content_mimetype_get(
            [self.sha1_2]))

        expected_mimetypes_v2 = [{
            'id': self.sha1_2,
            'mimetype': 'text/html',
            'encoding': 'us-ascii',
            'tool': {
                'id': self.tools['file']['id'],
                'name': 'file',
                'version': '5.22',
                'configuration': {'command_line': 'file --mime <filepath>'}
            }
        }]

        # mimetype did change as the v2 was used to overwrite v1
        self.assertEqual(actual_mimetypes, expected_mimetypes_v2)

    def test_content_mimetype_get(self):
        # given
        tool_id = self.tools['file']['id']

        mimetypes = [self.sha1_2, self.sha1_1]

        mimetype1 = {
            'id': self.sha1_2,
            'mimetype': 'text/plain',
            'encoding': 'utf-8',
            'indexer_configuration_id': tool_id,
        }

        # when
        self.storage.content_mimetype_add([mimetype1])

        # then
        actual_mimetypes = list(self.storage.content_mimetype_get(mimetypes))

        # then
        expected_mimetypes = [{
            'id': self.sha1_2,
            'mimetype': 'text/plain',
            'encoding': 'utf-8',
            'tool': self.tools['file']
        }]

        self.assertEqual(actual_mimetypes, expected_mimetypes)

    def test_content_language_missing(self):
        # given
        tool_id = self.tools['pygments']['id']

        languages = [
            {
                'id': self.sha1_2,
                'indexer_configuration_id': tool_id,
            },
            {
                'id': self.sha1_1,
                'indexer_configuration_id': tool_id,
            }
        ]

        # when
        actual_missing = list(self.storage.content_language_missing(languages))

        # then
        self.assertEqual(list(actual_missing), [
            self.sha1_2,
            self.sha1_1,
        ])

        # given
        self.storage.content_language_add([{
            'id': self.sha1_2,
            'lang': 'haskell',
            'indexer_configuration_id': tool_id,
        }])

        # when
        actual_missing = list(self.storage.content_language_missing(languages))

        # then
        self.assertEqual(actual_missing, [self.sha1_1])

    def test_content_language_get(self):
        # given
        tool_id = self.tools['pygments']['id']

        language1 = {
            'id': self.sha1_2,
            'lang': 'common-lisp',
            'indexer_configuration_id': tool_id,
        }

        # when
        self.storage.content_language_add([language1])

        # then
        actual_languages = list(self.storage.content_language_get(
            [self.sha1_2, self.sha1_1]))

        # then
        expected_languages = [{
            'id': self.sha1_2,
            'lang': 'common-lisp',
            'tool': self.tools['pygments']
        }]

        self.assertEqual(actual_languages, expected_languages)

    def test_content_language_add__drop_duplicate(self):
        # given
        tool_id = self.tools['pygments']['id']

        language_v1 = {
            'id': self.sha1_2,
            'lang': 'emacslisp',
            'indexer_configuration_id': tool_id,
        }

        # given
        self.storage.content_language_add([language_v1])

        # when
        actual_languages = list(self.storage.content_language_get(
            [self.sha1_2]))

        # then
        expected_languages_v1 = [{
            'id': self.sha1_2,
            'lang': 'emacslisp',
            'tool': self.tools['pygments']
        }]
        self.assertEqual(actual_languages, expected_languages_v1)

        # given
        language_v2 = language_v1.copy()
        language_v2.update({
            'lang': 'common-lisp',
        })

        self.storage.content_language_add([language_v2])

        actual_languages = list(self.storage.content_language_get(
            [self.sha1_2]))

        # language did not change as the v2 was dropped.
        self.assertEqual(actual_languages, expected_languages_v1)

    def test_content_language_add__update_in_place_duplicate(self):
        # given
        tool_id = self.tools['pygments']['id']

        language_v1 = {
            'id': self.sha1_2,
            'lang': 'common-lisp',
            'indexer_configuration_id': tool_id,
        }

        # given
        self.storage.content_language_add([language_v1])

        # when
        actual_languages = list(self.storage.content_language_get(
            [self.sha1_2]))

        # then
        expected_languages_v1 = [{
            'id': self.sha1_2,
            'lang': 'common-lisp',
            'tool': self.tools['pygments']
        }]
        self.assertEqual(actual_languages, expected_languages_v1)

        # given
        language_v2 = language_v1.copy()
        language_v2.update({
            'lang': 'emacslisp',
        })

        self.storage.content_language_add([language_v2], conflict_update=True)

        actual_languages = list(self.storage.content_language_get(
            [self.sha1_2]))

        # language did not change as the v2 was dropped.
        expected_languages_v2 = [{
            'id': self.sha1_2,
            'lang': 'emacslisp',
            'tool': self.tools['pygments']
        }]

        # language did change as the v2 was used to overwrite v1
        self.assertEqual(actual_languages, expected_languages_v2)

    def test_content_ctags_missing(self):
        # given
        tool_id = self.tools['universal-ctags']['id']

        ctags = [
            {
                'id': self.sha1_2,
                'indexer_configuration_id': tool_id,
            },
            {
                'id': self.sha1_1,
                'indexer_configuration_id': tool_id,
            }
        ]

        # when
        actual_missing = self.storage.content_ctags_missing(ctags)

        # then
        self.assertEqual(list(actual_missing), [
            self.sha1_2,
            self.sha1_1
        ])

        # given
        self.storage.content_ctags_add([
            {
                'id': self.sha1_2,
                'indexer_configuration_id': tool_id,
                'ctags': [{
                    'name': 'done',
                    'kind': 'variable',
                    'line': 119,
                    'lang': 'OCaml',
                }]
            },
        ])

        # when
        actual_missing = self.storage.content_ctags_missing(ctags)

        # then
        self.assertEqual(list(actual_missing), [self.sha1_1])

    def test_content_ctags_get(self):
        # given
        tool_id = self.tools['universal-ctags']['id']

        ctags = [self.sha1_2, self.sha1_1]

        ctag1 = {
            'id': self.sha1_2,
            'indexer_configuration_id': tool_id,
            'ctags': [
                {
                    'name': 'done',
                    'kind': 'variable',
                    'line': 100,
                    'lang': 'Python',
                },
                {
                    'name': 'main',
                    'kind': 'function',
                    'line': 119,
                    'lang': 'Python',
                }]
        }

        # when
        self.storage.content_ctags_add([ctag1])

        # then
        actual_ctags = list(self.storage.content_ctags_get(ctags))

        # then

        expected_ctags = [
            {
                'id': self.sha1_2,
                'tool': self.tools['universal-ctags'],
                'name': 'done',
                'kind': 'variable',
                'line': 100,
                'lang': 'Python',
            },
            {
                'id': self.sha1_2,
                'tool': self.tools['universal-ctags'],
                'name': 'main',
                'kind': 'function',
                'line': 119,
                'lang': 'Python',
            }
        ]

        self.assertEqual(actual_ctags, expected_ctags)

    def test_content_ctags_search(self):
        # 1. given
        tool = self.tools['universal-ctags']
        tool_id = tool['id']

        ctag1 = {
            'id': self.sha1_1,
            'indexer_configuration_id': tool_id,
            'ctags': [
                {
                    'name': 'hello',
                    'kind': 'function',
                    'line': 133,
                    'lang': 'Python',
                },
                {
                    'name': 'counter',
                    'kind': 'variable',
                    'line': 119,
                    'lang': 'Python',
                },
                {
                    'name': 'hello',
                    'kind': 'variable',
                    'line': 210,
                    'lang': 'Python',
                },
            ]
        }

        ctag2 = {
            'id': self.sha1_2,
            'indexer_configuration_id': tool_id,
            'ctags': [
                {
                    'name': 'hello',
                    'kind': 'variable',
                    'line': 100,
                    'lang': 'C',
                },
                {
                    'name': 'result',
                    'kind': 'variable',
                    'line': 120,
                    'lang': 'C',
                },
            ]
        }

        self.storage.content_ctags_add([ctag1, ctag2])

        # 1. when
        actual_ctags = list(self.storage.content_ctags_search('hello',
                                                              limit=1))

        # 1. then
        self.assertEqual(actual_ctags, [
            {
                'id': ctag1['id'],
                'tool': tool,
                'name': 'hello',
                'kind': 'function',
                'line': 133,
                'lang': 'Python',
            }
        ])

        # 2. when
        actual_ctags = list(self.storage.content_ctags_search(
            'hello',
            limit=1,
            last_sha1=ctag1['id']))

        # 2. then
        self.assertEqual(actual_ctags, [
            {
                'id': ctag2['id'],
                'tool': tool,
                'name': 'hello',
                'kind': 'variable',
                'line': 100,
                'lang': 'C',
            }
        ])

        # 3. when
        actual_ctags = list(self.storage.content_ctags_search('hello'))

        # 3. then
        self.assertEqual(actual_ctags, [
            {
                'id': ctag1['id'],
                'tool': tool,
                'name': 'hello',
                'kind': 'function',
                'line': 133,
                'lang': 'Python',
            },
            {
                'id': ctag1['id'],
                'tool': tool,
                'name': 'hello',
                'kind': 'variable',
                'line': 210,
                'lang': 'Python',
            },
            {
                'id': ctag2['id'],
                'tool': tool,
                'name': 'hello',
                'kind': 'variable',
                'line': 100,
                'lang': 'C',
            },
        ])

        # 4. when
        actual_ctags = list(self.storage.content_ctags_search('counter'))

        # then
        self.assertEqual(actual_ctags, [{
            'id': ctag1['id'],
            'tool': tool,
            'name': 'counter',
            'kind': 'variable',
            'line': 119,
            'lang': 'Python',
        }])

        # 5. when
        actual_ctags = list(self.storage.content_ctags_search('result',
                                                              limit=1))

        # then
        self.assertEqual(actual_ctags, [{
            'id': ctag2['id'],
            'tool': tool,
            'name': 'result',
            'kind': 'variable',
            'line': 120,
            'lang': 'C',
        }])

    def test_content_ctags_search_no_result(self):
        actual_ctags = list(self.storage.content_ctags_search('counter'))

        self.assertEqual(actual_ctags, [])

    def test_content_ctags_add__add_new_ctags_added(self):
        # given
        tool = self.tools['universal-ctags']
        tool_id = tool['id']

        ctag_v1 = {
            'id': self.sha1_2,
            'indexer_configuration_id': tool_id,
            'ctags': [{
                'name': 'done',
                'kind': 'variable',
                'line': 100,
                'lang': 'Scheme',
            }]
        }

        # given
        self.storage.content_ctags_add([ctag_v1])
        self.storage.content_ctags_add([ctag_v1])  # conflict does nothing

        # when
        actual_ctags = list(self.storage.content_ctags_get(
            [self.sha1_2]))

        # then
        expected_ctags = [{
            'id': self.sha1_2,
            'name': 'done',
            'kind': 'variable',
            'line': 100,
            'lang': 'Scheme',
            'tool': tool,
        }]

        self.assertEqual(actual_ctags, expected_ctags)

        # given
        ctag_v2 = ctag_v1.copy()
        ctag_v2.update({
            'ctags': [
                {
                    'name': 'defn',
                    'kind': 'function',
                    'line': 120,
                    'lang': 'Scheme',
                }
            ]
        })

        self.storage.content_ctags_add([ctag_v2])

        expected_ctags = [
            {
                'id': self.sha1_2,
                'name': 'done',
                'kind': 'variable',
                'line': 100,
                'lang': 'Scheme',
                'tool': tool,
            }, {
                'id': self.sha1_2,
                'name': 'defn',
                'kind': 'function',
                'line': 120,
                'lang': 'Scheme',
                'tool': tool,
            }
        ]

        actual_ctags = list(self.storage.content_ctags_get(
            [self.sha1_2]))

        self.assertEqual(actual_ctags, expected_ctags)

    def test_content_ctags_add__update_in_place(self):
        # given
        tool = self.tools['universal-ctags']
        tool_id = tool['id']

        ctag_v1 = {
            'id': self.sha1_2,
            'indexer_configuration_id': tool_id,
            'ctags': [{
                'name': 'done',
                'kind': 'variable',
                'line': 100,
                'lang': 'Scheme',
            }]
        }

        # given
        self.storage.content_ctags_add([ctag_v1])

        # when
        actual_ctags = list(self.storage.content_ctags_get(
            [self.sha1_2]))

        # then
        expected_ctags = [
            {
                'id': self.sha1_2,
                'name': 'done',
                'kind': 'variable',
                'line': 100,
                'lang': 'Scheme',
                'tool': tool
            }
        ]
        self.assertEqual(actual_ctags, expected_ctags)

        # given
        ctag_v2 = ctag_v1.copy()
        ctag_v2.update({
            'ctags': [
                {
                    'name': 'done',
                    'kind': 'variable',
                    'line': 100,
                    'lang': 'Scheme',
                },
                {
                    'name': 'defn',
                    'kind': 'function',
                    'line': 120,
                    'lang': 'Scheme',
                }
            ]
        })

        self.storage.content_ctags_add([ctag_v2], conflict_update=True)

        actual_ctags = list(self.storage.content_ctags_get(
            [self.sha1_2]))

        # ctag did change as the v2 was used to overwrite v1
        expected_ctags = [
            {
                'id': self.sha1_2,
                'name': 'done',
                'kind': 'variable',
                'line': 100,
                'lang': 'Scheme',
                'tool': tool,
            },
            {
                'id': self.sha1_2,
                'name': 'defn',
                'kind': 'function',
                'line': 120,
                'lang': 'Scheme',
                'tool': tool,
            }
        ]
        self.assertEqual(actual_ctags, expected_ctags)

    def test_content_fossology_license_get(self):
        # given
        tool = self.tools['nomos']
        tool_id = tool['id']

        license1 = {
            'id': self.sha1_1,
            'licenses': ['GPL-2.0+'],
            'indexer_configuration_id': tool_id,
        }

        # when
        self.storage.content_fossology_license_add([license1])

        # then
        actual_licenses = list(self.storage.content_fossology_license_get(
            [self.sha1_2, self.sha1_1]))

        expected_license = {
            self.sha1_1: [{
                'licenses': ['GPL-2.0+'],
                'tool': tool,
            }]
        }

        # then
        self.assertEqual(actual_licenses, [expected_license])

    def test_content_fossology_license_add__new_license_added(self):
        # given
        tool = self.tools['nomos']
        tool_id = tool['id']

        license_v1 = {
            'id': self.sha1_1,
            'licenses': ['Apache-2.0'],
            'indexer_configuration_id': tool_id,
        }

        # given
        self.storage.content_fossology_license_add([license_v1])
        # conflict does nothing
        self.storage.content_fossology_license_add([license_v1])

        # when
        actual_licenses = list(self.storage.content_fossology_license_get(
            [self.sha1_1]))

        # then
        expected_license = {
            self.sha1_1: [{
                'licenses': ['Apache-2.0'],
                'tool': tool,
            }]
        }
        self.assertEqual(actual_licenses, [expected_license])

        # given
        license_v2 = license_v1.copy()
        license_v2.update({
            'licenses': ['BSD-2-Clause'],
        })

        self.storage.content_fossology_license_add([license_v2])

        actual_licenses = list(self.storage.content_fossology_license_get(
            [self.sha1_1]))

        expected_license = {
            self.sha1_1: [{
                'licenses': ['Apache-2.0', 'BSD-2-Clause'],
                'tool': tool
            }]
        }

        # license did not change as the v2 was dropped.
        self.assertEqual(actual_licenses, [expected_license])

    def test_content_fossology_license_add__update_in_place_duplicate(self):
        # given
        tool = self.tools['nomos']
        tool_id = tool['id']

        license_v1 = {
            'id': self.sha1_1,
            'licenses': ['CECILL'],
            'indexer_configuration_id': tool_id,
        }

        # given
        self.storage.content_fossology_license_add([license_v1])
        # conflict does nothing
        self.storage.content_fossology_license_add([license_v1])

        # when
        actual_licenses = list(self.storage.content_fossology_license_get(
            [self.sha1_1]))

        # then
        expected_license = {
            self.sha1_1: [{
                'licenses': ['CECILL'],
                'tool': tool,
            }]
        }
        self.assertEqual(actual_licenses, [expected_license])

        # given
        license_v2 = license_v1.copy()
        license_v2.update({
            'licenses': ['CECILL-2.0']
        })

        self.storage.content_fossology_license_add([license_v2],
                                                   conflict_update=True)

        actual_licenses = list(self.storage.content_fossology_license_get(
            [self.sha1_1]))

        # license did change as the v2 was used to overwrite v1
        expected_license = {
            self.sha1_1: [{
                'licenses': ['CECILL-2.0'],
                'tool': tool,
            }]
        }
        self.assertEqual(actual_licenses, [expected_license])

    def test_content_metadata_missing(self):
        # given
        tool_id = self.tools['swh-metadata-translator']['id']

        metadata = [
            {
                'id': self.sha1_2,
                'indexer_configuration_id': tool_id,
            },
            {
                'id': self.sha1_1,
                'indexer_configuration_id': tool_id,
            }
        ]

        # when
        actual_missing = list(self.storage.content_metadata_missing(metadata))

        # then
        self.assertEqual(list(actual_missing), [
            self.sha1_2,
            self.sha1_1,
        ])

        # given
        self.storage.content_metadata_add([{
            'id': self.sha1_2,
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
            'indexer_configuration_id': tool_id
        }])

        # when
        actual_missing = list(self.storage.content_metadata_missing(metadata))

        # then
        self.assertEqual(actual_missing, [self.sha1_1])

    def test_content_metadata_get(self):
        # given
        tool_id = self.tools['swh-metadata-translator']['id']

        metadata1 = {
            'id': self.sha1_2,
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
            'indexer_configuration_id': tool_id,
        }

        # when
        self.storage.content_metadata_add([metadata1])

        # then
        actual_metadata = list(self.storage.content_metadata_get(
            [self.sha1_2, self.sha1_1]))

        expected_metadata = [{
            'id': self.sha1_2,
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
            'tool': self.tools['swh-metadata-translator']
        }]

        self.assertEqual(actual_metadata, expected_metadata)

    def test_content_metadata_add_drop_duplicate(self):
        # given
        tool_id = self.tools['swh-metadata-translator']['id']

        metadata_v1 = {
            'id': self.sha1_2,
            'translated_metadata': {
                'other': {},
                'name': 'test_metadata',
                'version': '0.0.1'
            },
            'indexer_configuration_id': tool_id,
        }

        # given
        self.storage.content_metadata_add([metadata_v1])

        # when
        actual_metadata = list(self.storage.content_metadata_get(
            [self.sha1_2]))

        expected_metadata_v1 = [{
            'id': self.sha1_2,
            'translated_metadata': {
                'other': {},
                'name': 'test_metadata',
                'version': '0.0.1'
            },
            'tool': self.tools['swh-metadata-translator']
        }]

        self.assertEqual(actual_metadata, expected_metadata_v1)

        # given
        metadata_v2 = metadata_v1.copy()
        metadata_v2.update({
            'translated_metadata': {
                'other': {},
                'name': 'test_drop_duplicated_metadata',
                'version': '0.0.1'
            },
        })

        self.storage.content_metadata_add([metadata_v2])

        # then
        actual_metadata = list(self.storage.content_metadata_get(
            [self.sha1_2]))

        # metadata did not change as the v2 was dropped.
        self.assertEqual(actual_metadata, expected_metadata_v1)

    def test_content_metadata_add_update_in_place_duplicate(self):
        # given
        tool_id = self.tools['swh-metadata-translator']['id']

        metadata_v1 = {
            'id': self.sha1_2,
            'translated_metadata': {
                'other': {},
                'name': 'test_metadata',
                'version': '0.0.1'
            },
            'indexer_configuration_id': tool_id,
        }

        # given
        self.storage.content_metadata_add([metadata_v1])

        # when
        actual_metadata = list(self.storage.content_metadata_get(
            [self.sha1_2]))

        # then
        expected_metadata_v1 = [{
            'id': self.sha1_2,
            'translated_metadata': {
                'other': {},
                'name': 'test_metadata',
                'version': '0.0.1'
            },
            'tool': self.tools['swh-metadata-translator']
        }]
        self.assertEqual(actual_metadata, expected_metadata_v1)

        # given
        metadata_v2 = metadata_v1.copy()
        metadata_v2.update({
            'translated_metadata': {
                'other': {},
                'name': 'test_update_duplicated_metadata',
                'version': '0.0.1'
            },
        })
        self.storage.content_metadata_add([metadata_v2], conflict_update=True)

        actual_metadata = list(self.storage.content_metadata_get(
            [self.sha1_2]))

        # language did not change as the v2 was dropped.
        expected_metadata_v2 = [{
            'id': self.sha1_2,
            'translated_metadata': {
                'other': {},
                'name': 'test_update_duplicated_metadata',
                'version': '0.0.1'
            },
            'tool': self.tools['swh-metadata-translator']
        }]

        # metadata did change as the v2 was used to overwrite v1
        self.assertEqual(actual_metadata, expected_metadata_v2)

    def test_revision_metadata_missing(self):
        # given
        tool_id = self.tools['swh-metadata-detector']['id']

        metadata = [
            {
                'id': self.revision_id_1,
                'indexer_configuration_id': tool_id,
            },
            {
                'id': self.revision_id_2,
                'indexer_configuration_id': tool_id,
            }
        ]

        # when
        actual_missing = list(self.storage.revision_metadata_missing(
                              metadata))

        # then
        self.assertEqual(list(actual_missing), [
            self.revision_id_1,
            self.revision_id_2,
        ])

        # given
        self.storage.revision_metadata_add([{
            'id': self.revision_id_1,
            'translated_metadata': {
                'developmentStatus': None,
                'version': None,
                'operatingSystem': None,
                'description': None,
                'keywords': None,
                'issueTracker': None,
                'name': None,
                'author': None,
                'relatedLink': None,
                'url': None,
                'license': None,
                'maintainer': None,
                'email': None,
                'softwareRequirements': None,
                'identifier': None
            },
            'indexer_configuration_id': tool_id
        }])

        # when
        actual_missing = list(self.storage.revision_metadata_missing(
                              metadata))

        # then
        self.assertEqual(actual_missing, [self.revision_id_2])

    def test_revision_metadata_get(self):
        # given
        tool_id = self.tools['swh-metadata-detector']['id']

        metadata_rev = {
            'id': self.revision_id_2,
            'translated_metadata': {
                'developmentStatus': None,
                'version': None,
                'operatingSystem': None,
                'description': None,
                'keywords': None,
                'issueTracker': None,
                'name': None,
                'author': None,
                'relatedLink': None,
                'url': None,
                'license': None,
                'maintainer': None,
                'email': None,
                'softwareRequirements': None,
                'identifier': None
            },
            'indexer_configuration_id': tool_id
        }

        # when
        self.storage.revision_metadata_add([metadata_rev])

        # then
        actual_metadata = list(self.storage.revision_metadata_get(
            [self.revision_id_2, self.revision_id_1]))

        expected_metadata = [{
            'id': self.revision_id_2,
            'translated_metadata': metadata_rev['translated_metadata'],
            'tool': self.tools['swh-metadata-detector']
        }]

        self.assertEqual(actual_metadata, expected_metadata)

    def test_revision_metadata_add_drop_duplicate(self):
        # given
        tool_id = self.tools['swh-metadata-detector']['id']

        metadata_v1 = {
            'id': self.revision_id_1,
            'translated_metadata':  {
                'developmentStatus': None,
                'version': None,
                'operatingSystem': None,
                'description': None,
                'keywords': None,
                'issueTracker': None,
                'name': None,
                'author': None,
                'relatedLink': None,
                'url': None,
                'license': None,
                'maintainer': None,
                'email': None,
                'softwareRequirements': None,
                'identifier': None
            },
            'indexer_configuration_id': tool_id,
        }

        # given
        self.storage.revision_metadata_add([metadata_v1])

        # when
        actual_metadata = list(self.storage.revision_metadata_get(
            [self.revision_id_1]))

        expected_metadata_v1 = [{
            'id': self.revision_id_1,
            'translated_metadata':  metadata_v1['translated_metadata'],
            'tool': self.tools['swh-metadata-detector']
        }]

        self.assertEqual(actual_metadata, expected_metadata_v1)

        # given
        metadata_v2 = metadata_v1.copy()
        metadata_v2.update({
            'translated_metadata':  {
                'name': 'test_metadata',
                'author': 'MG',
            },
        })

        self.storage.revision_metadata_add([metadata_v2])

        # then
        actual_metadata = list(self.storage.revision_metadata_get(
            [self.revision_id_1]))

        # metadata did not change as the v2 was dropped.
        self.assertEqual(actual_metadata, expected_metadata_v1)

    def test_revision_metadata_add_update_in_place_duplicate(self):
        # given
        tool_id = self.tools['swh-metadata-detector']['id']

        metadata_v1 = {
            'id': self.revision_id_2,
            'translated_metadata': {
                'developmentStatus': None,
                'version': None,
                'operatingSystem': None,
                'description': None,
                'keywords': None,
                'issueTracker': None,
                'name': None,
                'author': None,
                'relatedLink': None,
                'url': None,
                'license': None,
                'maintainer': None,
                'email': None,
                'softwareRequirements': None,
                'identifier': None
            },
            'indexer_configuration_id': tool_id,
        }

        # given
        self.storage.revision_metadata_add([metadata_v1])

        # when
        actual_metadata = list(self.storage.revision_metadata_get(
            [self.revision_id_2]))

        # then
        expected_metadata_v1 = [{
            'id': self.revision_id_2,
            'translated_metadata':  metadata_v1['translated_metadata'],
            'tool': self.tools['swh-metadata-detector']
        }]
        self.assertEqual(actual_metadata, expected_metadata_v1)

        # given
        metadata_v2 = metadata_v1.copy()
        metadata_v2.update({
            'translated_metadata':  {
                'name': 'test_update_duplicated_metadata',
                'author': 'MG'
            },
        })
        self.storage.revision_metadata_add([metadata_v2], conflict_update=True)

        actual_metadata = list(self.storage.revision_metadata_get(
            [self.revision_id_2]))

        expected_metadata_v2 = [{
            'id': self.revision_id_2,
            'translated_metadata': metadata_v2['translated_metadata'],
            'tool': self.tools['swh-metadata-detector']
        }]

        # metadata did change as the v2 was used to overwrite v1
        self.assertEqual(actual_metadata, expected_metadata_v2)

    def test_origin_intrinsic_metadata_get(self):
        # given
        tool_id = self.tools['swh-metadata-detector']['id']

        metadata = {
            'developmentStatus': None,
            'version': None,
            'operatingSystem': None,
            'description': None,
            'keywords': None,
            'issueTracker': None,
            'name': None,
            'author': None,
            'relatedLink': None,
            'url': None,
            'license': None,
            'maintainer': None,
            'email': None,
            'softwareRequirements': None,
            'identifier': None,
        }
        metadata_rev = {
            'id': self.revision_id_2,
            'translated_metadata': metadata,
            'indexer_configuration_id': tool_id,
        }
        metadata_origin = {
            'origin_id': self.origin_id_1,
            'metadata': metadata,
            'indexer_configuration_id': tool_id,
            'from_revision': self.revision_id_2,
            }

        # when
        self.storage.revision_metadata_add([metadata_rev])
        self.storage.origin_intrinsic_metadata_add([metadata_origin])

        # then
        actual_metadata = list(self.storage.origin_intrinsic_metadata_get(
            [self.origin_id_1, 42]))

        expected_metadata = [{
            'origin_id': self.origin_id_1,
            'metadata': metadata,
            'tool': self.tools['swh-metadata-detector'],
            'from_revision': self.revision_id_2,
        }]

        self.assertEqual(actual_metadata, expected_metadata)

    def test_origin_intrinsic_metadata_add_drop_duplicate(self):
        # given
        tool_id = self.tools['swh-metadata-detector']['id']

        metadata_v1 = {
            'developmentStatus': None,
            'version': None,
            'operatingSystem': None,
            'description': None,
            'keywords': None,
            'issueTracker': None,
            'name': None,
            'author': None,
            'relatedLink': None,
            'url': None,
            'license': None,
            'maintainer': None,
            'email': None,
            'softwareRequirements': None,
            'identifier': None
        }
        metadata_rev_v1 = {
            'id': self.revision_id_1,
            'translated_metadata': metadata_v1.copy(),
            'indexer_configuration_id': tool_id,
        }
        metadata_origin_v1 = {
            'origin_id': self.origin_id_1,
            'metadata': metadata_v1.copy(),
            'indexer_configuration_id': tool_id,
            'from_revision': self.revision_id_1,
        }

        # given
        self.storage.revision_metadata_add([metadata_rev_v1])
        self.storage.origin_intrinsic_metadata_add([metadata_origin_v1])

        # when
        actual_metadata = list(self.storage.origin_intrinsic_metadata_get(
            [self.origin_id_1, 42]))

        expected_metadata_v1 = [{
            'origin_id': self.origin_id_1,
            'metadata': metadata_v1,
            'tool': self.tools['swh-metadata-detector'],
            'from_revision': self.revision_id_1,
        }]

        self.assertEqual(actual_metadata, expected_metadata_v1)

        # given
        metadata_v2 = metadata_v1.copy()
        metadata_v2.update({
            'name': 'test_metadata',
            'author': 'MG',
        })
        metadata_rev_v2 = metadata_rev_v1.copy()
        metadata_origin_v2 = metadata_origin_v1.copy()
        metadata_rev_v2['translated_metadata'] = metadata_v2
        metadata_origin_v2['translated_metadata'] = metadata_v2

        self.storage.revision_metadata_add([metadata_rev_v2])
        self.storage.origin_intrinsic_metadata_add([metadata_origin_v2])

        # then
        actual_metadata = list(self.storage.origin_intrinsic_metadata_get(
            [self.origin_id_1]))

        # metadata did not change as the v2 was dropped.
        self.assertEqual(actual_metadata, expected_metadata_v1)

    def test_origin_intrinsic_metadata_add_update_in_place_duplicate(self):
        # given
        tool_id = self.tools['swh-metadata-detector']['id']

        metadata_v1 = {
            'developmentStatus': None,
            'version': None,
            'operatingSystem': None,
            'description': None,
            'keywords': None,
            'issueTracker': None,
            'name': None,
            'author': None,
            'relatedLink': None,
            'url': None,
            'license': None,
            'maintainer': None,
            'email': None,
            'softwareRequirements': None,
            'identifier': None
        }
        metadata_rev_v1 = {
            'id': self.revision_id_2,
            'translated_metadata': metadata_v1,
            'indexer_configuration_id': tool_id,
        }
        metadata_origin_v1 = {
            'origin_id': self.origin_id_1,
            'metadata': metadata_v1.copy(),
            'indexer_configuration_id': tool_id,
            'from_revision': self.revision_id_2,
        }

        # given
        self.storage.revision_metadata_add([metadata_rev_v1])
        self.storage.origin_intrinsic_metadata_add([metadata_origin_v1])

        # when
        actual_metadata = list(self.storage.origin_intrinsic_metadata_get(
            [self.origin_id_1]))

        # then
        expected_metadata_v1 = [{
            'origin_id': self.origin_id_1,
            'metadata': metadata_v1,
            'tool': self.tools['swh-metadata-detector'],
            'from_revision': self.revision_id_2,
        }]
        self.assertEqual(actual_metadata, expected_metadata_v1)

        # given
        metadata_v2 = metadata_v1.copy()
        metadata_v2.update({
            'name': 'test_update_duplicated_metadata',
            'author': 'MG',
        })
        metadata_rev_v2 = metadata_rev_v1.copy()
        metadata_origin_v2 = metadata_origin_v1.copy()
        metadata_rev_v2['translated_metadata'] = metadata_v2
        metadata_origin_v2['metadata'] = metadata_v2

        self.storage.revision_metadata_add([metadata_rev_v2],
                                           conflict_update=True)
        self.storage.origin_intrinsic_metadata_add([metadata_origin_v2],
                                                   conflict_update=True)

        actual_metadata = list(self.storage.origin_intrinsic_metadata_get(
            [self.origin_id_1]))

        expected_metadata_v2 = [{
            'origin_id': self.origin_id_1,
            'metadata': metadata_v2,
            'tool': self.tools['swh-metadata-detector'],
            'from_revision': self.revision_id_2,
        }]

        # metadata did change as the v2 was used to overwrite v1
        self.assertEqual(actual_metadata, expected_metadata_v2)

    def test_origin_intrinsic_metadata_search_fulltext(self):
        # given
        tool_id = self.tools['swh-metadata-detector']['id']

        metadata1 = {
            'author': 'John Doe',
        }
        metadata1_rev = {
            'id': self.revision_id_1,
            'translated_metadata': metadata1,
            'indexer_configuration_id': tool_id,
        }
        metadata1_origin = {
            'origin_id': self.origin_id_1,
            'metadata': metadata1,
            'indexer_configuration_id': tool_id,
            'from_revision': self.revision_id_1,
        }
        metadata2 = {
            'author': 'Jane Doe',
        }
        metadata2_rev = {
            'id': self.revision_id_2,
            'translated_metadata': metadata2,
            'indexer_configuration_id': tool_id,
        }
        metadata2_origin = {
            'origin_id': self.origin_id_2,
            'metadata': metadata2,
            'indexer_configuration_id': tool_id,
            'from_revision': self.revision_id_2,
        }

        # when
        self.storage.revision_metadata_add([metadata1_rev])
        self.storage.origin_intrinsic_metadata_add([metadata1_origin])
        self.storage.revision_metadata_add([metadata2_rev])
        self.storage.origin_intrinsic_metadata_add([metadata2_origin])

        # then
        search = self.storage.origin_intrinsic_metadata_search_fulltext
        self.assertCountEqual(
                [res['origin_id'] for res in search(['Doe'])],
                [self.origin_id_1, self.origin_id_2])
        self.assertEqual(
                [res['origin_id'] for res in search(['John', 'Doe'])],
                [self.origin_id_1])
        self.assertEqual(
                [res['origin_id'] for res in search(['John'])],
                [self.origin_id_1])
        self.assertEqual(
                [res['origin_id'] for res in search(['John', 'Jane'])],
                [])

    def test_origin_intrinsic_metadata_search_fulltext_rank(self):
        # given
        tool_id = self.tools['swh-metadata-detector']['id']

        # The following authors have "Random Person" to add some more content
        # to the JSON data, to work around normalization quirks when there
        # are few words (rank/(1+ln(nb_words)) is very sensitive to nb_words
        # for small values of nb_words).
        metadata1 = {
            'author': [
                'Random Person',
                'John Doe',
                'Jane Doe',
            ]
        }
        metadata1_rev = {
            'id': self.revision_id_1,
            'translated_metadata': metadata1,
            'indexer_configuration_id': tool_id,
        }
        metadata1_origin = {
            'origin_id': self.origin_id_1,
            'metadata': metadata1,
            'indexer_configuration_id': tool_id,
            'from_revision': self.revision_id_1,
        }
        metadata2 = {
            'author': [
                'Random Person',
                'Jane Doe',
            ]
        }
        metadata2_rev = {
            'id': self.revision_id_2,
            'translated_metadata': metadata2,
            'indexer_configuration_id': tool_id,
        }
        metadata2_origin = {
            'origin_id': self.origin_id_2,
            'metadata': metadata2,
            'indexer_configuration_id': tool_id,
            'from_revision': self.revision_id_2,
        }

        # when
        self.storage.revision_metadata_add([metadata1_rev])
        self.storage.origin_intrinsic_metadata_add([metadata1_origin])
        self.storage.revision_metadata_add([metadata2_rev])
        self.storage.origin_intrinsic_metadata_add([metadata2_origin])

        # then
        search = self.storage.origin_intrinsic_metadata_search_fulltext
        self.assertEqual(
                [res['origin_id'] for res in search(['Doe'])],
                [self.origin_id_1, self.origin_id_2])
        self.assertEqual(
                [res['origin_id'] for res in search(['Doe'], limit=1)],
                [self.origin_id_1])
        self.assertEqual(
                [res['origin_id'] for res in search(['John'])],
                [self.origin_id_1])
        self.assertEqual(
                [res['origin_id'] for res in search(['Jane'])],
                [self.origin_id_2, self.origin_id_1])
        self.assertEqual(
                [res['origin_id'] for res in search(['John', 'Jane'])],
                [self.origin_id_1])

    def test_indexer_configuration_add(self):
        tool = {
            'tool_name': 'some-unknown-tool',
            'tool_version': 'some-version',
            'tool_configuration': {"debian-package": "some-package"},
        }

        actual_tool = self.storage.indexer_configuration_get(tool)
        self.assertIsNone(actual_tool)  # does not exist

        # add it
        actual_tools = list(self.storage.indexer_configuration_add([tool]))

        self.assertEqual(len(actual_tools), 1)
        actual_tool = actual_tools[0]
        self.assertIsNotNone(actual_tool)  # now it exists
        new_id = actual_tool.pop('id')
        self.assertEqual(actual_tool, tool)

        actual_tools2 = list(self.storage.indexer_configuration_add([tool]))
        actual_tool2 = actual_tools2[0]
        self.assertIsNotNone(actual_tool2)  # now it exists
        new_id2 = actual_tool2.pop('id')

        self.assertEqual(new_id, new_id2)
        self.assertEqual(actual_tool, actual_tool2)

    def test_indexer_configuration_add_multiple(self):
        tool = {
            'tool_name': 'some-unknown-tool',
            'tool_version': 'some-version',
            'tool_configuration': {"debian-package": "some-package"},
        }

        actual_tools = list(self.storage.indexer_configuration_add([tool]))
        self.assertEqual(len(actual_tools), 1)

        new_tools = [tool, {
            'tool_name': 'yet-another-tool',
            'tool_version': 'version',
            'tool_configuration': {},
        }]

        actual_tools = list(self.storage.indexer_configuration_add(new_tools))
        self.assertEqual(len(actual_tools), 2)

        # order not guaranteed, so we iterate over results to check
        for tool in actual_tools:
            _id = tool.pop('id')
            self.assertIsNotNone(_id)
            self.assertIn(tool, new_tools)

    def test_indexer_configuration_get_missing(self):
        tool = {
            'tool_name': 'unknown-tool',
            'tool_version': '3.1.0rc2-31-ga2cbb8c',
            'tool_configuration': {"command_line": "nomossa <filepath>"},
        }

        actual_tool = self.storage.indexer_configuration_get(tool)

        self.assertIsNone(actual_tool)

    def test_indexer_configuration_get(self):
        tool = {
            'tool_name': 'nomos',
            'tool_version': '3.1.0rc2-31-ga2cbb8c',
            'tool_configuration': {"command_line": "nomossa <filepath>"},
        }

        self.storage.indexer_configuration_add([tool])
        actual_tool = self.storage.indexer_configuration_get(tool)

        expected_tool = tool.copy()
        del actual_tool['id']

        self.assertEqual(expected_tool, actual_tool)

    def test_indexer_configuration_metadata_get_missing_context(self):
        tool = {
            'tool_name': 'swh-metadata-translator',
            'tool_version': '0.0.1',
            'tool_configuration': {"context": "unknown-context"},
        }

        actual_tool = self.storage.indexer_configuration_get(tool)

        self.assertIsNone(actual_tool)

    def test_indexer_configuration_metadata_get(self):
        tool = {
            'tool_name': 'swh-metadata-translator',
            'tool_version': '0.0.1',
            'tool_configuration': {"type": "local", "context": "NpmMapping"},
        }

        self.storage.indexer_configuration_add([tool])
        actual_tool = self.storage.indexer_configuration_get(tool)

        expected_tool = tool.copy()
        expected_tool['id'] = actual_tool['id']

        self.assertEqual(expected_tool, actual_tool)

    @pytest.mark.property_based
    def test_generate_content_mimetype_get_range_limit_none(self):
        """mimetype_get_range call with wrong limit input should fail"""
        with self.assertRaises(ValueError) as e:
            self.storage.content_mimetype_get_range(
                start=None, end=None, indexer_configuration_id=None,
                limit=None)

        self.assertEqual(e.exception.args, (
            'Development error: limit should not be None',))

    @pytest.mark.property_based
    @given(gen_content_mimetypes(min_size=1, max_size=4))
    def test_generate_content_mimetype_get_range_no_limit(self, mimetypes):
        """mimetype_get_range returns mimetypes within range provided"""
        self.reset_storage_tables()
        # add mimetypes to storage
        self.storage.content_mimetype_add(mimetypes)

        # All ids from the db
        content_ids = sorted([c['id'] for c in mimetypes])

        start = content_ids[0]
        end = content_ids[-1]

        # retrieve mimetypes
        tool_id = mimetypes[0]['indexer_configuration_id']
        actual_result = self.storage.content_mimetype_get_range(
            start, end, indexer_configuration_id=tool_id)

        actual_ids = actual_result['ids']
        actual_next = actual_result['next']

        self.assertEqual(len(mimetypes), len(actual_ids))
        self.assertIsNone(actual_next)
        self.assertEqual(content_ids, actual_ids)

    @pytest.mark.property_based
    @given(gen_content_mimetypes(min_size=4, max_size=4))
    def test_generate_content_mimetype_get_range_limit(self, mimetypes):
        """mimetype_get_range paginates results if limit exceeded"""
        self.reset_storage_tables()

        # add mimetypes to storage
        self.storage.content_mimetype_add(mimetypes)

        # input the list of sha1s we want from storage
        content_ids = sorted([c['id'] for c in mimetypes])
        start = content_ids[0]
        end = content_ids[-1]

        # retrieve mimetypes limited to 3 results
        limited_results = len(mimetypes) - 1
        tool_id = mimetypes[0]['indexer_configuration_id']
        actual_result = self.storage.content_mimetype_get_range(
            start, end,
            indexer_configuration_id=tool_id, limit=limited_results)

        actual_ids = actual_result['ids']
        actual_next = actual_result['next']

        self.assertEqual(limited_results, len(actual_ids))
        self.assertIsNotNone(actual_next)
        self.assertEqual(actual_next, content_ids[-1])

        expected_mimetypes = content_ids[:-1]
        self.assertEqual(expected_mimetypes, actual_ids)

        # retrieve next part
        actual_results2 = self.storage.content_mimetype_get_range(
            start=end, end=end, indexer_configuration_id=tool_id)
        actual_ids2 = actual_results2['ids']
        actual_next2 = actual_results2['next']

        self.assertIsNone(actual_next2)
        expected_mimetypes2 = [content_ids[-1]]
        self.assertEqual(expected_mimetypes2, actual_ids2)

    @pytest.mark.property_based
    def test_generate_content_fossology_license_get_range_limit_none(self):
        """license_get_range call with wrong limit input should fail"""
        with self.assertRaises(ValueError) as e:
            self.storage.content_fossology_license_get_range(
                start=None, end=None, indexer_configuration_id=None,
                limit=None)

        self.assertEqual(e.exception.args, (
            'Development error: limit should not be None',))

    @pytest.mark.property_based
    def prepare_mimetypes_from(self, fossology_licenses):
        """Fossology license needs some consistent data in db to run.

        """
        mimetypes = []
        for c in fossology_licenses:
            mimetypes.append({
                'id': c['id'],
                'mimetype': 'text/plain',
                'encoding': 'utf-8',
                'indexer_configuration_id': c['indexer_configuration_id'],
            })
        return mimetypes

    @pytest.mark.property_based
    @given(gen_content_fossology_licenses(min_size=1, max_size=4))
    def test_generate_content_fossology_license_get_range_no_limit(
            self, fossology_licenses):
        """license_get_range returns licenses within range provided"""
        self.reset_storage_tables()
        # craft some consistent mimetypes
        mimetypes = self.prepare_mimetypes_from(fossology_licenses)

        self.storage.content_mimetype_add(mimetypes)
        # add fossology_licenses to storage
        self.storage.content_fossology_license_add(fossology_licenses)

        # All ids from the db
        content_ids = sorted([c['id'] for c in fossology_licenses])

        start = content_ids[0]
        end = content_ids[-1]

        # retrieve fossology_licenses
        tool_id = fossology_licenses[0]['indexer_configuration_id']
        actual_result = self.storage.content_fossology_license_get_range(
            start, end, indexer_configuration_id=tool_id)

        actual_ids = actual_result['ids']
        actual_next = actual_result['next']

        self.assertEqual(len(fossology_licenses), len(actual_ids))
        self.assertIsNone(actual_next)
        self.assertEqual(content_ids, actual_ids)

    @pytest.mark.property_based
    @given(gen_content_fossology_licenses(min_size=1, max_size=4),
           gen_content_mimetypes(min_size=1, max_size=1))
    def test_generate_content_fossology_license_get_range_no_limit_with_filter(
            self, fossology_licenses, mimetypes):
        """This filters non textual, then returns results within range"""
        self.reset_storage_tables()

        # craft some consistent mimetypes
        _mimetypes = self.prepare_mimetypes_from(fossology_licenses)
        # add binary mimetypes which will get filtered out in results
        for m in mimetypes:
            _mimetypes.append({
                'mimetype': 'binary',
                **m,
            })

        self.storage.content_mimetype_add(_mimetypes)
        # add fossology_licenses to storage
        self.storage.content_fossology_license_add(fossology_licenses)

        # All ids from the db
        content_ids = sorted([c['id'] for c in fossology_licenses])

        start = content_ids[0]
        end = content_ids[-1]

        # retrieve fossology_licenses
        tool_id = fossology_licenses[0]['indexer_configuration_id']
        actual_result = self.storage.content_fossology_license_get_range(
            start, end, indexer_configuration_id=tool_id)

        actual_ids = actual_result['ids']
        actual_next = actual_result['next']

        self.assertEqual(len(fossology_licenses), len(actual_ids))
        self.assertIsNone(actual_next)
        self.assertEqual(content_ids, actual_ids)

    @pytest.mark.property_based
    @given(gen_content_fossology_licenses(min_size=4, max_size=4))
    def test_generate_fossology_license_get_range_limit(
            self, fossology_licenses):
        """fossology_license_get_range paginates results if limit exceeded"""
        self.reset_storage_tables()
        # craft some consistent mimetypes
        mimetypes = self.prepare_mimetypes_from(fossology_licenses)

        # add fossology_licenses to storage
        self.storage.content_mimetype_add(mimetypes)
        self.storage.content_fossology_license_add(fossology_licenses)

        # input the list of sha1s we want from storage
        content_ids = sorted([c['id'] for c in fossology_licenses])
        start = content_ids[0]
        end = content_ids[-1]

        # retrieve fossology_licenses limited to 3 results
        limited_results = len(fossology_licenses) - 1
        tool_id = fossology_licenses[0]['indexer_configuration_id']
        actual_result = self.storage.content_fossology_license_get_range(
            start, end,
            indexer_configuration_id=tool_id, limit=limited_results)

        actual_ids = actual_result['ids']
        actual_next = actual_result['next']

        self.assertEqual(limited_results, len(actual_ids))
        self.assertIsNotNone(actual_next)
        self.assertEqual(actual_next, content_ids[-1])

        expected_fossology_licenses = content_ids[:-1]
        self.assertEqual(expected_fossology_licenses, actual_ids)

        # retrieve next part
        actual_results2 = self.storage.content_fossology_license_get_range(
            start=end, end=end, indexer_configuration_id=tool_id)
        actual_ids2 = actual_results2['ids']
        actual_next2 = actual_results2['next']

        self.assertIsNone(actual_next2)
        expected_fossology_licenses2 = [content_ids[-1]]
        self.assertEqual(expected_fossology_licenses2, actual_ids2)


@pytest.mark.db
class IndexerTestStorage(CommonTestStorage, BasePgTestStorage,
                         unittest.TestCase):
    """Running the tests locally.

    For the client api tests (remote storage), see
    `class`:swh.indexer.storage.test_api_client:TestRemoteStorage
    class.

    """
    pass
