# Copyright (C) 2015-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import threading
import unittest

import pytest
from hypothesis import given

from swh.model.hashutil import hash_to_bytes

from swh.indexer.storage import get_indexer_storage, MAPPING_NAMES
from swh.core.db.tests.db_testing import SingleDbTestFixture
from swh.indexer.tests.storage.generate_data_test import (
    gen_content_mimetypes, gen_content_fossology_licenses
)
from swh.indexer.tests.storage import SQL_DIR
from swh.indexer.metadata_dictionary import MAPPINGS

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
        'tool_name': 'swh-metadata-detector2',
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


def gen_generic_endpoint_tests(endpoint_type, tool_name,
                               example_data1, example_data2):
    def rename(f):
        f.__name__ = 'test_' + endpoint_type + f.__name__
        return f

    def endpoint(self, endpoint_name):
        return getattr(self.storage, endpoint_type + '_' + endpoint_name)

    @rename
    def missing(self):
        # given
        tool_id = self.tools[tool_name]['id']

        query = [
            {
                'id': self.sha1_1,
                'indexer_configuration_id': tool_id,
            },
            {
                'id': self.sha1_2,
                'indexer_configuration_id': tool_id,
            }]

        # when
        actual_missing = endpoint(self, 'missing')(query)

        # then
        self.assertEqual(list(actual_missing), [
            self.sha1_1,
            self.sha1_2,
        ])

        # given
        endpoint(self, 'add')([{
            'id': self.sha1_2,
            **example_data1,
            'indexer_configuration_id': tool_id,
        }])

        # when
        actual_missing = endpoint(self, 'missing')(query)

        # then
        self.assertEqual(list(actual_missing), [self.sha1_1])

    @rename
    def add__drop_duplicate(self):
        # given
        tool_id = self.tools[tool_name]['id']

        data_v1 = {
            'id': self.sha1_2,
            **example_data1,
            'indexer_configuration_id': tool_id,
        }

        # given
        endpoint(self, 'add')([data_v1])

        # when
        actual_data = list(endpoint(self, 'get')([self.sha1_2]))

        # then
        expected_data_v1 = [{
            'id': self.sha1_2,
            **example_data1,
            'tool': self.tools[tool_name],
        }]
        self.assertEqual(actual_data, expected_data_v1)

        # given
        data_v2 = data_v1.copy()
        data_v2.update(example_data2)

        endpoint(self, 'add')([data_v2])

        actual_data = list(endpoint(self, 'get')([self.sha1_2]))

        # data did not change as the v2 was dropped.
        self.assertEqual(actual_data, expected_data_v1)

    @rename
    def add__update_in_place_duplicate(self):
        # given
        tool_id = self.tools[tool_name]['id']

        data_v1 = {
            'id': self.sha1_2,
            **example_data1,
            'indexer_configuration_id': tool_id,
        }

        # given
        endpoint(self, 'add')([data_v1])

        # when
        actual_data = list(endpoint(self, 'get')([self.sha1_2]))

        expected_data_v1 = [{
            'id': self.sha1_2,
            **example_data1,
            'tool': self.tools[tool_name],
        }]

        # then
        self.assertEqual(actual_data, expected_data_v1)

        # given
        data_v2 = data_v1.copy()
        data_v2.update(example_data2)

        endpoint(self, 'add')([data_v2], conflict_update=True)

        actual_data = list(endpoint(self, 'get')([self.sha1_2]))

        expected_data_v2 = [{
            'id': self.sha1_2,
            **example_data2,
            'tool': self.tools[tool_name],
        }]

        # data did change as the v2 was used to overwrite v1
        self.assertEqual(actual_data, expected_data_v2)

    @rename
    def add__update_in_place_deadlock(self):
        # given
        tool_id = self.tools[tool_name]['id']

        hashes = [
            hash_to_bytes(
                '34973274ccef6ab4dfaaf86599792fa9c3fe4{:03d}'.format(i))
            for i in range(1000)]

        data_v1 = [
            {
                'id': hash_,
                **example_data1,
                'indexer_configuration_id': tool_id,
            }
            for hash_ in hashes
        ]
        data_v2 = [
            {
                'id': hash_,
                **example_data2,
                'indexer_configuration_id': tool_id,
            }
            for hash_ in hashes
        ]

        # Remove one item from each, so that both queries have to succeed for
        # all items to be in the DB.
        data_v2a = data_v2[1:]
        data_v2b = list(reversed(data_v2[0:-1]))

        # given
        endpoint(self, 'add')(data_v1)

        # when
        actual_data = list(endpoint(self, 'get')(hashes))

        expected_data_v1 = [
            {
                'id': hash_,
                **example_data1,
                'tool': self.tools[tool_name],
            }
            for hash_ in hashes
        ]

        # then
        self.assertEqual(actual_data, expected_data_v1)

        # given
        def f1():
            endpoint(self, 'add')(data_v2a, conflict_update=True)

        def f2():
            endpoint(self, 'add')(data_v2b, conflict_update=True)

        t1 = threading.Thread(target=f1)
        t2 = threading.Thread(target=f2)
        t2.start()
        t1.start()

        t1.join()
        t2.join()

        actual_data = list(endpoint(self, 'get')(hashes))

        expected_data_v2 = [
            {
                'id': hash_,
                **example_data2,
                'tool': self.tools[tool_name],
            }
            for hash_ in hashes
        ]

        self.assertCountEqual(actual_data, expected_data_v2)

    def add__duplicate_twice(self):
        # given
        tool_id = self.tools[tool_name]['id']

        data_rev1 = {
            'id': self.revision_id_2,
            **example_data1,
            'indexer_configuration_id': tool_id
        }

        data_rev2 = {
            'id': self.revision_id_2,
            **example_data2,
            'indexer_configuration_id': tool_id
        }

        # when
        endpoint(self, 'add')([data_rev1])

        with self.assertRaises(ValueError):
            endpoint(self, 'add')(
                [data_rev2, data_rev2],
                conflict_update=True)

        # then
        actual_data = list(endpoint(self, 'get')(
            [self.revision_id_2, self.revision_id_1]))

        expected_data = [{
            'id': self.revision_id_2,
            **example_data1,
            'tool': self.tools[tool_name]
        }]
        self.assertEqual(actual_data, expected_data)

    @rename
    def get(self):
        # given
        tool_id = self.tools[tool_name]['id']

        query = [self.sha1_2, self.sha1_1]

        data1 = {
            'id': self.sha1_2,
            **example_data1,
            'indexer_configuration_id': tool_id,
        }

        # when
        endpoint(self, 'add')([data1])

        # then
        actual_data = list(endpoint(self, 'get')(query))

        # then
        expected_data = [{
            'id': self.sha1_2,
            **example_data1,
            'tool': self.tools[tool_name]
        }]

        self.assertEqual(actual_data, expected_data)

    @rename
    def delete(self):
        # given
        tool_id = self.tools[tool_name]['id']

        query = [self.sha1_2, self.sha1_1]

        data1 = {
            'id': self.sha1_2,
            **example_data1,
            'indexer_configuration_id': tool_id,
        }

        # when
        endpoint(self, 'add')([data1])
        endpoint(self, 'delete')([
            {
                'id': self.sha1_2,
                'indexer_configuration_id': tool_id,
            }
        ])

        # then
        actual_data = list(endpoint(self, 'get')(query))

        # then
        self.assertEqual(actual_data, [])

    @rename
    def delete_nonexisting(self):
        tool_id = self.tools[tool_name]['id']
        endpoint(self, 'delete')([
            {
                'id': self.sha1_2,
                'indexer_configuration_id': tool_id,
            }
        ])

    return (
        missing,
        add__drop_duplicate,
        add__update_in_place_duplicate,
        add__update_in_place_deadlock,
        add__duplicate_twice,
        get,
        delete,
        delete_nonexisting,
    )


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
        self.revision_id_3 = hash_to_bytes(
            '7026b7c1a2af56521e9587659012345678904320')
        self.origin_id_1 = 44434341
        self.origin_id_2 = 44434342
        self.origin_id_3 = 54974445

    def test_check_config(self):
        self.assertTrue(self.storage.check_config(check_write=True))
        self.assertTrue(self.storage.check_config(check_write=False))

    # generate content_mimetype tests
    (
        test_content_mimetype_missing,
        test_content_mimetype_add__drop_duplicate,
        test_content_mimetype_add__update_in_place_duplicate,
        test_content_mimetype_add__update_in_place_deadlock,
        test_content_mimetype_add__duplicate_twice,
        test_content_mimetype_get,
        _,  # content_mimetype_detete,
        _,  # content_mimetype_detete_nonexisting,
    ) = gen_generic_endpoint_tests(
        endpoint_type='content_mimetype',
        tool_name='file',
        example_data1={
            'mimetype': 'text/plain',
            'encoding': 'utf-8',
        },
        example_data2={
            'mimetype': 'text/html',
            'encoding': 'us-ascii',
        },
    )

    # content_language tests
    (
        test_content_language_missing,
        test_content_language_add__drop_duplicate,
        test_content_language_add__update_in_place_duplicate,
        test_content_language_add__update_in_place_deadlock,
        test_content_language_add__duplicate_twice,
        test_content_language_get,
        _,  # test_content_language_delete,
        _,  # test_content_language_delete_nonexisting,
    ) = gen_generic_endpoint_tests(
        endpoint_type='content_language',
        tool_name='pygments',
        example_data1={
            'lang': 'haskell',
        },
        example_data2={
            'lang': 'common-lisp',
        },
    )

    # content_ctags tests
    (
        test_content_ctags_missing,
        # the following tests are disabled because CTAGS behave differently
        _,  # test_content_ctags_add__drop_duplicate,
        _,  # test_content_ctags_add__update_in_place_duplicate,
        _,  # test_content_ctags_add__update_in_place_deadlock,
        _,  # test_content_ctags_add__duplicate_twice,
        _,  # test_content_ctags_get,
        _,  # test_content_ctags_delete,
        _,  # test_content_ctags_delete_nonexisting,
    ) = gen_generic_endpoint_tests(
        endpoint_type='content_ctags',
        tool_name='universal-ctags',
        example_data1={
            'ctags': [{
                'name': 'done',
                'kind': 'variable',
                'line': 119,
                'lang': 'OCaml',
            }]
        },
        example_data2={
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
        },
    )

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

    # content_fossology_license tests
    (
        _,  # The endpoint content_fossology_license_missing does not exist
        # the following tests are disabled because fossology_license tests
        # behave differently
        _,  # test_content_fossology_license_add__drop_duplicate,
        _,  # test_content_fossology_license_add__update_in_place_duplicate,
        _,  # test_content_fossology_license_add__update_in_place_deadlock,
        _,  # test_content_metadata_add__duplicate_twice,
        _,  # test_content_fossology_license_get,
        _,  # test_content_fossology_license_delete,
        _,  # test_content_fossology_license_delete_nonexisting,
    ) = gen_generic_endpoint_tests(
        endpoint_type='content_fossology_license',
        tool_name='nomos',
        example_data1={
            'licenses': ['Apache-2.0'],
        },
        example_data2={
            'licenses': ['BSD-2-Clause'],
        },
    )

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

    # content_metadata tests
    (
        test_content_metadata_missing,
        test_content_metadata_add__drop_duplicate,
        test_content_metadata_add__update_in_place_duplicate,
        test_content_metadata_add__update_in_place_deadlock,
        test_content_metadata_add__duplicate_twice,
        test_content_metadata_get,
        _,  # test_content_metadata_delete,
        _,  # test_content_metadata_delete_nonexisting,
    ) = gen_generic_endpoint_tests(
        endpoint_type='content_metadata',
        tool_name='swh-metadata-detector',
        example_data1={
            'metadata': {
                'other': {},
                'codeRepository': {
                    'type': 'git',
                    'url': 'https://github.com/moranegg/metadata_test'
                },
                'description': 'Simple package.json test for indexer',
                'name': 'test_metadata',
                'version': '0.0.1'
            },
        },
        example_data2={
            'metadata': {
                'other': {},
                'name': 'test_metadata',
                'version': '0.0.1'
            },
        },
    )

    # revision_intrinsic_metadata tests
    (
        test_revision_intrinsic_metadata_missing,
        test_revision_intrinsic_metadata_add__drop_duplicate,
        test_revision_intrinsic_metadata_add__update_in_place_duplicate,
        test_revision_intrinsic_metadata_add__update_in_place_deadlock,
        test_revision_intrinsic_metadata_add__duplicate_twice,
        test_revision_intrinsic_metadata_get,
        test_revision_intrinsic_metadata_delete,
        test_revision_intrinsic_metadata_delete_nonexisting,
    ) = gen_generic_endpoint_tests(
        endpoint_type='revision_intrinsic_metadata',
        tool_name='swh-metadata-detector',
        example_data1={
            'metadata': {
                'other': {},
                'codeRepository': {
                    'type': 'git',
                    'url': 'https://github.com/moranegg/metadata_test'
                },
                'description': 'Simple package.json test for indexer',
                'name': 'test_metadata',
                'version': '0.0.1'
            },
            'mappings': ['mapping1'],
        },
        example_data2={
            'metadata': {
                'other': {},
                'name': 'test_metadata',
                'version': '0.0.1'
            },
            'mappings': ['mapping2'],
        },
    )

    def test_origin_intrinsic_metadata_get(self):
        # given
        tool_id = self.tools['swh-metadata-detector']['id']

        metadata = {
            'version': None,
            'name': None,
        }
        metadata_rev = {
            'id': self.revision_id_2,
            'metadata': metadata,
            'mappings': ['mapping1'],
            'indexer_configuration_id': tool_id,
        }
        metadata_origin = {
            'id': self.origin_id_1,
            'origin_url': 'file:///dev/zero',
            'metadata': metadata,
            'indexer_configuration_id': tool_id,
            'mappings': ['mapping1'],
            'from_revision': self.revision_id_2,
            }

        # when
        self.storage.revision_intrinsic_metadata_add([metadata_rev])
        self.storage.origin_intrinsic_metadata_add([metadata_origin])

        # then
        actual_metadata = list(self.storage.origin_intrinsic_metadata_get(
            [self.origin_id_1, 42]))

        expected_metadata = [{
            'id': self.origin_id_1,
            'origin_url': 'file:///dev/zero',
            'metadata': metadata,
            'tool': self.tools['swh-metadata-detector'],
            'from_revision': self.revision_id_2,
            'mappings': ['mapping1'],
        }]

        self.assertEqual(actual_metadata, expected_metadata)

    def test_origin_intrinsic_metadata_delete(self):
        # given
        tool_id = self.tools['swh-metadata-detector']['id']

        metadata = {
            'version': None,
            'name': None,
        }
        metadata_rev = {
            'id': self.revision_id_2,
            'metadata': metadata,
            'mappings': ['mapping1'],
            'indexer_configuration_id': tool_id,
        }
        metadata_origin = {
            'id': self.origin_id_1,
            'origin_url': 'file:///dev/zero',
            'metadata': metadata,
            'indexer_configuration_id': tool_id,
            'mappings': ['mapping1'],
            'from_revision': self.revision_id_2,
            }
        metadata_origin2 = metadata_origin.copy()
        metadata_origin2['id'] = self.origin_id_2

        # when
        self.storage.revision_intrinsic_metadata_add([metadata_rev])
        self.storage.origin_intrinsic_metadata_add([
            metadata_origin, metadata_origin2])

        self.storage.origin_intrinsic_metadata_delete([
            {
                'id': self.origin_id_1,
                'indexer_configuration_id': tool_id
            }
        ])

        # then
        actual_metadata = list(self.storage.origin_intrinsic_metadata_get(
            [self.origin_id_1, self.origin_id_2, 42]))
        for item in actual_metadata:
            item['indexer_configuration_id'] = item.pop('tool')['id']
        self.assertEqual(actual_metadata, [metadata_origin2])

    def test_origin_intrinsic_metadata_delete_nonexisting(self):
        tool_id = self.tools['swh-metadata-detector']['id']
        self.storage.origin_intrinsic_metadata_delete([
            {
                'id': self.origin_id_1,
                'indexer_configuration_id': tool_id
            }
        ])

    def test_origin_intrinsic_metadata_add_drop_duplicate(self):
        # given
        tool_id = self.tools['swh-metadata-detector']['id']

        metadata_v1 = {
            'version': None,
            'name': None,
        }
        metadata_rev_v1 = {
            'id': self.revision_id_1,
            'origin_url': 'file:///dev/zero',
            'metadata': metadata_v1.copy(),
            'mappings': [],
            'indexer_configuration_id': tool_id,
        }
        metadata_origin_v1 = {
            'id': self.origin_id_1,
            'origin_url': 'file:///dev/zero',
            'metadata': metadata_v1.copy(),
            'indexer_configuration_id': tool_id,
            'mappings': [],
            'from_revision': self.revision_id_1,
        }

        # given
        self.storage.revision_intrinsic_metadata_add([metadata_rev_v1])
        self.storage.origin_intrinsic_metadata_add([metadata_origin_v1])

        # when
        actual_metadata = list(self.storage.origin_intrinsic_metadata_get(
            [self.origin_id_1, 42]))

        expected_metadata_v1 = [{
            'id': self.origin_id_1,
            'origin_url': 'file:///dev/zero',
            'metadata': metadata_v1,
            'tool': self.tools['swh-metadata-detector'],
            'from_revision': self.revision_id_1,
            'mappings': [],
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
        metadata_rev_v2['metadata'] = metadata_v2
        metadata_origin_v2['metadata'] = metadata_v2

        self.storage.revision_intrinsic_metadata_add([metadata_rev_v2])
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
            'version': None,
            'name': None,
        }
        metadata_rev_v1 = {
            'id': self.revision_id_2,
            'metadata': metadata_v1,
            'mappings': [],
            'indexer_configuration_id': tool_id,
        }
        metadata_origin_v1 = {
            'id': self.origin_id_1,
            'origin_url': 'file:///dev/zero',
            'metadata': metadata_v1.copy(),
            'indexer_configuration_id': tool_id,
            'mappings': [],
            'from_revision': self.revision_id_2,
        }

        # given
        self.storage.revision_intrinsic_metadata_add([metadata_rev_v1])
        self.storage.origin_intrinsic_metadata_add([metadata_origin_v1])

        # when
        actual_metadata = list(self.storage.origin_intrinsic_metadata_get(
            [self.origin_id_1]))

        # then
        expected_metadata_v1 = [{
            'id': self.origin_id_1,
            'origin_url': 'file:///dev/zero',
            'metadata': metadata_v1,
            'tool': self.tools['swh-metadata-detector'],
            'from_revision': self.revision_id_2,
            'mappings': [],
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
        metadata_rev_v2['metadata'] = metadata_v2
        metadata_origin_v2 = {
            'id': self.origin_id_1,
            'origin_url': 'file:///dev/null',
            'metadata': metadata_v2.copy(),
            'indexer_configuration_id': tool_id,
            'mappings': ['npm'],
            'from_revision': self.revision_id_1,
        }

        self.storage.revision_intrinsic_metadata_add(
                [metadata_rev_v2], conflict_update=True)
        self.storage.origin_intrinsic_metadata_add(
                [metadata_origin_v2], conflict_update=True)

        actual_metadata = list(self.storage.origin_intrinsic_metadata_get(
            [self.origin_id_1]))

        expected_metadata_v2 = [{
            'id': self.origin_id_1,
            'origin_url': 'file:///dev/null',
            'metadata': metadata_v2,
            'tool': self.tools['swh-metadata-detector'],
            'from_revision': self.revision_id_1,
            'mappings': ['npm'],
        }]

        # metadata did change as the v2 was used to overwrite v1
        self.assertEqual(actual_metadata, expected_metadata_v2)

    def test_origin_intrinsic_metadata_add__update_in_place_deadlock(self):
        # given
        tool_id = self.tools['swh-metadata-detector']['id']

        ids = list(range(10))

        example_data1 = {
            'metadata': {
                'version': None,
                'name': None,
            },
            'mappings': [],
        }
        example_data2 = {
            'metadata': {
                'version': 'v1.1.1',
                'name': 'foo',
            },
            'mappings': [],
        }

        metadata_rev_v1 = {
            'id': self.revision_id_2,
            'metadata': {
                'version': None,
                'name': None,
            },
            'mappings': [],
            'indexer_configuration_id': tool_id,
        }

        data_v1 = [
            {
                'id': id_,
                'origin_url': 'file:///tmp/origin%d' % id_,
                'from_revision': self.revision_id_2,
                **example_data1,
                'indexer_configuration_id': tool_id,
            }
            for id_ in ids
        ]
        data_v2 = [
            {
                'id': id_,
                'origin_url': 'file:///tmp/origin%d' % id_,
                'from_revision': self.revision_id_2,
                **example_data2,
                'indexer_configuration_id': tool_id,
            }
            for id_ in ids
        ]

        # Remove one item from each, so that both queries have to succeed for
        # all items to be in the DB.
        data_v2a = data_v2[1:]
        data_v2b = list(reversed(data_v2[0:-1]))

        # given
        self.storage.revision_intrinsic_metadata_add([metadata_rev_v1])
        self.storage.origin_intrinsic_metadata_add(data_v1)

        # when
        actual_data = list(self.storage.origin_intrinsic_metadata_get(ids))

        expected_data_v1 = [
            {
                'id': id_,
                'origin_url': 'file:///tmp/origin%d' % id_,
                'from_revision': self.revision_id_2,
                **example_data1,
                'tool': self.tools['swh-metadata-detector'],
            }
            for id_ in ids
        ]

        # then
        self.assertEqual(actual_data, expected_data_v1)

        # given
        def f1():
            self.storage.origin_intrinsic_metadata_add(
                data_v2a, conflict_update=True)

        def f2():
            self.storage.origin_intrinsic_metadata_add(
                data_v2b, conflict_update=True)

        t1 = threading.Thread(target=f1)
        t2 = threading.Thread(target=f2)
        t2.start()
        t1.start()

        t1.join()
        t2.join()

        actual_data = list(self.storage.origin_intrinsic_metadata_get(ids))

        expected_data_v2 = [
            {
                'id': id_,
                'origin_url': 'file:///tmp/origin%d' % id_,
                'from_revision': self.revision_id_2,
                **example_data2,
                'tool': self.tools['swh-metadata-detector'],
            }
            for id_ in ids
        ]

        self.maxDiff = None
        self.assertCountEqual(actual_data, expected_data_v2)

    def test_origin_intrinsic_metadata_add__duplicate_twice(self):
        # given
        tool_id = self.tools['swh-metadata-detector']['id']

        metadata = {
            'developmentStatus': None,
            'name': None,
        }
        metadata_rev = {
            'id': self.revision_id_2,
            'metadata': metadata,
            'mappings': ['mapping1'],
            'indexer_configuration_id': tool_id,
        }
        metadata_origin = {
            'id': self.origin_id_1,
            'origin_url': 'file:///dev/zero',
            'metadata': metadata,
            'indexer_configuration_id': tool_id,
            'mappings': ['mapping1'],
            'from_revision': self.revision_id_2,
            }

        # when
        self.storage.revision_intrinsic_metadata_add([metadata_rev])

        with self.assertRaises(ValueError):
            self.storage.origin_intrinsic_metadata_add([
                metadata_origin, metadata_origin])

    def test_origin_intrinsic_metadata_search_fulltext(self):
        # given
        tool_id = self.tools['swh-metadata-detector']['id']

        metadata1 = {
            'author': 'John Doe',
        }
        metadata1_rev = {
            'id': self.revision_id_1,
            'metadata': metadata1,
            'mappings': [],
            'indexer_configuration_id': tool_id,
        }
        metadata1_origin = {
            'id': self.origin_id_1,
            'origin_url': 'file:///dev/zero',
            'metadata': metadata1,
            'mappings': [],
            'indexer_configuration_id': tool_id,
            'from_revision': self.revision_id_1,
        }
        metadata2 = {
            'author': 'Jane Doe',
        }
        metadata2_rev = {
            'id': self.revision_id_2,
            'origin_url': 'file:///dev/zero',
            'metadata': metadata2,
            'mappings': [],
            'indexer_configuration_id': tool_id,
        }
        metadata2_origin = {
            'id': self.origin_id_2,
            'origin_url': 'file:///dev/zero',
            'metadata': metadata2,
            'mappings': [],
            'indexer_configuration_id': tool_id,
            'from_revision': self.revision_id_2,
        }

        # when
        self.storage.revision_intrinsic_metadata_add([metadata1_rev])
        self.storage.origin_intrinsic_metadata_add([metadata1_origin])
        self.storage.revision_intrinsic_metadata_add([metadata2_rev])
        self.storage.origin_intrinsic_metadata_add([metadata2_origin])

        # then
        search = self.storage.origin_intrinsic_metadata_search_fulltext
        self.assertCountEqual(
                [res['id'] for res in search(['Doe'])],
                [self.origin_id_1, self.origin_id_2])
        self.assertEqual(
                [res['id'] for res in search(['John', 'Doe'])],
                [self.origin_id_1])
        self.assertEqual(
                [res['id'] for res in search(['John'])],
                [self.origin_id_1])
        self.assertEqual(
                [res['id'] for res in search(['John', 'Jane'])],
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
            'metadata': metadata1,
            'mappings': [],
            'indexer_configuration_id': tool_id,
        }
        metadata1_origin = {
            'id': self.origin_id_1,
            'origin_url': 'file:///dev/zero',
            'metadata': metadata1,
            'mappings': [],
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
            'metadata': metadata2,
            'mappings': [],
            'indexer_configuration_id': tool_id,
        }
        metadata2_origin = {
            'id': self.origin_id_2,
            'origin_url': 'file:///dev/zero',
            'metadata': metadata2,
            'mappings': [],
            'indexer_configuration_id': tool_id,
            'from_revision': self.revision_id_2,
        }

        # when
        self.storage.revision_intrinsic_metadata_add([metadata1_rev])
        self.storage.origin_intrinsic_metadata_add([metadata1_origin])
        self.storage.revision_intrinsic_metadata_add([metadata2_rev])
        self.storage.origin_intrinsic_metadata_add([metadata2_origin])

        # then
        search = self.storage.origin_intrinsic_metadata_search_fulltext
        self.assertEqual(
                [res['id'] for res in search(['Doe'])],
                [self.origin_id_1, self.origin_id_2])
        self.assertEqual(
                [res['id'] for res in search(['Doe'], limit=1)],
                [self.origin_id_1])
        self.assertEqual(
                [res['id'] for res in search(['John'])],
                [self.origin_id_1])
        self.assertEqual(
                [res['id'] for res in search(['Jane'])],
                [self.origin_id_2, self.origin_id_1])
        self.assertEqual(
                [res['id'] for res in search(['John', 'Jane'])],
                [self.origin_id_1])

    def _fill_origin_intrinsic_metadata(self):
        tool1_id = self.tools['swh-metadata-detector']['id']
        tool2_id = self.tools['swh-metadata-detector2']['id']

        metadata1 = {
            '@context': 'foo',
            'author': 'John Doe',
        }
        metadata1_rev = {
            'id': self.revision_id_1,
            'metadata': metadata1,
            'mappings': ['npm'],
            'indexer_configuration_id': tool1_id,
        }
        metadata1_origin = {
            'id': self.origin_id_1,
            'origin_url': 'file:///dev/zero',
            'metadata': metadata1,
            'mappings': ['npm'],
            'indexer_configuration_id': tool1_id,
            'from_revision': self.revision_id_1,
        }
        metadata2 = {
            '@context': 'foo',
            'author': 'Jane Doe',
        }
        metadata2_rev = {
            'id': self.revision_id_2,
            'metadata': metadata2,
            'mappings': ['npm', 'gemspec'],
            'indexer_configuration_id': tool2_id,
        }
        metadata2_origin = {
            'id': self.origin_id_2,
            'origin_url': 'file:///dev/zero',
            'metadata': metadata2,
            'mappings': ['npm', 'gemspec'],
            'indexer_configuration_id': tool2_id,
            'from_revision': self.revision_id_2,
        }
        metadata3 = {
            '@context': 'foo',
        }
        metadata3_rev = {
            'id': self.revision_id_3,
            'metadata': metadata3,
            'mappings': ['npm', 'gemspec'],
            'indexer_configuration_id': tool2_id,
        }
        metadata3_origin = {
            'id': self.origin_id_3,
            'origin_url': 'file:///dev/zero',
            'metadata': metadata3,
            'mappings': ['pkg-info'],
            'indexer_configuration_id': tool2_id,
            'from_revision': self.revision_id_3,
        }

        self.storage.revision_intrinsic_metadata_add([metadata1_rev])
        self.storage.origin_intrinsic_metadata_add([metadata1_origin])
        self.storage.revision_intrinsic_metadata_add([metadata2_rev])
        self.storage.origin_intrinsic_metadata_add([metadata2_origin])
        self.storage.revision_intrinsic_metadata_add([metadata3_rev])
        self.storage.origin_intrinsic_metadata_add([metadata3_origin])

    def test_origin_intrinsic_metadata_search_by_producer(self):
        self._fill_origin_intrinsic_metadata()
        tool1 = self.tools['swh-metadata-detector']
        tool2 = self.tools['swh-metadata-detector2']
        endpoint = self.storage.origin_intrinsic_metadata_search_by_producer

        # test pagination
        self.assertCountEqual(
            endpoint(ids_only=True),
            [self.origin_id_1, self.origin_id_2, self.origin_id_3])
        self.assertCountEqual(
            endpoint(start=0, ids_only=True),
            [self.origin_id_1, self.origin_id_2, self.origin_id_3])
        self.assertCountEqual(
            endpoint(start=0, limit=2, ids_only=True),
            [self.origin_id_1, self.origin_id_2])
        self.assertCountEqual(
            endpoint(start=self.origin_id_1+1, ids_only=True),
            [self.origin_id_2, self.origin_id_3])
        self.assertCountEqual(
            endpoint(start=self.origin_id_1+1, end=self.origin_id_3-1,
                     ids_only=True),
            [self.origin_id_2])

        # test mappings filtering
        self.assertCountEqual(
            endpoint(mappings=['npm'], ids_only=True),
            [self.origin_id_1, self.origin_id_2])
        self.assertCountEqual(
            endpoint(mappings=['npm', 'gemspec'], ids_only=True),
            [self.origin_id_1, self.origin_id_2])
        self.assertCountEqual(
            endpoint(mappings=['gemspec'], ids_only=True),
            [self.origin_id_2])
        self.assertCountEqual(
            endpoint(mappings=['pkg-info'], ids_only=True),
            [self.origin_id_3])
        self.assertCountEqual(
            endpoint(mappings=['foobar'], ids_only=True),
            [])

        # test pagination + mappings
        self.assertCountEqual(
            endpoint(mappings=['npm'], limit=1, ids_only=True),
            [self.origin_id_1])

        # test tool filtering
        self.assertCountEqual(
            endpoint(tool_ids=[tool1['id']], ids_only=True),
            [self.origin_id_1])
        self.assertCountEqual(
            endpoint(tool_ids=[tool2['id']], ids_only=True),
            [self.origin_id_2, self.origin_id_3])
        self.assertCountEqual(
            endpoint(tool_ids=[tool1['id'], tool2['id']], ids_only=True),
            [self.origin_id_1, self.origin_id_2, self.origin_id_3])

        # test ids_only=False
        self.assertEqual(list(endpoint(mappings=['gemspec'])), [{
            'id': self.origin_id_2,
            'origin_url': 'file:///dev/zero',
            'metadata': {
                '@context': 'foo',
                'author': 'Jane Doe',
            },
            'mappings': ['npm', 'gemspec'],
            'tool': tool2,
            'from_revision': self.revision_id_2,
        }])

    def test_origin_intrinsic_metadata_stats(self):
        self._fill_origin_intrinsic_metadata()

        result = self.storage.origin_intrinsic_metadata_stats()
        self.assertEqual(result, {
            'per_mapping': {
                'gemspec': 1,
                'npm': 2,
                'pkg-info': 1,
                'codemeta': 0,
                'maven': 0,
            },
            'total': 3,
            'non_empty': 2,
        })

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


def test_mapping_names():
    assert set(MAPPING_NAMES) == {m.name for m in MAPPINGS.values()}
