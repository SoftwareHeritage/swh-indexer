# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.indexer.storage import converters


def test_ctags_to_db():
    input_ctag = {
        "id": b"some-id",
        "indexer_configuration_id": 100,
        "ctags": [
            {"name": "some-name", "kind": "some-kind", "line": 10, "lang": "Yaml",},
            {"name": "main", "kind": "function", "line": 12, "lang": "Yaml",},
        ],
    }

    expected_ctags = [
        {
            "id": b"some-id",
            "name": "some-name",
            "kind": "some-kind",
            "line": 10,
            "lang": "Yaml",
            "indexer_configuration_id": 100,
        },
        {
            "id": b"some-id",
            "name": "main",
            "kind": "function",
            "line": 12,
            "lang": "Yaml",
            "indexer_configuration_id": 100,
        },
    ]

    # when
    actual_ctags = list(converters.ctags_to_db(input_ctag))

    # then
    assert actual_ctags == expected_ctags


def test_db_to_ctags():
    input_ctags = {
        "id": b"some-id",
        "name": "some-name",
        "kind": "some-kind",
        "line": 10,
        "lang": "Yaml",
        "tool_id": 200,
        "tool_name": "some-toolname",
        "tool_version": "some-toolversion",
        "tool_configuration": {},
    }
    expected_ctags = {
        "id": b"some-id",
        "name": "some-name",
        "kind": "some-kind",
        "line": 10,
        "lang": "Yaml",
        "tool": {
            "id": 200,
            "name": "some-toolname",
            "version": "some-toolversion",
            "configuration": {},
        },
    }

    # when
    actual_ctags = converters.db_to_ctags(input_ctags)

    # then
    assert actual_ctags == expected_ctags


def test_db_to_mimetype():
    input_mimetype = {
        "id": b"some-id",
        "tool_id": 10,
        "tool_name": "some-toolname",
        "tool_version": "some-toolversion",
        "tool_configuration": {},
        "encoding": b"ascii",
        "mimetype": b"text/plain",
    }

    expected_mimetype = {
        "id": b"some-id",
        "encoding": b"ascii",
        "mimetype": b"text/plain",
        "tool": {
            "id": 10,
            "name": "some-toolname",
            "version": "some-toolversion",
            "configuration": {},
        },
    }

    actual_mimetype = converters.db_to_mimetype(input_mimetype)

    assert actual_mimetype == expected_mimetype


def test_db_to_language():
    input_language = {
        "id": b"some-id",
        "tool_id": 20,
        "tool_name": "some-toolname",
        "tool_version": "some-toolversion",
        "tool_configuration": {},
        "lang": b"css",
    }

    expected_language = {
        "id": b"some-id",
        "lang": b"css",
        "tool": {
            "id": 20,
            "name": "some-toolname",
            "version": "some-toolversion",
            "configuration": {},
        },
    }

    actual_language = converters.db_to_language(input_language)

    assert actual_language == expected_language


def test_db_to_fossology_license():
    input_license = {
        "id": b"some-id",
        "tool_id": 20,
        "tool_name": "nomossa",
        "tool_version": "5.22",
        "tool_configuration": {},
        "licenses": ["GPL2.0"],
    }

    expected_license = {
        "licenses": ["GPL2.0"],
        "tool": {"id": 20, "name": "nomossa", "version": "5.22", "configuration": {},},
    }

    actual_license = converters.db_to_fossology_license(input_license)

    assert actual_license == expected_license


def test_db_to_metadata():
    input_metadata = {
        "id": b"some-id",
        "tool_id": 20,
        "tool_name": "some-toolname",
        "tool_version": "some-toolversion",
        "tool_configuration": {},
        "metadata": b"metadata",
    }

    expected_metadata = {
        "id": b"some-id",
        "metadata": b"metadata",
        "tool": {
            "id": 20,
            "name": "some-toolname",
            "version": "some-toolversion",
            "configuration": {},
        },
    }

    actual_metadata = converters.db_to_metadata(input_metadata)

    assert actual_metadata == expected_metadata
