# Copyright (C) 2015-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.indexer.storage import converters


def test_db_to_mimetype() -> None:
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


def test_db_to_fossology_license() -> None:
    input_license = {
        "id": b"some-id",
        "tool_id": 20,
        "tool_name": "nomossa",
        "tool_version": "5.22",
        "tool_configuration": {},
        "license": "GPL2.0",
    }

    expected_license = {
        "id": b"some-id",
        "license": "GPL2.0",
        "tool": {
            "id": 20,
            "name": "nomossa",
            "version": "5.22",
            "configuration": {},
        },
    }

    actual_license = converters.db_to_fossology_license(input_license)

    assert actual_license == expected_license


def test_db_to_metadata() -> None:
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
