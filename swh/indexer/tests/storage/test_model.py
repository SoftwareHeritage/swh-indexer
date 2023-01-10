# Copyright (C) 2020-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.indexer.storage.model import BaseRow, ContentLicenseRow


def test_unique_key__no_tool_dict():
    with pytest.raises(ValueError, match="indexer_configuration_id"):
        BaseRow(id=12, indexer_configuration_id=34).unique_key()
    with pytest.raises(ValueError, match="indexer_configuration_id"):
        ContentLicenseRow(
            id=12, indexer_configuration_id=34, license="BSD"
        ).unique_key()


def test_unique_key():
    assert BaseRow(
        id=12, tool={"id": 34, "name": "foo", "version": "1.2.3", "configuration": {}}
    ).unique_key() == {
        "id": 12,
        "tool_name": "foo",
        "tool_version": "1.2.3",
        "tool_configuration": "{}",
    }

    assert ContentLicenseRow(
        id=12,
        tool={"id": 34, "name": "foo", "version": "1.2.3", "configuration": {}},
        license="BSD",
    ).unique_key() == {
        "id": 12,
        "license": "BSD",
        "tool_name": "foo",
        "tool_version": "1.2.3",
        "tool_configuration": "{}",
    }

    assert ContentLicenseRow(
        id=12,
        tool={
            "id": 34,
            "name": "foo",
            "version": "1.2.3",
            "configuration": {"foo": 1, "bar": 2},
        },
        license="BSD",
    ).unique_key() == {
        "id": 12,
        "license": "BSD",
        "tool_name": "foo",
        "tool_version": "1.2.3",
        "tool_configuration": '{"bar": 2, "foo": 1}',
    }
