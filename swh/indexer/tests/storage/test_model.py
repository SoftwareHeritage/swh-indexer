# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.indexer.storage.model import BaseRow, ContentLicenseRow


def test_unique_key():
    assert BaseRow(id=12, indexer_configuration_id=34).unique_key() == {
        "id": 12,
        "indexer_configuration_id": 34,
    }

    assert BaseRow(id=12, tool={"id": 34, "name": "foo"}).unique_key() == {
        "id": 12,
        "indexer_configuration_id": 34,
    }

    assert ContentLicenseRow(
        id=12, indexer_configuration_id=34, license="BSD"
    ).unique_key() == {"id": 12, "indexer_configuration_id": 34, "license": "BSD"}

    assert ContentLicenseRow(
        id=12, tool={"id": 34, "name": "foo"}, license="BSD"
    ).unique_key() == {"id": 12, "indexer_configuration_id": 34, "license": "BSD"}
