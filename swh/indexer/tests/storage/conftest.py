# Copyright (C) 2015-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from os.path import join
import pytest

from . import SQL_DIR
from swh.storage.pytest_plugin import postgresql_fact
from swh.indexer.storage import get_indexer_storage
from swh.model.hashutil import hash_to_bytes
from .generate_data_test import MIMETYPE_OBJECTS, FOSSOLOGY_LICENSES, TOOLS


DUMP_FILES = join(SQL_DIR, "*.sql")


class DataObj(dict):
    def __getattr__(self, key):
        return self.__getitem__(key)

    def __setattr__(self, key, value):
        return self.__setitem__(key, value)


@pytest.fixture
def swh_indexer_storage_with_data(swh_indexer_storage):
    data = DataObj()
    tools = {
        tool["tool_name"]: {
            "id": tool["id"],
            "name": tool["tool_name"],
            "version": tool["tool_version"],
            "configuration": tool["tool_configuration"],
        }
        for tool in swh_indexer_storage.indexer_configuration_add(TOOLS)
    }
    data.tools = tools
    data.sha1_1 = hash_to_bytes("34973274ccef6ab4dfaaf86599792fa9c3fe4689")
    data.sha1_2 = hash_to_bytes("61c2b3a30496d329e21af70dd2d7e097046d07b7")
    data.revision_id_1 = hash_to_bytes("7026b7c1a2af56521e951c01ed20f255fa054238")
    data.revision_id_2 = hash_to_bytes("7026b7c1a2af56521e9587659012345678904321")
    data.revision_id_3 = hash_to_bytes("7026b7c1a2af56521e9587659012345678904320")
    data.origin_url_1 = "file:///dev/0/zero"  # 44434341
    data.origin_url_2 = "file:///dev/1/one"  # 44434342
    data.origin_url_3 = "file:///dev/2/two"  # 54974445
    data.mimetypes = [
        {**mimetype_obj, "indexer_configuration_id": tools["file"]["id"]}
        for mimetype_obj in MIMETYPE_OBJECTS
    ]
    swh_indexer_storage.content_mimetype_add(data.mimetypes)
    data.fossology_licenses = [
        {**fossology_obj, "indexer_configuration_id": tools["nomos"]["id"]}
        for fossology_obj in FOSSOLOGY_LICENSES
    ]
    swh_indexer_storage._test_data = data

    return (swh_indexer_storage, data)


swh_indexer_storage_postgresql = postgresql_fact(
    "postgresql_proc", dump_files=DUMP_FILES
)


@pytest.fixture
def swh_indexer_storage(swh_indexer_storage_postgresql):
    storage_config = {
        "cls": "local",
        "args": {"db": swh_indexer_storage_postgresql.dsn,},
    }
    return get_indexer_storage(**storage_config)
