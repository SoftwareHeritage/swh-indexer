# Copyright (C) 2019-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from functools import partial
import os
from unittest.mock import patch

import pytest
from pytest_postgresql import factories
import yaml

from swh.core.db.db_utils import initialize_database_for_module
from swh.indexer.storage import IndexerStorage, get_indexer_storage
from swh.objstorage.factory import get_objstorage
from swh.storage import get_storage

from .utils import fill_obj_storage, fill_storage

idx_postgresql_proc = factories.postgresql_proc(
    load=[
        partial(
            initialize_database_for_module,
            modname="indexer.storage",
            version=IndexerStorage.current_version,
        )
    ],
)

idx_storage_postgresql = factories.postgresql("idx_postgresql_proc")


@pytest.fixture
def idx_storage_backend_config(idx_storage_postgresql):
    """Basic pg storage configuration with no journal collaborator for the indexer
    storage (to avoid pulling optional dependency on clients of this fixture)

    """
    return {
        "cls": "postgresql",
        "db": idx_storage_postgresql.info.dsn,
    }


@pytest.fixture
def swh_indexer_config(
    swh_storage_backend_config,
    idx_storage_backend_config,
):
    return {
        "storage": swh_storage_backend_config,
        "objstorage": {"cls": "memory"},
        "indexer_storage": idx_storage_backend_config,
        "tools": {
            "name": "file",
            "version": "1:5.30-1+deb9u1",
            "configuration": {"type": "library", "debian-package": "python3-magic"},
        },
        "compute_checksums": ["blake2b512"],  # for rehash indexer
    }


@pytest.fixture
def idx_storage(swh_indexer_config):
    """An instance of in-memory indexer storage that gets injected into all
    indexers classes.

    """
    idx_storage_config = swh_indexer_config["indexer_storage"]
    return get_indexer_storage(**idx_storage_config)


@pytest.fixture
def storage(swh_indexer_config):
    """An instance of in-memory storage that gets injected into all indexers
    classes.

    """
    storage = get_storage(**swh_indexer_config["storage"])
    fill_storage(storage)
    return storage


@pytest.fixture
def obj_storage(swh_indexer_config):
    """An instance of in-memory objstorage that gets injected into all indexers
    classes.

    """
    objstorage = get_objstorage(**swh_indexer_config["objstorage"])
    fill_obj_storage(objstorage)
    with patch("swh.indexer.indexer.get_objstorage", return_value=objstorage):
        yield objstorage


@pytest.fixture
def swh_config(swh_indexer_config, monkeypatch, tmp_path):
    conffile = os.path.join(str(tmp_path), "indexer.yml")
    with open(conffile, "w") as f:
        f.write(yaml.dump(swh_indexer_config))
    monkeypatch.setenv("SWH_CONFIG_FILENAME", conffile)
    return conffile
