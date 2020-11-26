# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import timedelta
import os
from typing import List, Tuple
from unittest.mock import patch

import pytest
import yaml

from swh.indexer.storage import get_indexer_storage
from swh.objstorage.factory import get_objstorage
from swh.storage import get_storage

from .utils import fill_obj_storage, fill_storage

TASK_NAMES: List[Tuple[str, str]] = [
    # (scheduler-task-type, task-class-test-name)
    ("index-revision-metadata", "revision_intrinsic_metadata"),
    ("index-origin-metadata", "origin_intrinsic_metadata"),
]


@pytest.fixture
def indexer_scheduler(swh_scheduler):
    # Insert the expected task types within the scheduler
    for task_name, task_class_name in TASK_NAMES:
        swh_scheduler.create_task_type(
            {
                "type": task_name,
                "description": f"The {task_class_name} indexer testing task",
                "backend_name": f"swh.indexer.tests.tasks.{task_class_name}",
                "default_interval": timedelta(days=1),
                "min_interval": timedelta(hours=6),
                "max_interval": timedelta(days=12),
                "num_retries": 3,
            }
        )
    return swh_scheduler


@pytest.fixture
def idx_storage():
    """An instance of in-memory indexer storage that gets injected into all
    indexers classes.

    """
    idx_storage = get_indexer_storage("memory")
    with patch("swh.indexer.storage.in_memory.IndexerStorage") as idx_storage_mock:
        idx_storage_mock.return_value = idx_storage
        yield idx_storage


@pytest.fixture
def storage():
    """An instance of in-memory storage that gets injected into all indexers
       classes.

    """
    storage = get_storage(cls="memory")
    fill_storage(storage)
    with patch("swh.storage.in_memory.InMemoryStorage") as storage_mock:
        storage_mock.return_value = storage
        yield storage


@pytest.fixture
def obj_storage():
    """An instance of in-memory objstorage that gets injected into all indexers
    classes.

    """
    objstorage = get_objstorage("memory")
    fill_obj_storage(objstorage)
    with patch.dict(
        "swh.objstorage.factory._STORAGE_CLASSES", {"memory": lambda: objstorage}
    ):
        yield objstorage


@pytest.fixture
def swh_indexer_config():
    return {
        "storage": {"cls": "memory"},
        "objstorage": {"cls": "memory"},
        "indexer_storage": {"cls": "memory"},
        "tools": {
            "name": "file",
            "version": "1:5.30-1+deb9u1",
            "configuration": {"type": "library", "debian-package": "python3-magic"},
        },
        "compute_checksums": ["blake2b512"],  # for rehash indexer
    }


@pytest.fixture
def swh_config(swh_indexer_config, monkeypatch, tmp_path):
    conffile = os.path.join(str(tmp_path), "indexer.yml")
    with open(conffile, "w") as f:
        f.write(yaml.dump(swh_indexer_config))
    monkeypatch.setenv("SWH_CONFIG_FILENAME", conffile)
    return conffile
