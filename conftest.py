# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from hypothesis import settings
import pytest

# define tests profile. Full documentation is at:
# https://hypothesis.readthedocs.io/en/latest/settings.html#settings-profiles
settings.register_profile("fast", max_examples=5, deadline=5000)
settings.register_profile("slow", max_examples=20, deadline=5000)

# Ignore the following modules because wsgi module fails as no
# configuration file is found (--doctest-modules forces the module
# loading)
collect_ignore = ["swh/indexer/storage/api/wsgi.py"]

# we use the various swh fixtures
pytest_plugins = [
    "swh.scheduler.pytest_plugin",
    "swh.storage.pytest_plugin",
    "swh.core.db.pytest_plugin",
]


@pytest.fixture(scope="session")
def swh_scheduler_celery_includes(swh_scheduler_celery_includes):
    return swh_scheduler_celery_includes + [
        "swh.indexer.tasks",
    ]
