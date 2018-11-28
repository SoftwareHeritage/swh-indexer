# Copyright (C) 2015-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from swh.core.tests.server_testing import ServerTestFixture
from swh.indexer.storage import INDEXER_CFG_KEY
from swh.indexer.storage.api.client import RemoteStorage
from swh.indexer.storage.api.server import app

from .test_storage import CommonTestStorage, BasePgTestStorage


class TestRemoteStorage(CommonTestStorage, ServerTestFixture,
                        BasePgTestStorage, unittest.TestCase):
    """Test the indexer's remote storage API.

    This class doesn't define any tests as we want identical
    functionality between local and remote storage. All the tests are
    therefore defined in
    `class`:swh.indexer.storage.test_storage.CommonTestStorage.

    """

    def setUp(self):
        self.config = {
            INDEXER_CFG_KEY: {
                'cls': 'local',
                'args': {
                    'db': 'dbname=%s' % self.TEST_DB_NAME,
                }
            }
        }
        self.app = app
        super().setUp()
        self.storage = RemoteStorage(self.url())
