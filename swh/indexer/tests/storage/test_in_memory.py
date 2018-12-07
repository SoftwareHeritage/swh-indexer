from unittest import TestCase

from .test_storage import CommonTestStorage


class IndexerTestInMemoryStorage(CommonTestStorage, TestCase):
    def setUp(self):
        self.storage_config = {
            'cls': 'memory',
            'args': {
            },
        }
        super().setUp()

    def reset_storage_tables(self):
        self.storage = self.storage.__class__()

    def test_check_config(self):
        pass
