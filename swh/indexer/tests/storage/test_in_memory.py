from unittest import TestCase
import pytest

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

    @pytest.mark.xfail
    def test_check_config(self):
        pass

    @pytest.mark.xfail
    def test_origin_intrinsic_metadata_get(self):
        pass

    @pytest.mark.xfail
    def test_origin_intrinsic_metadata_add_drop_duplicate(self):
        pass

    @pytest.mark.xfail
    def test_origin_intrinsic_metadata_add_update_in_place_duplicate(self):
        pass

    @pytest.mark.xfail
    def test_origin_intrinsic_metadata_search_fulltext(self):
        pass

    @pytest.mark.xfail
    def test_origin_intrinsic_metadata_search_fulltext_rank(self):
        pass

    @pytest.mark.xfail
    def test_indexer_configuration_metadata_get_missing_context(self):
        pass

    @pytest.mark.xfail
    def test_indexer_configuration_metadata_get(self):
        pass
