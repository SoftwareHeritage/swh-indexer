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

    @pytest.mark.xfail
    def test_check_config(self):
        pass

    @pytest.mark.xfail
    def test_content_mimetype_missing(self):
        pass

    @pytest.mark.xfail
    def test_content_mimetype_add__drop_duplicate(self):
        pass

    @pytest.mark.xfail
    def test_content_mimetype_add__update_in_place_duplicate(self):
        pass

    @pytest.mark.xfail
    def test_content_mimetype_get(self):
        pass

    @pytest.mark.xfail
    def test_content_language_missing(self):
        pass

    @pytest.mark.xfail
    def test_content_language_get(self):
        pass

    @pytest.mark.xfail
    def test_content_language_add__drop_duplicate(self):
        pass

    @pytest.mark.xfail
    def test_content_language_add__update_in_place_duplicate(self):
        pass

    @pytest.mark.xfail
    def test_content_ctags_missing(self):
        pass

    @pytest.mark.xfail
    def test_content_ctags_get(self):
        pass

    @pytest.mark.xfail
    def test_content_ctags_search(self):
        pass

    @pytest.mark.xfail
    def test_content_ctags_search_no_result(self):
        pass

    @pytest.mark.xfail
    def test_content_ctags_add__add_new_ctags_added(self):
        pass

    @pytest.mark.xfail
    def test_content_ctags_add__update_in_place(self):
        pass

    @pytest.mark.xfail
    def test_content_fossology_license_get(self):
        pass

    @pytest.mark.xfail
    def test_content_fossology_license_add__new_license_added(self):
        pass

    @pytest.mark.xfail
    def test_content_fossology_license_add__update_in_place_duplicate(self):
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

    @pytest.mark.xfail
    def test_generate_content_mimetype_get_range_limit_none(self):
        pass

    @pytest.mark.xfail
    def test_generate_content_mimetype_get_range_no_limit(self, mimetypes):
        pass

    @pytest.mark.xfail
    def test_generate_content_mimetype_get_range_limit(self, mimetypes):
        pass

    @pytest.mark.xfail
    def test_generate_content_fossology_license_get_range_limit_none(self):
        pass

    @pytest.mark.xfail
    def test_generate_content_fossology_license_get_range_no_limit(self):
        pass

    @pytest.mark.xfail
    def test_generate_content_fossology_license_get_range_no_limit_with_filter(
            self):
        pass

    @pytest.mark.xfail
    def test_generate_fossology_license_get_range_limit(self):
        pass
