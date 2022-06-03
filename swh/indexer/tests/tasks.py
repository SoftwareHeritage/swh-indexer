from celery import current_app as app

from swh.indexer.metadata import DirectoryMetadataIndexer, OriginMetadataIndexer

from .test_metadata import ContentMetadataTestIndexer
from .utils import BASE_TEST_CONFIG


class DirectoryMetadataTestIndexer(DirectoryMetadataIndexer):
    """Specific indexer whose configuration is enough to satisfy the
    indexing tests.
    """

    ContentMetadataIndexer = ContentMetadataTestIndexer

    def parse_config_file(self, *args, **kwargs):
        return {
            **BASE_TEST_CONFIG,
            "tools": {
                "name": "swh-metadata-detector",
                "version": "0.0.2",
                "configuration": {"type": "local", "context": "NpmMapping"},
            },
        }


class OriginMetadataTestIndexer(OriginMetadataIndexer):
    def parse_config_file(self, *args, **kwargs):
        return {**BASE_TEST_CONFIG, "tools": []}

    def _prepare_sub_indexers(self):
        self.directory_metadata_indexer = DirectoryMetadataTestIndexer()


@app.task
def directory_intrinsic_metadata(*args, **kwargs):
    indexer = DirectoryMetadataTestIndexer()
    indexer.run(*args, **kwargs)
    print("REV RESULT=", indexer.results)


@app.task
def origin_intrinsic_metadata(*args, **kwargs):
    indexer = OriginMetadataTestIndexer()
    indexer.run(*args, **kwargs)
