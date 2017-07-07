# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from .indexer import BaseIndexer
from swh.indexer.metadata_dictionary import compute_metadata


class ContentMetadataIndexer(BaseIndexer):
    """Indexer in charge of:
    - filtering out content already indexed
    - reading content from objstorage with the content's id sha1
    - computing translated_metadata by given context
    - using the MetadataDict and a tool for each context
    - store result in storage
    """
    CONFIG_BASE_FILENAME = 'indexer/metadata'

    ADDITIONAL_CONFIG = {
        'tools': ('dict', {
            'name': 'swh-metadata-translator',
            'version': '0.0.1',
            'configuration': {
                'type': 'local',
                'context': 'npm'
            },
        }),
    }

    def prepare(self):
        super().prepare()

    def filter_contents(self, sha1s):
        """Filter out known sha1s and return only missing ones.
        """
        yield from self.storage.content_metadata_missing((
            {
                'id': sha1,
                'indexer_configuration_id': self.tools['id'],
            } for sha1 in sha1s
        ))

    def index_content(self, sha1, raw_content):
        """Index sha1s' content and store result.

        Args:
            sha1 (bytes): content's identifier
            raw_content (bytes): raw content in bytes

        Returns:
            result (dict): representing a content_metadata
            if translation wasn't successful the translated_metadata keys
            will be kept as None

        """
        result = {
            'id': sha1,
            'indexer_configuration_id': self.tools['id'],
            'translated_metadata': None
        }
        try:
            context = self.tools['configuration']['context']
            result['translated_metadata'] = compute_metadata(
                                            context, raw_content)
        except:
            self.log.exception(
                "Problem during tool retrieval of metadata translation")
        return result

    def persist_index_computations(self, results, policy_update):
        """Persist the results in storage.

        Args:
            results ([dict]): list of content_metadata, dict with the
            following keys:
              - id (bytes): content's identifier (sha1)
              - translated_metadata (jsonb): detected metadata
            policy_update ([str]): either 'update-dups' or 'ignore-dups' to
            respectively update duplicates or ignore them

        """
        self.storage.content_metadata_add(
            results, conflict_update=(policy_update == 'update-dups'))
