# Copyright (C) 2017-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import click
import itertools
import logging

from swh.indexer.indexer import ContentIndexer, RevisionIndexer, OriginIndexer
from swh.indexer.metadata_dictionary import MAPPINGS
from swh.indexer.metadata_detector import detect_metadata
from swh.indexer.metadata_detector import extract_minimal_metadata_dict
from swh.indexer.storage import INDEXER_CFG_KEY

from swh.model import hashutil


class ContentMetadataIndexer(ContentIndexer):
    """Content-level indexer

    This indexer is in charge of:

    - filtering out content already indexed in content_metadata
    - reading content from objstorage with the content's id sha1
    - computing translated_metadata by given context
    - using the metadata_dictionary as the 'swh-metadata-translator' tool
    - store result in content_metadata table

    """
    # Note: This used when the content metadata indexer is used alone
    # (not the case for example in the case of the RevisionMetadataIndexer)
    CONFIG_BASE_FILENAME = 'indexer/content_metadata'

    def __init__(self, tool, config):
        # FIXME: Simplify this twisted way to use the exact same
        # config of RevisionMetadataIndexer object that uses
        # internally ContentMetadataIndexer
        self.config = config
        self.config['tools'] = tool
        self.results = []
        super().__init__()
        self.tool = self.tools[0]  # Tool is now registered (cf. prepare call)

    def filter(self, ids):
        """Filter out known sha1s and return only missing ones.
        """
        yield from self.idx_storage.content_metadata_missing((
            {
                'id': sha1,
                'indexer_configuration_id': self.tool['id'],
            } for sha1 in ids
        ))

    def index(self, id, data):
        """Index sha1s' content and store result.

        Args:
            id (bytes): content's identifier
            data (bytes): raw content in bytes

        Returns:
            dict: dictionary representing a content_metadata. If the
            translation wasn't successful the translated_metadata keys will
            be returned as None

        """
        result = {
            'id': id,
            'indexer_configuration_id': self.tool['id'],
            'translated_metadata': None
        }
        try:
            mapping_name = self.tool['tool_configuration']['context']
            result['translated_metadata'] = MAPPINGS[mapping_name] \
                .translate(data)
        except Exception:
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
        self.idx_storage.content_metadata_add(
            results, conflict_update=(policy_update == 'update-dups'))


class RevisionMetadataIndexer(RevisionIndexer):
    """Revision-level indexer

    This indexer is in charge of:

    - filtering revisions already indexed in revision_metadata table with
      defined computation tool
    - retrieve all entry_files in root directory
    - use metadata_detector for file_names containing metadata
    - compute metadata translation if necessary and possible (depends on tool)
    - send sha1s to content indexing if possible
    - store the results for revision

    """
    CONFIG_BASE_FILENAME = 'indexer/revision_metadata'

    ADDITIONAL_CONFIG = {
        'tools': ('dict', {
            'name': 'swh-metadata-detector',
            'version': '0.0.2',
            'configuration': {
                'type': 'local',
                'context': ['NpmMapping', 'CodemetaMapping']
            },
        }),
    }

    ContentMetadataIndexer = ContentMetadataIndexer

    def prepare(self):
        super().prepare()
        self.tool = self.tools[0]

    def filter(self, sha1_gits):
        """Filter out known sha1s and return only missing ones.

        """
        yield from self.idx_storage.revision_metadata_missing((
            {
                'id': sha1_git,
                'indexer_configuration_id': self.tool['id'],
            } for sha1_git in sha1_gits
        ))

    def index(self, rev):
        """Index rev by processing it and organizing result.

        use metadata_detector to iterate on filenames

        - if one filename detected -> sends file to content indexer
        - if multiple file detected -> translation needed at revision level

        Args:
          rev (bytes): revision artifact from storage

        Returns:
            dict: dictionary representing a revision_metadata, with keys:

            - id (str): rev's identifier (sha1_git)
            - indexer_configuration_id (bytes): tool used
            - translated_metadata: dict of retrieved metadata

        """
        result = {
            'id': rev['id'],
            'indexer_configuration_id': self.tool['id'],
            'translated_metadata': None
        }

        try:
            root_dir = rev['directory']
            dir_ls = self.storage.directory_ls(root_dir, recursive=False)
            files = [entry for entry in dir_ls if entry['type'] == 'file']
            detected_files = detect_metadata(files)
            result['translated_metadata'] = self.translate_revision_metadata(
                    detected_files)
        except Exception as e:
            self.log.exception(
                'Problem when indexing rev: %r', e)
        return result

    def persist_index_computations(self, results, policy_update):
        """Persist the results in storage.

        Args:
            results ([dict]): list of content_mimetype, dict with the
              following keys:
              - id (bytes): content's identifier (sha1)
              - mimetype (bytes): mimetype in bytes
              - encoding (bytes): encoding in bytes
            policy_update ([str]): either 'update-dups' or 'ignore-dups' to
              respectively update duplicates or ignore them

        """
        # TODO: add functions in storage to keep data in revision_metadata
        self.idx_storage.revision_metadata_add(
            results, conflict_update=(policy_update == 'update-dups'))

    def translate_revision_metadata(self, detected_files):
        """
        Determine plan of action to translate metadata when containing
        one or multiple detected files:

        Args:
            detected_files (dict): dictionary mapping context names (e.g.,
              "npm", "authors") to list of sha1

        Returns:
            dict: dict with translated metadata according to the CodeMeta
            vocabulary

        """
        translated_metadata = []
        tool = {
                'name': 'swh-metadata-translator',
                'version': '0.0.2',
                'configuration': {
                    'type': 'local',
                    'context': None
                },
            }
        # TODO: iterate on each context, on each file
        # -> get raw_contents
        # -> translate each content
        config = {
            k: self.config[k]
            for k in [INDEXER_CFG_KEY, 'objstorage', 'storage']
        }
        for context in detected_files.keys():
            tool['configuration']['context'] = context
            c_metadata_indexer = self.ContentMetadataIndexer(tool, config)
            # sha1s that are in content_metadata table
            sha1s_in_storage = []
            metadata_generator = self.idx_storage.content_metadata_get(
                detected_files[context])
            for c in metadata_generator:
                # extracting translated_metadata
                sha1 = c['id']
                sha1s_in_storage.append(sha1)
                local_metadata = c['translated_metadata']
                # local metadata is aggregated
                if local_metadata:
                    translated_metadata.append(local_metadata)

            sha1s_filtered = [item for item in detected_files[context]
                              if item not in sha1s_in_storage]

            if sha1s_filtered:
                # content indexing
                try:
                    c_metadata_indexer.run(sha1s_filtered,
                                           policy_update='ignore-dups')
                    # on the fly possibility:
                    for result in c_metadata_indexer.results:
                        local_metadata = result['translated_metadata']
                        translated_metadata.append(local_metadata)

                except Exception:
                    self.log.exception(
                        "Exception while indexing metadata on contents")

        # transform translated_metadata into min set with swh-metadata-detector
        min_metadata = extract_minimal_metadata_dict(translated_metadata)
        return min_metadata


class OriginMetadataIndexer(OriginIndexer):
    CONFIG_BASE_FILENAME = 'indexer/origin_intrinsic_metadata'

    ADDITIONAL_CONFIG = {
        'tools': ('list', [])
    }

    def check(self, **kwargs):
        kwargs['check_tools'] = False
        super().check(**kwargs)

    def filter(self, ids):
        return ids

    def run(self, origin_head, policy_update):
        """Expected to be called with the result of RevisionMetadataIndexer
        as first argument; ie. not a list of ids as other indexers would.

        Args:
            origin_head (dict): {str(origin_id): rev_id}
              keys `origin_id` and `revision_id`, which is the result
              of OriginHeadIndexer.
            policy_update (str): `'ignore-dups'` or `'update-dups'`
        """
        origin_head_map = {origin_id: hashutil.hash_to_bytes(rev_id)
                           for (origin_id, rev_id) in origin_head.items()}

        # Fix up the argument order. revisions_metadata has to be the
        # first argument because of celery.chain; the next line calls
        # run() with the usual order, ie. origin ids first.
        return super().run(ids=list(origin_head_map),
                           policy_update=policy_update,
                           parse_ids=False,
                           origin_head_map=origin_head_map)

    def index(self, origin, *, origin_head_map):
        # Get the last revision of the origin.
        revision_id = origin_head_map[str(origin['id'])]

        revision_metadata = self.idx_storage \
            .revision_metadata_get([revision_id])

        results = []
        for item in revision_metadata:
            assert item['id'] == revision_id
            # Get the metadata of that revision, and return it
            results.append({
                    'origin_id': origin['id'],
                    'metadata': item['translated_metadata'],
                    'from_revision': revision_id,
                    'indexer_configuration_id':
                    item['tool']['id'],
                    })
        return results

    def persist_index_computations(self, results, policy_update):
        self.idx_storage.origin_intrinsic_metadata_add(
            list(itertools.chain(*results)),
            conflict_update=(policy_update == 'update-dups'))


@click.command()
@click.option('--revs', '-i',
              help='Default sha1_git to lookup', multiple=True)
def main(revs):
    _git_sha1s = list(map(hashutil.hash_to_bytes, revs))
    rev_metadata_indexer = RevisionMetadataIndexer()
    rev_metadata_indexer.run(_git_sha1s, 'update-dups')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
