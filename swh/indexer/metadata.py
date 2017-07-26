# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information
import click

from swh.indexer.indexer import ContentIndexer, RevisionIndexer
from swh.indexer.metadata_dictionary import compute_metadata
from swh.indexer.metadata_detector import detect_metadata
from swh.indexer.metadata_detector import extract_minimal_metadata_dict

from swh.model import hashutil


class ContentMetadataIndexer(ContentIndexer):
    """Indexer at content level in charge of:
    - filtering out content already indexed in content_metadata
    - reading content from objstorage with the content's id sha1
    - computing translated_metadata by given context
    - using the metadata_dictionary as the 'swh-metadata-translator' tool
    - store result in content_metadata table
    """
    CONFIG_BASE_FILENAME = 'indexer/metadata'

    def __init__(self, tool, config):
        self.tool = tool
        # twisted way to use the exact same config of RevisionMetadataIndexer
        # object that uses internally ContentMetadataIndexer
        self.new_config = config
        super().__init__()

    def prepare(self):
        super().prepare()
        self.results = []
        if self.new_config['storage']:
            self.storage = self.new_config['storage']
        if self.new_config['objstorage']:
            self.objstorage = self.new_config['objstorage']

    def retrieve_tools_information(self):
        self.config['tools'] = self.tool
        return super().retrieve_tools_information()

    def filter(self, sha1s):
        """Filter out known sha1s and return only missing ones.
        """
        yield from self.storage.content_metadata_missing((
            {
                'id': sha1,
                'indexer_configuration_id': self.tools['id'],
            } for sha1 in sha1s
        ))

    def index(self, sha1, raw_content):
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
            context = self.tools['tool_configuration']['context']
            result['translated_metadata'] = compute_metadata(
                                            context, raw_content)
            # a twisted way to keep result with indexer object for get_results
            self.results.append(result)
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

    def get_results(self):
        """
        can be called only if run method was called before

        Returns:
            results (list): list of content_metadata entries calculated
            by current indxer
        """
        return self.results


class RevisionMetadataIndexer(RevisionIndexer):
    """Indexer at Revision level in charge of:
    - filtering revisions already indexed in revision_metadata table with
      defined computation tool
    - retrieve all entry_files in root directory
    - use metadata_detector for file_names containig metadata
    - compute metadata translation if necessary and possible (depends on tool)
    - send sha1s to content indexing if possible
    - store the results for revision

    """
    CONFIG_BASE_FILENAME = 'indexer/metadata'

    ADDITIONAL_CONFIG = {
        'tools': ('dict', {
            'name': 'swh-metadata-detector',
            'version': '0.0.1',
            'configuration': {
                'type': 'local',
                'context': ['npm', 'codemeta']
            },
        }),
    }

    def prepare(self):
        super().prepare()

    def filter(self, sha1_gits):
        """Filter out known sha1s and return only missing ones.

        """
        yield from self.storage.revision_metadata_missing((
            {
                'id': sha1_git,
                'indexer_configuration_id': self.tools['id'],
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
                A dict, representing a revision_metadata, with keys:
                  - id (bytes): rev's identifier (sha1_git)
                  - indexer_configuration_id (bytes): tool used
                  - translated_metadata (bytes): dict of retrieved metadata

        """
        try:
            result = {
                'id': rev['id'],
                'indexer_configuration_id': self.tools['id'],
                'translated_metadata': None
            }

            root_dir = rev['directory']
            dir_ls = self.storage.directory_ls(root_dir, recursive=False)
            files = (entry for entry in dir_ls if entry['type'] == 'file')
            detected_files = detect_metadata(files)
            result['translated_metadata'] = self.translate_revision_metadata(
                                                                detected_files)
        except Exception as e:
            self.log.exception(
                'Problem when indexing rev')
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
        self.storage.revision_metadata_add(
            results, conflict_update=(policy_update == 'update-dups'))

    def translate_revision_metadata(self, detected_files):
        """
        Determine plan of action to translate metadata when containing
        one or multiple detected files:
        Args:
            - detected_files : dict with context name and list of sha1s
            (e.g : {'npm' : [sha1_1, sha1_2],
                     'authors': sha1_3})

        Returns:
            - translated_metadata: dict with the CodeMeta vocabulary
        """
        translated_metadata = []
        tool = {
                'name': 'swh-metadata-translator',
                'version': '0.0.1',
                'configuration': {
                    'type': 'local',
                    'context': None
                },
            }
        # TODO: iterate on each context, on each file
        # -> get raw_contents
        # -> translate each content
        config = {
            'storage': self.storage,
            'objstorage': self.objstorage
        }
        for context in detected_files.keys():
            tool['configuration']['context'] = context
            c_metadata_indexer = ContentMetadataIndexer(tool, config)
            # sha1s that aren't in content_metadata table
            sha1s_filtered = list(c_metadata_indexer.filter(
                                                    detected_files[context]))
            if sha1s_filtered:
                print(sha1s_filtered)
                # schedule indexation of content
                try:
                    c_metadata_indexer.run(sha1s_filtered,
                                           policy_update='ignore-dups')
                    # on the fly possibility:
                    local_metadata = c_metadata_indexer.get_results()
                except Exception as e:
                    self.log.warn("""Exception while indexing content""", e)
            sha1s_in_storage = [item for item in detected_files[context]
                                if item not in sha1s_filtered]
            # fetch from storage results that were skipped with filter
            for sha1 in sha1s_in_storage:
                local_metadata = {}
                # fetch content_metadata from storage
                metadata_generator = self.storage.content_metadata_get([sha1])
                for c in metadata_generator:
                    # extracting translated_metadata
                    local_metadata = c['translated_metadata']
                # local metadata is aggregated
                if local_metadata:
                    translated_metadata.append(local_metadata)
        # transform translated_metadata into min set with swh-metadata-detector
        min_metadata = extract_minimal_metadata_dict(translated_metadata)
        return min_metadata


@click.command()
@click.option('--revs_ids',
              default=['8dbb6aeb036e7fd80664eb8bfd1507881af1ba9f',
                       '026040ea79dec1b49b4e3e7beda9132b6b26b51b',
                       '9699072e21eded4be8d45e3b8d543952533fa190'],
              help='Default sha1_git to lookup')
def main(revs_ids):
    _git_sha1s = list(map(hashutil.hash_to_bytes, revs_ids))
    rev_metadata_indexer = RevisionMetadataIndexer()
    rev_metadata_indexer.run(_git_sha1s, 'update-dups')


if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    main()
