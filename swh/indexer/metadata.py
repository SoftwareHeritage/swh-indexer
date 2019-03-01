# Copyright (C) 2017-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from copy import deepcopy

from swh.indexer.indexer import ContentIndexer, RevisionIndexer, OriginIndexer
from swh.indexer.origin_head import OriginHeadIndexer
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
    def filter(self, ids):
        """Filter out known sha1s and return only missing ones.
        """
        yield from self.idx_storage.content_metadata_missing((
            {
                'id': sha1,
                'indexer_configuration_id': self.tool['id'],
            } for sha1 in ids
        ))

    def index(self, id, data, log_suffix='unknown revision'):
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
            log_suffix += ', content_id=%s' % hashutil.hash_to_hex(id)
            result['translated_metadata'] = \
                MAPPINGS[mapping_name](log_suffix).translate(data)
        except Exception:
            self.log.exception(
                "Problem during metadata translation "
                "for content %s" % hashutil.hash_to_hex(id))
        if result['translated_metadata'] is None:
            return None
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
    ADDITIONAL_CONFIG = {
        'tools': ('dict', {
            'name': 'swh-metadata-detector',
            'version': '0.0.2',
            'configuration': {
            },
        }),
    }

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
          rev (dict): revision artifact from storage

        Returns:
            dict: dictionary representing a revision_metadata, with keys:

            - id (str): rev's identifier (sha1_git)
            - indexer_configuration_id (bytes): tool used
            - translated_metadata: dict of retrieved metadata

        """
        result = {
            'id': rev['id'],
            'indexer_configuration_id': self.tool['id'],
            'mappings': None,
            'translated_metadata': None
        }

        try:
            root_dir = rev['directory']
            dir_ls = self.storage.directory_ls(root_dir, recursive=False)
            files = [entry for entry in dir_ls if entry['type'] == 'file']
            detected_files = detect_metadata(files)
            (mappings, metadata) = self.translate_revision_metadata(
                detected_files,
                log_suffix='revision=%s' % hashutil.hash_to_hex(rev['id']))
            result['mappings'] = mappings
            result['translated_metadata'] = metadata
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

    def translate_revision_metadata(self, detected_files, log_suffix):
        """
        Determine plan of action to translate metadata when containing
        one or multiple detected files:

        Args:
            detected_files (dict): dictionary mapping context names (e.g.,
              "npm", "authors") to list of sha1

        Returns:
            (List[str], dict): list of mappings used and dict with
            translated metadata according to the CodeMeta vocabulary

        """
        used_mappings = [MAPPINGS[context].name for context in detected_files]
        translated_metadata = []
        tool = {
                'name': 'swh-metadata-translator',
                'version': '0.0.2',
                'configuration': {
                },
            }
        # TODO: iterate on each context, on each file
        # -> get raw_contents
        # -> translate each content
        config = {
            k: self.config[k]
            for k in [INDEXER_CFG_KEY, 'objstorage', 'storage']
        }
        config['tools'] = [tool]
        for context in detected_files.keys():
            cfg = deepcopy(config)
            cfg['tools'][0]['configuration']['context'] = context
            c_metadata_indexer = ContentMetadataIndexer(config=cfg)
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
                                           policy_update='ignore-dups',
                                           log_suffix=log_suffix)
                    # on the fly possibility:
                    for result in c_metadata_indexer.results:
                        local_metadata = result['translated_metadata']
                        translated_metadata.append(local_metadata)

                except Exception:
                    self.log.exception(
                        "Exception while indexing metadata on contents")

        # transform translated_metadata into min set with swh-metadata-detector
        min_metadata = extract_minimal_metadata_dict(translated_metadata)
        return (used_mappings, min_metadata)


class OriginMetadataIndexer(OriginIndexer):
    ADDITIONAL_CONFIG = RevisionMetadataIndexer.ADDITIONAL_CONFIG

    USE_TOOLS = False

    def __init__(self, config=None, **kwargs):
        super().__init__(config=config, **kwargs)
        self.origin_head_indexer = OriginHeadIndexer(config=config)
        self.revision_metadata_indexer = RevisionMetadataIndexer(config=config)

    def index_list(self, origins):
        head_rev_ids = []
        origins_with_head = []
        for origin in origins:
            head_result = self.origin_head_indexer.index(origin)
            if head_result:
                origins_with_head.append(origin)
                head_rev_ids.append(head_result['revision_id'])

        head_revs = list(self.storage.revision_get(head_rev_ids))
        assert len(head_revs) == len(head_rev_ids)

        results = []
        for (origin, rev) in zip(origins_with_head, head_revs):
            if not rev:
                self.log.warning('Missing head revision of origin %r',
                                 origin)
                continue

            rev_metadata = self.revision_metadata_indexer.index(rev)
            orig_metadata = {
                'from_revision': rev_metadata['id'],
                'origin_id': origin['id'],
                'metadata': rev_metadata['translated_metadata'],
                'mappings': rev_metadata['mappings'],
                'indexer_configuration_id':
                    rev_metadata['indexer_configuration_id'],
            }
            results.append((orig_metadata, rev_metadata))
        return results

    def persist_index_computations(self, results, policy_update):
        conflict_update = (policy_update == 'update-dups')

        # Deduplicate revisions
        rev_metadata = []
        orig_metadata = []
        revs_to_delete = []
        origs_to_delete = []
        for (orig_item, rev_item) in results:
            assert rev_item['mappings'] == orig_item['mappings']
            if rev_item['mappings']:
                # Only store translated metadata if we found a metadata file.
                # Otherwise it's just an empty dict with a "@context" key.
                if rev_item not in rev_metadata:
                    rev_metadata.append(rev_item)
                if orig_item not in orig_metadata:
                    orig_metadata.append(orig_item)
            else:
                if rev_item not in revs_to_delete:
                    revs_to_delete.append(rev_item)
                if orig_item not in origs_to_delete:
                    origs_to_delete.append(orig_item)

        if rev_metadata:
            self.idx_storage.revision_metadata_add(
                rev_metadata, conflict_update=conflict_update)
        if orig_metadata:
            self.idx_storage.origin_intrinsic_metadata_add(
                orig_metadata, conflict_update=conflict_update)

        # revs_to_delete should always be empty unless we changed a mapping
        # to detect less files.
        # However, origs_to_delete may be empty whenever an upstream deletes
        # a metadata file.
        if origs_to_delete:
            self.idx_storage.origin_intrinsic_metadata_delete(origs_to_delete)
        if revs_to_delete:
            self.idx_storage.revision_metadata_delete(revs_to_delete)
