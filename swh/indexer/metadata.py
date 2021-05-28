# Copyright (C) 2017-2021 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from copy import deepcopy
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    TypeVar,
)

from swh.core.config import merge_configs
from swh.core.utils import grouper
from swh.indexer.codemeta import merge_documents
from swh.indexer.indexer import ContentIndexer, OriginIndexer, RevisionIndexer
from swh.indexer.metadata_detector import detect_metadata
from swh.indexer.metadata_dictionary import MAPPINGS
from swh.indexer.origin_head import OriginHeadIndexer
from swh.indexer.storage import INDEXER_CFG_KEY, Sha1
from swh.indexer.storage.model import (
    ContentMetadataRow,
    OriginIntrinsicMetadataRow,
    RevisionIntrinsicMetadataRow,
)
from swh.model import hashutil
from swh.model.model import Revision, Sha1Git

REVISION_GET_BATCH_SIZE = 10
ORIGIN_GET_BATCH_SIZE = 10


T1 = TypeVar("T1")
T2 = TypeVar("T2")


def call_with_batches(
    f: Callable[[List[T1]], Iterable[T2]], args: List[T1], batch_size: int,
) -> Iterator[T2]:
    """Calls a function with batches of args, and concatenates the results.
    """
    groups = grouper(args, batch_size)
    for group in groups:
        yield from f(list(group))


class ContentMetadataIndexer(ContentIndexer[ContentMetadataRow]):
    """Content-level indexer

    This indexer is in charge of:

    - filtering out content already indexed in content_metadata
    - reading content from objstorage with the content's id sha1
    - computing metadata by given context
    - using the metadata_dictionary as the 'swh-metadata-translator' tool
    - store result in content_metadata table

    """

    def filter(self, ids):
        """Filter out known sha1s and return only missing ones.
        """
        yield from self.idx_storage.content_metadata_missing(
            ({"id": sha1, "indexer_configuration_id": self.tool["id"],} for sha1 in ids)
        )

    def index(
        self,
        id: Sha1,
        data: Optional[bytes] = None,
        log_suffix="unknown revision",
        **kwargs,
    ) -> List[ContentMetadataRow]:
        """Index sha1s' content and store result.

        Args:
            id: content's identifier
            data: raw content in bytes

        Returns:
            dict: dictionary representing a content_metadata. If the
            translation wasn't successful the metadata keys will
            be returned as None

        """
        assert isinstance(id, bytes)
        assert data is not None
        metadata = None
        try:
            mapping_name = self.tool["tool_configuration"]["context"]
            log_suffix += ", content_id=%s" % hashutil.hash_to_hex(id)
            metadata = MAPPINGS[mapping_name](log_suffix).translate(data)
        except Exception:
            self.log.exception(
                "Problem during metadata translation "
                "for content %s" % hashutil.hash_to_hex(id)
            )
        if metadata is None:
            return []
        return [
            ContentMetadataRow(
                id=id, indexer_configuration_id=self.tool["id"], metadata=metadata,
            )
        ]

    def persist_index_computations(
        self, results: List[ContentMetadataRow]
    ) -> Dict[str, int]:
        """Persist the results in storage.

        Args:
            results: list of content_metadata, dict with the
              following keys:
              - id (bytes): content's identifier (sha1)
              - metadata (jsonb): detected metadata

        """
        return self.idx_storage.content_metadata_add(results)


DEFAULT_CONFIG: Dict[str, Any] = {
    "tools": {
        "name": "swh-metadata-detector",
        "version": "0.0.2",
        "configuration": {},
    },
}


class RevisionMetadataIndexer(RevisionIndexer[RevisionIntrinsicMetadataRow]):
    """Revision-level indexer

    This indexer is in charge of:

    - filtering revisions already indexed in revision_intrinsic_metadata table
      with defined computation tool
    - retrieve all entry_files in root directory
    - use metadata_detector for file_names containing metadata
    - compute metadata translation if necessary and possible (depends on tool)
    - send sha1s to content indexing if possible
    - store the results for revision

    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = merge_configs(DEFAULT_CONFIG, self.config)

    def filter(self, sha1_gits):
        """Filter out known sha1s and return only missing ones.

        """
        yield from self.idx_storage.revision_intrinsic_metadata_missing(
            (
                {"id": sha1_git, "indexer_configuration_id": self.tool["id"],}
                for sha1_git in sha1_gits
            )
        )

    def index(
        self, id: Sha1Git, data: Optional[Revision], **kwargs
    ) -> List[RevisionIntrinsicMetadataRow]:
        """Index rev by processing it and organizing result.

        use metadata_detector to iterate on filenames

        - if one filename detected -> sends file to content indexer
        - if multiple file detected -> translation needed at revision level

        Args:
          id: sha1_git of the revision
          data: revision model object from storage

        Returns:
            dict: dictionary representing a revision_intrinsic_metadata, with
            keys:

            - id (str): rev's identifier (sha1_git)
            - indexer_configuration_id (bytes): tool used
            - metadata: dict of retrieved metadata

        """
        rev = data
        assert isinstance(rev, Revision)

        try:
            root_dir = rev.directory
            dir_ls = list(self.storage.directory_ls(root_dir, recursive=False))
            if [entry["type"] for entry in dir_ls] == ["dir"]:
                # If the root is just a single directory, recurse into it
                # eg. PyPI packages, GNU tarballs
                subdir = dir_ls[0]["target"]
                dir_ls = list(self.storage.directory_ls(subdir, recursive=False))
            files = [entry for entry in dir_ls if entry["type"] == "file"]
            detected_files = detect_metadata(files)
            (mappings, metadata) = self.translate_revision_intrinsic_metadata(
                detected_files, log_suffix="revision=%s" % hashutil.hash_to_hex(rev.id),
            )
        except Exception as e:
            self.log.exception("Problem when indexing rev: %r", e)
        return [
            RevisionIntrinsicMetadataRow(
                id=rev.id,
                indexer_configuration_id=self.tool["id"],
                mappings=mappings,
                metadata=metadata,
            )
        ]

    def persist_index_computations(
        self, results: List[RevisionIntrinsicMetadataRow]
    ) -> Dict[str, int]:
        """Persist the results in storage.

        Args:
            results: list of content_mimetype, dict with the
              following keys:
              - id (bytes): content's identifier (sha1)
              - mimetype (bytes): mimetype in bytes
              - encoding (bytes): encoding in bytes

        """
        # TODO: add functions in storage to keep data in
        # revision_intrinsic_metadata
        return self.idx_storage.revision_intrinsic_metadata_add(results)

    def translate_revision_intrinsic_metadata(
        self, detected_files: Dict[str, List[Any]], log_suffix: str
    ) -> Tuple[List[Any], Any]:
        """
        Determine plan of action to translate metadata when containing
        one or multiple detected files:

        Args:
            detected_files: dictionary mapping context names (e.g.,
              "npm", "authors") to list of sha1

        Returns:
            (List[str], dict): list of mappings used and dict with
            translated metadata according to the CodeMeta vocabulary

        """
        used_mappings = [MAPPINGS[context].name for context in detected_files]
        metadata = []
        tool = {
            "name": "swh-metadata-translator",
            "version": "0.0.2",
            "configuration": {},
        }
        # TODO: iterate on each context, on each file
        # -> get raw_contents
        # -> translate each content
        config = {k: self.config[k] for k in [INDEXER_CFG_KEY, "objstorage", "storage"]}
        config["tools"] = [tool]
        for context in detected_files.keys():
            cfg = deepcopy(config)
            cfg["tools"][0]["configuration"]["context"] = context
            c_metadata_indexer = ContentMetadataIndexer(config=cfg)
            # sha1s that are in content_metadata table
            sha1s_in_storage = []
            metadata_generator = self.idx_storage.content_metadata_get(
                detected_files[context]
            )
            for c in metadata_generator:
                # extracting metadata
                sha1 = c.id
                sha1s_in_storage.append(sha1)
                local_metadata = c.metadata
                # local metadata is aggregated
                if local_metadata:
                    metadata.append(local_metadata)

            sha1s_filtered = [
                item for item in detected_files[context] if item not in sha1s_in_storage
            ]

            if sha1s_filtered:
                # content indexing
                try:
                    c_metadata_indexer.run(
                        sha1s_filtered, log_suffix=log_suffix,
                    )
                    # on the fly possibility:
                    for result in c_metadata_indexer.results:
                        local_metadata = result.metadata
                        metadata.append(local_metadata)

                except Exception:
                    self.log.exception("Exception while indexing metadata on contents")

        metadata = merge_documents(metadata)
        return (used_mappings, metadata)


class OriginMetadataIndexer(
    OriginIndexer[Tuple[OriginIntrinsicMetadataRow, RevisionIntrinsicMetadataRow]]
):
    USE_TOOLS = False

    def __init__(self, config=None, **kwargs) -> None:
        super().__init__(config=config, **kwargs)
        self.origin_head_indexer = OriginHeadIndexer(config=config)
        self.revision_metadata_indexer = RevisionMetadataIndexer(config=config)

    def index_list(
        self, origin_urls: List[str], **kwargs
    ) -> List[Tuple[OriginIntrinsicMetadataRow, RevisionIntrinsicMetadataRow]]:
        head_rev_ids = []
        origins_with_head = []
        origins = list(
            call_with_batches(
                self.storage.origin_get, origin_urls, ORIGIN_GET_BATCH_SIZE,
            )
        )
        for origin in origins:
            if origin is None:
                continue
            head_results = self.origin_head_indexer.index(origin.url)
            if head_results:
                (head_result,) = head_results
                origins_with_head.append(origin)
                head_rev_ids.append(head_result["revision_id"])

        head_revs = list(
            call_with_batches(
                self.storage.revision_get, head_rev_ids, REVISION_GET_BATCH_SIZE
            )
        )
        assert len(head_revs) == len(head_rev_ids)

        results = []
        for (origin, rev) in zip(origins_with_head, head_revs):
            if not rev:
                self.log.warning("Missing head revision of origin %r", origin.url)
                continue

            for rev_metadata in self.revision_metadata_indexer.index(rev.id, rev):
                # There is at most one rev_metadata
                orig_metadata = OriginIntrinsicMetadataRow(
                    from_revision=rev_metadata.id,
                    id=origin.url,
                    metadata=rev_metadata.metadata,
                    mappings=rev_metadata.mappings,
                    indexer_configuration_id=rev_metadata.indexer_configuration_id,
                )
                results.append((orig_metadata, rev_metadata))
        return results

    def persist_index_computations(
        self,
        results: List[Tuple[OriginIntrinsicMetadataRow, RevisionIntrinsicMetadataRow]],
    ) -> Dict[str, int]:
        # Deduplicate revisions
        rev_metadata: List[RevisionIntrinsicMetadataRow] = []
        orig_metadata: List[OriginIntrinsicMetadataRow] = []
        summary: Dict = {}
        for (orig_item, rev_item) in results:
            assert rev_item.metadata == orig_item.metadata
            if rev_item.metadata and not (rev_item.metadata.keys() <= {"@context"}):
                # Only store non-empty metadata sets
                if rev_item not in rev_metadata:
                    rev_metadata.append(rev_item)
                if orig_item not in orig_metadata:
                    orig_metadata.append(orig_item)

        if rev_metadata:
            summary_rev = self.idx_storage.revision_intrinsic_metadata_add(rev_metadata)
            summary.update(summary_rev)
        if orig_metadata:
            summary_ori = self.idx_storage.origin_intrinsic_metadata_add(orig_metadata)
            summary.update(summary_ori)

        return summary
