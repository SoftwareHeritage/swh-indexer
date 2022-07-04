# Copyright (C) 2017-2022 The Software Heritage developers
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
    cast,
)
from urllib.parse import urlparse

import sentry_sdk

from swh.core.config import merge_configs
from swh.core.utils import grouper
from swh.indexer.codemeta import merge_documents
from swh.indexer.indexer import (
    BaseIndexer,
    ContentIndexer,
    DirectoryIndexer,
    ObjectsDict,
    OriginIndexer,
)
from swh.indexer.metadata_detector import detect_metadata
from swh.indexer.metadata_dictionary import EXTRINSIC_MAPPINGS, INTRINSIC_MAPPINGS
from swh.indexer.metadata_dictionary.base import DirectoryLsEntry
from swh.indexer.origin_head import get_head_swhid
from swh.indexer.storage import INDEXER_CFG_KEY, Sha1
from swh.indexer.storage.model import (
    ContentMetadataRow,
    DirectoryIntrinsicMetadataRow,
    OriginExtrinsicMetadataRow,
    OriginIntrinsicMetadataRow,
)
from swh.model import hashutil
from swh.model.model import Directory, MetadataAuthorityType
from swh.model.model import ObjectType as ModelObjectType
from swh.model.model import Origin, RawExtrinsicMetadata, Sha1Git
from swh.model.swhids import CoreSWHID, ExtendedObjectType, ObjectType

REVISION_GET_BATCH_SIZE = 10
RELEASE_GET_BATCH_SIZE = 10
ORIGIN_GET_BATCH_SIZE = 10


T1 = TypeVar("T1")
T2 = TypeVar("T2")


def call_with_batches(
    f: Callable[[List[T1]], Iterable[T2]],
    args: List[T1],
    batch_size: int,
) -> Iterator[T2]:
    """Calls a function with batches of args, and concatenates the results."""
    groups = grouper(args, batch_size)
    for group in groups:
        yield from f(list(group))


class ExtrinsicMetadataIndexer(
    BaseIndexer[Sha1Git, RawExtrinsicMetadata, OriginExtrinsicMetadataRow]
):
    def process_journal_objects(self, objects: ObjectsDict) -> Dict:
        summary: Dict[str, Any] = {"status": "uneventful"}
        try:
            results = []
            for item in objects.get("raw_extrinsic_metadata", []):
                results.extend(
                    self.index(item["id"], data=RawExtrinsicMetadata.from_dict(item))
                )
        except Exception:
            if not self.catch_exceptions:
                raise
            summary["status"] = "failed"
            return summary

        summary_persist = self.persist_index_computations(results)
        self.results = results
        if summary_persist:
            for value in summary_persist.values():
                if value > 0:
                    summary["status"] = "eventful"
            summary.update(summary_persist)
        return summary

    def index(
        self,
        id: Sha1Git,
        data: Optional[RawExtrinsicMetadata],
        **kwargs,
    ) -> List[OriginExtrinsicMetadataRow]:
        if data is None:
            raise NotImplementedError(
                "ExtrinsicMetadataIndexer.index() without RawExtrinsicMetadata data"
            )
        if data.target.object_type != ExtendedObjectType.ORIGIN:
            # other types are not supported yet
            return []

        if data.authority.type != MetadataAuthorityType.FORGE:
            # metadata provided by a third-party; don't trust it
            # (technically this could be handled below, but we check it here
            # to return early; sparing a translation and origin lookup)
            # TODO: add ways to define trusted authorities
            return []

        metadata_items = []
        mappings = []
        for (mapping_name, mapping) in EXTRINSIC_MAPPINGS.items():
            if data.format in mapping.extrinsic_metadata_formats():
                metadata_item = mapping().translate(data.metadata)
                if metadata_item is not None:
                    metadata_items.append(metadata_item)
                    mappings.append(mapping_name)

        if not metadata_items:
            # Don't have any mapping to parse it, ignore
            return []

        # TODO: batch requests to origin_get_by_sha1()
        origins = self.storage.origin_get_by_sha1([data.target.object_id])
        try:
            (origin,) = origins
            if origin is None:
                raise ValueError()
        except ValueError:
            raise ValueError(f"Unknown origin {data.target}") from None

        if urlparse(data.authority.url).netloc != urlparse(origin["url"]).netloc:
            # metadata provided by a third-party; don't trust it
            # TODO: add ways to define trusted authorities
            return []

        metadata = merge_documents(metadata_items)

        return [
            OriginExtrinsicMetadataRow(
                id=origin["url"],
                indexer_configuration_id=self.tool["id"],
                from_remd_id=data.id,
                mappings=mappings,
                metadata=metadata,
            )
        ]

    def persist_index_computations(
        self, results: List[OriginExtrinsicMetadataRow]
    ) -> Dict[str, int]:
        """Persist the results in storage."""
        return self.idx_storage.origin_extrinsic_metadata_add(results)


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
        """Filter out known sha1s and return only missing ones."""
        yield from self.idx_storage.content_metadata_missing(
            (
                {
                    "id": sha1,
                    "indexer_configuration_id": self.tool["id"],
                }
                for sha1 in ids
            )
        )

    def index(
        self,
        id: Sha1,
        data: Optional[bytes] = None,
        log_suffix="unknown directory",
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
            metadata = INTRINSIC_MAPPINGS[mapping_name](log_suffix).translate(data)
        except Exception:
            self.log.exception(
                "Problem during metadata translation "
                "for content %s" % hashutil.hash_to_hex(id)
            )
            sentry_sdk.capture_exception()
        if metadata is None:
            return []
        return [
            ContentMetadataRow(
                id=id,
                indexer_configuration_id=self.tool["id"],
                metadata=metadata,
            )
        ]

    def persist_index_computations(
        self, results: List[ContentMetadataRow]
    ) -> Dict[str, int]:
        """Persist the results in storage."""
        return self.idx_storage.content_metadata_add(results)


DEFAULT_CONFIG: Dict[str, Any] = {
    "tools": {
        "name": "swh-metadata-detector",
        "version": "0.0.2",
        "configuration": {},
    },
}


class DirectoryMetadataIndexer(DirectoryIndexer[DirectoryIntrinsicMetadataRow]):
    """Directory-level indexer

    This indexer is in charge of:

    - filtering directories already indexed in directory_intrinsic_metadata table
      with defined computation tool
    - retrieve all entry_files in directory
    - use metadata_detector for file_names containing metadata
    - compute metadata translation if necessary and possible (depends on tool)
    - send sha1s to content indexing if possible
    - store the results for directory

    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = merge_configs(DEFAULT_CONFIG, self.config)

    def filter(self, sha1_gits):
        """Filter out known sha1s and return only missing ones."""
        yield from self.idx_storage.directory_intrinsic_metadata_missing(
            (
                {
                    "id": sha1_git,
                    "indexer_configuration_id": self.tool["id"],
                }
                for sha1_git in sha1_gits
            )
        )

    def index(
        self, id: Sha1Git, data: Optional[Directory] = None, **kwargs
    ) -> List[DirectoryIntrinsicMetadataRow]:
        """Index directory by processing it and organizing result.

        use metadata_detector to iterate on filenames, passes them to the content
        indexers, then merges (if more than one)

        Args:
          id: sha1_git of the directory
          data: should always be None

        Returns:
            dict: dictionary representing a directory_intrinsic_metadata, with
            keys:

            - id: directory's identifier (sha1_git)
            - indexer_configuration_id (bytes): tool used
            - metadata: dict of retrieved metadata

        """
        dir_: List[DirectoryLsEntry]
        assert data is None, "Unexpected directory object"
        dir_ = cast(
            List[DirectoryLsEntry],
            list(self.storage.directory_ls(id, recursive=False)),
        )

        try:
            if [entry["type"] for entry in dir_] == ["dir"]:
                # If the root is just a single directory, recurse into it
                # eg. PyPI packages, GNU tarballs
                subdir = dir_[0]["target"]
                dir_ = cast(
                    List[DirectoryLsEntry],
                    list(self.storage.directory_ls(subdir, recursive=False)),
                )
            files = [entry for entry in dir_ if entry["type"] == "file"]
            (mappings, metadata) = self.translate_directory_intrinsic_metadata(
                files,
                log_suffix="directory=%s" % hashutil.hash_to_hex(id),
            )
        except Exception as e:
            self.log.exception("Problem when indexing dir: %r", e)
            sentry_sdk.capture_exception()
            return []
        return [
            DirectoryIntrinsicMetadataRow(
                id=id,
                indexer_configuration_id=self.tool["id"],
                mappings=mappings,
                metadata=metadata,
            )
        ]

    def persist_index_computations(
        self, results: List[DirectoryIntrinsicMetadataRow]
    ) -> Dict[str, int]:
        """Persist the results in storage."""
        # TODO: add functions in storage to keep data in
        # directory_intrinsic_metadata
        return self.idx_storage.directory_intrinsic_metadata_add(results)

    def translate_directory_intrinsic_metadata(
        self, files: List[DirectoryLsEntry], log_suffix: str
    ) -> Tuple[List[Any], Any]:
        """
        Determine plan of action to translate metadata in the given root directory

        Args:
            files: list of file entries, as returned by
              :meth:`swh.storage.interface.StorageInterface.directory_ls`

        Returns:
            (List[str], dict): list of mappings used and dict with
            translated metadata according to the CodeMeta vocabulary

        """
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
        all_detected_files = detect_metadata(files)
        used_mappings = [
            INTRINSIC_MAPPINGS[context].name for context in all_detected_files
        ]
        for (mapping_name, detected_files) in all_detected_files.items():
            cfg = deepcopy(config)
            cfg["tools"][0]["configuration"]["context"] = mapping_name
            c_metadata_indexer = ContentMetadataIndexer(config=cfg)
            # sha1s that are in content_metadata table
            sha1s_in_storage = []
            metadata_generator = self.idx_storage.content_metadata_get(detected_files)
            for c in metadata_generator:
                # extracting metadata
                sha1 = c.id
                sha1s_in_storage.append(sha1)
                local_metadata = c.metadata
                # local metadata is aggregated
                if local_metadata:
                    metadata.append(local_metadata)

            sha1s_filtered = [
                item for item in detected_files if item not in sha1s_in_storage
            ]

            if sha1s_filtered:
                # content indexing
                try:
                    c_metadata_indexer.run(
                        sha1s_filtered,
                        log_suffix=log_suffix,
                    )
                    # on the fly possibility:
                    for result in c_metadata_indexer.results:
                        local_metadata = result.metadata
                        metadata.append(local_metadata)

                except Exception:
                    self.log.exception("Exception while indexing metadata on contents")
                    sentry_sdk.capture_exception()

        metadata = merge_documents(metadata)
        return (used_mappings, metadata)


class OriginMetadataIndexer(
    OriginIndexer[Tuple[OriginIntrinsicMetadataRow, DirectoryIntrinsicMetadataRow]]
):
    USE_TOOLS = False

    def __init__(self, config=None, **kwargs) -> None:
        super().__init__(config=config, **kwargs)
        self.directory_metadata_indexer = DirectoryMetadataIndexer(config=config)

    def index_list(
        self, origins: List[Origin], check_origin_known: bool = True, **kwargs
    ) -> List[Tuple[OriginIntrinsicMetadataRow, DirectoryIntrinsicMetadataRow]]:
        head_rev_ids = []
        head_rel_ids = []
        origin_heads: Dict[Origin, CoreSWHID] = {}

        # Filter out origins not in the storage
        if check_origin_known:
            known_origins = list(
                call_with_batches(
                    self.storage.origin_get,
                    [origin.url for origin in origins],
                    ORIGIN_GET_BATCH_SIZE,
                )
            )
        else:
            known_origins = list(origins)

        for origin in known_origins:
            if origin is None:
                continue
            head_swhid = get_head_swhid(self.storage, origin.url)
            if head_swhid:
                origin_heads[origin] = head_swhid
                if head_swhid.object_type == ObjectType.REVISION:
                    head_rev_ids.append(head_swhid.object_id)
                elif head_swhid.object_type == ObjectType.RELEASE:
                    head_rel_ids.append(head_swhid.object_id)
                else:
                    assert False, head_swhid

        head_revs = dict(
            zip(
                head_rev_ids,
                call_with_batches(
                    self.storage.revision_get, head_rev_ids, REVISION_GET_BATCH_SIZE
                ),
            )
        )
        head_rels = dict(
            zip(
                head_rel_ids,
                call_with_batches(
                    self.storage.release_get, head_rel_ids, RELEASE_GET_BATCH_SIZE
                ),
            )
        )

        results = []
        for (origin, head_swhid) in origin_heads.items():
            if head_swhid.object_type == ObjectType.REVISION:
                rev = head_revs[head_swhid.object_id]
                if not rev:
                    self.log.warning(
                        "Missing head object %s of origin %r", head_swhid, origin.url
                    )
                    continue
                directory_id = rev.directory
            elif head_swhid.object_type == ObjectType.RELEASE:
                rel = head_rels[head_swhid.object_id]
                if not rel:
                    self.log.warning(
                        "Missing head object %s of origin %r", head_swhid, origin.url
                    )
                    continue
                if rel.target_type != ModelObjectType.DIRECTORY:
                    # TODO
                    self.log.warning(
                        "Head release %s of %r has unexpected target type %s",
                        head_swhid,
                        origin.url,
                        rel.target_type,
                    )
                    continue
                assert rel.target, rel
                directory_id = rel.target
            else:
                assert False, head_swhid

            for dir_metadata in self.directory_metadata_indexer.index(directory_id):
                # There is at most one dir_metadata
                orig_metadata = OriginIntrinsicMetadataRow(
                    from_directory=dir_metadata.id,
                    id=origin.url,
                    metadata=dir_metadata.metadata,
                    mappings=dir_metadata.mappings,
                    indexer_configuration_id=dir_metadata.indexer_configuration_id,
                )
                results.append((orig_metadata, dir_metadata))

        return results

    def persist_index_computations(
        self,
        results: List[Tuple[OriginIntrinsicMetadataRow, DirectoryIntrinsicMetadataRow]],
    ) -> Dict[str, int]:
        # Deduplicate directories
        dir_metadata: List[DirectoryIntrinsicMetadataRow] = []
        orig_metadata: List[OriginIntrinsicMetadataRow] = []
        summary: Dict = {}
        for (orig_item, dir_item) in results:
            assert dir_item.metadata == orig_item.metadata
            if dir_item.metadata and not (dir_item.metadata.keys() <= {"@context"}):
                # Only store non-empty metadata sets
                if dir_item not in dir_metadata:
                    dir_metadata.append(dir_item)
                if orig_item not in orig_metadata:
                    orig_metadata.append(orig_item)

        if dir_metadata:
            summary_dir = self.idx_storage.directory_intrinsic_metadata_add(
                dir_metadata
            )
            summary.update(summary_dir)
        if orig_metadata:
            summary_ori = self.idx_storage.origin_intrinsic_metadata_add(orig_metadata)
            summary.update(summary_ori)

        return summary
