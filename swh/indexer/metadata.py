# Copyright (C) 2017-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from copy import deepcopy
import datetime
import hashlib
from importlib.metadata import version
import logging
import re
import time
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
import urllib.parse
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
from swh.indexer.metadata_detector import detect_metadata_from_directory_entries
from swh.indexer.metadata_mapping import get_extrinsic_mappings, get_intrinsic_mappings
from swh.indexer.origin_head import get_head_swhid
from swh.indexer.storage import INDEXER_CFG_KEY
from swh.indexer.storage.model import (
    ContentMetadataRow,
    DirectoryIntrinsicMetadataRow,
    OriginExtrinsicMetadataRow,
    OriginIntrinsicMetadataRow,
)
from swh.model.hashutil import HashDict, hash_to_hex
from swh.model.model import (
    Content,
    Directory,
    MetadataAuthorityType,
    Origin,
    RawExtrinsicMetadata,
    ReleaseTargetType,
    Sha1Git,
)
from swh.model.swhids import CoreSWHID, ExtendedObjectType, ObjectType
from swh.storage.algos.directory import directory_get

# Default batch size per object type (can be overridden through indexer configuration)
DEFAULT_BATCH_SIZE = {
    "revision": 10,
    "release": 10,
    "origin": 10,
}


T1 = TypeVar("T1")
T2 = TypeVar("T2")

logger = logging.getLogger(__name__)


def fetch_in_batches(
    fetch_fn: Callable[[List[T1]], Iterable[T2]],
    args: List[T1],
    batch_size: int,
) -> Iterator[T2]:
    """Calls a function `fetch_fn` on batchs of args, this yields the results when ok.

    When a batch raised, processing continues with the next batch of data to read.

    Then another round of read is executed on the failed batchs but one object at a
    time, any further failure is logged and skipped, so callers receive a *partial*
    result set rather than a total failure.

    """
    batchs = grouper(args, batch_size)

    # Read and yield ids we successfully read from `fetch_fn` call
    for batch in batchs:
        batch_list: List[T1] = list(batch)
        try:
            yield from fetch_fn(batch_list)
        except Exception:
            # If the whole batch failed, fall back to fetching objects individually
            # in case a single object is causing the exception
            for obj in batch_list:
                try:
                    # Try to fetch one object at a time
                    yield from fetch_fn([obj])
                except Exception as exc:
                    # If it failed again, we found the problematic object, this time, we just
                    # log it and skip it
                    logger.error(
                        "Failure to retrieve object %s when calling %r: %s",
                        obj,
                        fetch_fn,
                        exc,
                        exc_info=True,
                    )


def fetch_as_dict(
    fetch_fn: Callable[[List[T1]], Iterable[T2]],
    ids: List[T1],
    batch_size: int,
) -> Dict[T1, T2]:
    """Return a dict ``{id: object}``; missing items are logged."""
    result: Dict[T1, T2] = {}
    for obj in fetch_in_batches(fetch_fn, ids, batch_size):
        if obj is None:
            continue
        result[obj.id] = obj  # type: ignore
    return result


class ExtrinsicMetadataIndexer(
    BaseIndexer[Sha1Git, RawExtrinsicMetadata, OriginExtrinsicMetadataRow]
):
    """Indexer for Raw Extrinsic Metadata

    For supported extrinsic metadata formats, translate the original format
    into CodeMeta, and attach the result to the Origin.

    Use XXX to get registered mapping formats.

    """

    object_types = ["raw_extrinsic_metadata"]

    def process_journal_objects(self, objects: ObjectsDict) -> Dict:
        summary: Dict[str, Any] = {"status": "uneventful"}
        try:
            results = {}
            for item in objects.get("raw_extrinsic_metadata", []):
                remd = RawExtrinsicMetadata.from_dict(item)
                sentry_sdk.set_tag("swh-indexer-remd-swhid", str(remd.swhid()))
                for result in self.index(remd.id, data=remd):
                    results[result.id] = result
        except Exception:
            if not self.catch_exceptions:
                raise
            summary["status"] = "failed"
            return summary

        summary_persist = self.persist_index_computations(list(results.values()))
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
        if data.target.object_type == ExtendedObjectType.ORIGIN:
            origin_sha1 = data.target.object_id
        elif data.origin is not None:
            if (
                data.fetcher.name == "swh-deposit"
                and data.discovery_date
                < datetime.datetime(
                    2024, 5, 16, 15, 20, 0, tzinfo=datetime.timezone.utc
                )
            ):
                # Workaround for deposits while swh.model.swhids did not unescape origin
                # URLs while parsing <swh:object swhid="..." />, which was fixed in
                # https://gitlab.softwareheritage.org/swh/devel/swh-model/-/merge_requests/348
                # itself deployed shortly after 2024-05-16T15:18:07 by
                # https://gitlab.softwareheritage.org/swh/infra/swh-apps/-/commit/70bd86aafcbc1787183e5d2cd52c392ae012e65e
                if "%" in data.origin:
                    assert re.match(
                        "^https://cran.r-project.org/package%3D[a-zA-Z]+$", data.origin
                    ), data
                origin_url = urllib.parse.unquote_to_bytes(data.origin)
            else:
                origin_url = data.origin.encode()

            # HACK: As swh-search does not (yet?) support searching on directories
            # and traversing back to origins, we index metadata on non-origins with
            # an origin context as if they were on the origin itself.
            origin_sha1 = hashlib.sha1(origin_url).digest()
        else:
            # other types are not supported yet
            return []
        metadata_items = []
        mappings: List[str] = []
        for mapping_cls in get_extrinsic_mappings().values():
            if data.format in mapping_cls.extrinsic_metadata_formats():
                mapping = mapping_cls()
                metadata_item = mapping.translate(data.metadata)
                if metadata_item is not None:
                    metadata_items.append(metadata_item)
                    mappings.append(mapping.name)

        if not metadata_items:
            # Don't have any mapping to parse it, ignore
            return []

        # TODO: batch requests to origin_get_by_sha1()
        num_retries = 6
        sleep_delay = 10
        for _ in range(num_retries):
            origins = self.storage.origin_get_by_sha1([origin_sha1])
            try:
                (origin,) = origins
                if origin is not None:
                    break
            except ValueError:
                pass
            # The origin does not exist. This may be due to some replication lag
            # between the loader's DB/journal and the DB we are consuming from.
            # Wait a bit and try again
            logger.debug(
                "Origin %s not found, sleeping for %ss.", data.target, sleep_delay
            )
            time.sleep(sleep_delay)
        else:
            # Does not exist, or replication lag > 60s.
            raise ValueError(
                f"Unknown origin swh:1:ori:{origin_sha1.hex()} for metadata target: "
                f"{data.target}. Is the swh-storage database replication lag "
                f"over {num_retries * sleep_delay}s?"
            ) from None

        authority_base_url = urlparse(data.authority.url).netloc
        origin_base_url = urlparse(origin["url"]).netloc

        if (
            data.authority.type != MetadataAuthorityType.REGISTRY
            and authority_base_url != origin_base_url
        ):
            # Registries are allowed to push metadata provided related to origins that
            # do not match their own URL
            # TODO: add ways to define trusted authorities
            logger.debug(
                "Authority URL %s and origin URL %s do not match, ignoring.",
                authority_base_url,
                origin_base_url,
            )
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
    - using the metadata_mapping as the 'swh-metadata-translator' tool
    - store result in content_metadata table

    """

    def filter(self, ids: List[HashDict]):
        """Filter out known sha1s and return only missing ones."""
        yield from self.idx_storage.content_metadata_missing(
            (
                {
                    "id": id["sha1"],
                    "indexer_configuration_id": self.tool["id"],
                }
                for id in ids
            )
        )

    def index(
        self,
        id: HashDict,
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
        assert "sha1" in id
        assert data is not None
        metadata = None
        try:
            mapping_name = self.tool["tool_configuration"]["context"]
            log_suffix += ", content_id=%s" % hash_to_hex(id["sha1"])
            metadata = get_intrinsic_mappings()[mapping_name](log_suffix).translate(
                data
            )
        except Exception:
            self.log.exception(
                "Problem during metadata translation "
                "for content %s" % hash_to_hex(id["sha1"])
            )
            sentry_sdk.capture_exception()
        if metadata is None:
            return []
        return [
            ContentMetadataRow(
                id=id["sha1"],
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
        "name": "swh.indexer.metadata",
        "version": version("swh.indexer"),
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

        assert data is None, "Unexpected directory object"
        directory = directory_get(self.storage, id)
        assert directory is not None

        try:
            subdirs = [entry for entry in directory.entries if entry.type == "dir"]
            if len(subdirs) == 1:
                # If the root is just a single directory, recurse into it
                # eg. PyPI packages, GNU tarballs
                directory = directory_get(self.storage, subdirs[0].target)

            assert directory is not None
            interesting_content_ids = []
            # Map from file direntry to mapping detected
            entry_to_mapping = {}
            # Filtering now relevant metadata file entries
            for mapping_dir_entry, entry in detect_metadata_from_directory_entries(
                list(directory.entries)
            ).items():
                if entry is None:
                    continue
                content_id = entry.target  # It's a sha1_git
                interesting_content_ids.append(content_id)
                entry_to_mapping[content_id] = mapping_dir_entry

            # We have to transform the list of directory entries returned by the storage
            # into DirectoryLsEntry (so ids are correct). Currently, DirectoryEntry uses
            # sha1_git as id but we need the sha1)

            mapping_content: Dict[str, Content] = {}

            # Now that we have filtered the interesting file entries, we can retrieve
            # the filter list of interesting associated contents to have their full ids
            # and keep the mapping dict updated with a Content reference instead of a
            # DirectoryEntry
            for content in self.storage.content_get(
                interesting_content_ids, algo="sha1_git"
            ):
                if content is None:
                    continue
                mapping_name = entry_to_mapping[content.sha1_git]
                mapping_content[mapping_name] = content

            # We can now translate into relevant metadata information
            (mappings, metadata) = self.translate_directory_intrinsic_metadata(
                mapping_content, log_suffix=f"directory={hash_to_hex(id)}"
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
        self, mapping_content: Dict[str, Content], log_suffix: str
    ) -> Tuple[List[Any], Any]:
        """Determine how to translate metadata from the directory file entries.

        Args:
            files: list of file entries DirectoryEntry of type 'file'

        Returns:
            (List[str], dict): list of mappings used and dict with
            translated metadata according to the CodeMeta vocabulary

        """
        metadata = []
        # Load/Retrieve intrinsic mappings
        intrinsic_mappings = get_intrinsic_mappings()

        config = {
            k: self.config[k]
            for k in [INDEXER_CFG_KEY, "objstorage", "storage", "tools"]
        }
        used_mappings = []
        for mapping_name, detected_content in mapping_content.items():
            # Compulse the list of used mappings
            used_mappings.append(intrinsic_mappings[mapping_name].name)

            detected_contents = [detected_content]
            # sha1s that are in content_metadata table
            sha1s_in_idx_storage = []
            for c in self.idx_storage.content_metadata_get(
                [content.sha1 for content in detected_contents]
            ):
                # extracting metadata
                sha1s_in_idx_storage.append(c.id)  # id is a sha1
                local_metadata = c.metadata
                # local metadata is aggregated
                if local_metadata:
                    metadata.append(local_metadata)

            sha1s_to_index = [
                HashDict(sha1=content.sha1)
                for content in detected_contents
                if content.sha1 not in sha1s_in_idx_storage
            ]

            # If we did not have yet indexed the file
            if sha1s_to_index:
                cfg = deepcopy(config)
                cfg["tools"]["configuration"]["context"] = mapping_name
                c_metadata_indexer = ContentMetadataIndexer(config=cfg)
                # content indexing
                try:
                    _, results = c_metadata_indexer.run(
                        sha1s_to_index,
                        log_suffix=log_suffix,
                    )
                    # on the fly possibility:
                    for result in results:
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
    """Indexer for intrinsic metadata found within origin's root directory

    If there is a metadata file corresponding to a known format in the root
    directory of an Origin (i.e. in the root directory of the , read it and

    """

    USE_TOOLS = False

    def __init__(self, config=None, **kwargs) -> None:
        super().__init__(config=config, **kwargs)
        self.directory_metadata_indexer = DirectoryMetadataIndexer(config=config)
        self.batch_size = (
            config.get("batch_size", DEFAULT_BATCH_SIZE)
            if config
            else DEFAULT_BATCH_SIZE
        )

    def index_list(
        self,
        origins: List[Origin],
        *,
        check_origin_known: bool = True,
        **kwargs,
    ) -> List[Tuple[OriginIntrinsicMetadataRow, DirectoryIntrinsicMetadataRow]]:
        head_rev_ids = []
        head_rel_ids = []
        origin_heads: Dict[Origin, CoreSWHID] = {}

        # Filter out origins missing from the storage
        if check_origin_known:
            known_urls = set(
                fetch_in_batches(
                    self.storage.origin_get,
                    [origin.url for origin in origins],
                    self.batch_size["origin"],
                )
            )
            known_origins = [o for o in origins if o in known_urls]
        else:
            known_origins = list(origins)

        # Scan origins once, collect head IDs per object type {release, revision}
        for origin in known_origins:
            if origin is None:
                continue
            head_swhid = get_head_swhid(self.storage, origin.url)
            if head_swhid is None:
                continue
            if head_swhid.object_type == ObjectType.REVISION:
                head_rev_ids.append(head_swhid.object_id)
            elif head_swhid.object_type == ObjectType.RELEASE:
                head_rel_ids.append(head_swhid.object_id)
            else:
                self.log.error(
                    "Unexpected object type %s for origin %s. Skipping",
                    head_swhid,
                    origin.url,
                )
                continue
            # Skip origin already whose head is already detected
            if origin in origin_heads:
                continue
            origin_heads[origin] = head_swhid

        # fetch revisions (and releases) as dict. If revision_get (or release_get)
        # raises, this will skip such objects. It will receive less results but continue
        # indexation.
        head_revs = fetch_as_dict(
            self.storage.revision_get, head_rev_ids, self.batch_size["revision"]
        )
        head_rels = fetch_as_dict(
            self.storage.release_get, head_rel_ids, self.batch_size["release"]
        )

        results = []
        for origin, head_swhid in origin_heads.items():
            sentry_sdk.set_tag("swh-indexer-origin-url", origin.url)
            sentry_sdk.set_tag("swh-indexer-origin-head-swhid", str(head_swhid))
            if head_swhid.object_type == ObjectType.REVISION:
                rev = head_revs.get(head_swhid.object_id)
                if not rev:
                    self.log.warning(
                        "Missing revision head object %s of origin %r",
                        head_swhid,
                        origin.url,
                    )
                    continue
                directory_id = rev.directory
            elif head_swhid.object_type == ObjectType.RELEASE:
                rel = head_rels.get(head_swhid.object_id)
                if not rel:
                    self.log.warning(
                        "Missing release head object %s of origin %r",
                        head_swhid,
                        origin.url,
                    )
                    continue
                if rel.target_type != ReleaseTargetType.DIRECTORY:
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
                self.log.error("Unhandled head type %s for %s", head_swhid, origin.url)
                continue

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
        dir_metadata: Dict[bytes, DirectoryIntrinsicMetadataRow] = {}
        orig_metadata: Dict[str, OriginIntrinsicMetadataRow] = {}
        summary: Dict = {}
        for orig_item, dir_item in results:
            assert dir_item.metadata == orig_item.metadata
            if dir_item.metadata and not (dir_item.metadata.keys() <= {"@context"}):
                # Only store non-empty metadata sets
                if dir_item.id not in dir_metadata:
                    dir_metadata[dir_item.id] = dir_item
                if orig_item.id not in orig_metadata:
                    orig_metadata[orig_item.id] = orig_item

        if dir_metadata:
            summary_dir = self.idx_storage.directory_intrinsic_metadata_add(
                list(dir_metadata.values())
            )
            summary.update(summary_dir)
        if orig_metadata:
            summary_ori = self.idx_storage.origin_intrinsic_metadata_add(
                list(orig_metadata.values())
            )
            summary.update(summary_ori)

        return summary
