# Copyright (C) 2018-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import Counter, defaultdict
import itertools
import json
import math
import operator
import re
from typing import (
    Any,
    Dict,
    Generic,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from swh.core.collections import SortedList
from swh.model.hashutil import hash_to_bytes, hash_to_hex
from swh.model.model import SHA1_SIZE, Sha1Git
from swh.storage.utils import get_partition_bounds_bytes

from . import MAPPING_NAMES, check_id_duplicates
from .exc import IndexerStorageArgumentException
from .interface import PagedResult, Sha1
from .model import (
    BaseRow,
    ContentCtagsRow,
    ContentLanguageRow,
    ContentLicenseRow,
    ContentMetadataRow,
    ContentMimetypeRow,
    DirectoryIntrinsicMetadataRow,
    OriginExtrinsicMetadataRow,
    OriginIntrinsicMetadataRow,
)
from .writer import JournalWriter

SHA1_DIGEST_SIZE = 160

ToolId = int


def _transform_tool(tool):
    return {
        "id": tool["id"],
        "name": tool["tool_name"],
        "version": tool["tool_version"],
        "configuration": tool["tool_configuration"],
    }


def check_id_types(data: List[Dict[str, Any]]):
    """Checks all elements of the list have an 'id' whose type is 'bytes'."""
    if not all(isinstance(item.get("id"), bytes) for item in data):
        raise IndexerStorageArgumentException("identifiers must be bytes.")


def _key_from_dict(d):
    return tuple(sorted(d.items()))


TValue = TypeVar("TValue", bound=BaseRow)


class SubStorage(Generic[TValue]):
    """Implements common missing/get/add logic for each indexer type."""

    _data: Dict[Sha1, Dict[Tuple, Dict[str, Any]]]
    _tools_per_id: Dict[Sha1, Set[ToolId]]

    def __init__(self, row_class: Type[TValue], tools, journal_writer):
        self.row_class = row_class
        self._tools = tools
        self._sorted_ids = SortedList[bytes, Sha1]()
        self._data = defaultdict(dict)
        self._journal_writer = journal_writer
        self._tools_per_id = defaultdict(set)

    def _key_from_dict(self, d) -> Tuple:
        """Like the global _key_from_dict, but filters out dict keys that don't
        belong in the unique key."""
        return _key_from_dict({k: d[k] for k in self.row_class.UNIQUE_KEY_FIELDS})

    def missing(self, keys: Iterable[Dict]) -> List[Sha1]:
        """List data missing from storage.

        Args:
            data (iterable): dictionaries with keys:

                - **id** (bytes): sha1 identifier
                - **indexer_configuration_id** (int): tool used to compute
                  the results

        Yields:
            missing sha1s

        """
        results = []
        for key in keys:
            tool_id = key["indexer_configuration_id"]
            id_ = key["id"]
            if tool_id not in self._tools_per_id.get(id_, set()):
                results.append(id_)
        return results

    def get(self, ids: Iterable[Sha1]) -> List[TValue]:
        """Retrieve data per id.

        Args:
            ids (iterable): sha1 checksums

        Yields:
            dict: dictionaries with the following keys:

              - **id** (bytes)
              - **tool** (dict): tool used to compute metadata
              - arbitrary data (as provided to `add`)

        """
        results = []
        for id_ in ids:
            for entry in self._data[id_].values():
                entry = entry.copy()
                tool_id = entry.pop("indexer_configuration_id")
                results.append(
                    self.row_class(
                        id=id_,
                        tool=_transform_tool(self._tools[tool_id]),
                        **entry,
                    )
                )
        return results

    def get_all(self) -> List[TValue]:
        return self.get(self._sorted_ids)

    def get_partition(
        self,
        indexer_configuration_id: int,
        partition_id: int,
        nb_partitions: int,
        page_token: Optional[str] = None,
        limit: int = 1000,
    ) -> PagedResult[Sha1]:
        """Retrieve ids of content with `indexer_type` within partition partition_id
        bound by limit.

        Args:
            **indexer_type**: Type of data content to index (mimetype, language, etc...)
            **indexer_configuration_id**: The tool used to index data
            **partition_id**: index of the partition to fetch
            **nb_partitions**: total number of partitions to split into
            **page_token**: opaque token used for pagination
            **limit**: Limit result (default to 1000)
            **with_textual_data** (bool): Deal with only textual content (True) or all
                content (all contents by defaults, False)

        Raises:
            IndexerStorageArgumentException for;
            - limit to None
            - wrong indexer_type provided

        Returns:
            PagedResult of Sha1. If next_page_token is None, there is no more data to
            fetch

        """
        if limit is None:
            raise IndexerStorageArgumentException("limit should not be None")
        (start, end) = get_partition_bounds_bytes(
            partition_id, nb_partitions, SHA1_SIZE
        )

        if page_token:
            start = hash_to_bytes(page_token)
        if end is None:
            end = b"\xff" * SHA1_SIZE

        next_page_token: Optional[str] = None
        ids: List[Sha1] = []
        sha1s = (sha1 for sha1 in self._sorted_ids.iter_from(start))
        for counter, sha1 in enumerate(sha1s):
            if sha1 > end:
                break
            if counter >= limit:
                next_page_token = hash_to_hex(sha1)
                break
            ids.append(sha1)

        assert len(ids) <= limit
        return PagedResult(results=ids, next_page_token=next_page_token)

    def add(self, data: Iterable[TValue]) -> int:
        """Add data not present in storage.

        Args:
            data (iterable): dictionaries with keys:

              - **id**: sha1
              - **indexer_configuration_id**: tool used to compute the
                results
              - arbitrary data

        """
        data = list(data)
        check_id_duplicates(data)
        object_type = self.row_class.object_type  # type: ignore
        self._journal_writer.write_additions(object_type, data)
        count = 0
        for obj in data:
            item = obj.to_dict()
            id_ = item.pop("id")
            tool_id = item["indexer_configuration_id"]
            key = _key_from_dict(obj.unique_key())
            self._data[id_][key] = item
            self._tools_per_id[id_].add(tool_id)
            count += 1
            if id_ not in self._sorted_ids:
                self._sorted_ids.add(id_)
        return count


class IndexerStorage:
    """In-memory SWH indexer storage."""

    def __init__(self, journal_writer=None):
        self._tools = {}

        def tool_getter(id_):
            tool = self._tools[id_]
            return {
                "id": tool["id"],
                "name": tool["tool_name"],
                "version": tool["tool_version"],
                "configuration": tool["tool_configuration"],
            }

        self.journal_writer = JournalWriter(tool_getter, journal_writer)
        args = (self._tools, self.journal_writer)
        self._mimetypes = SubStorage(ContentMimetypeRow, *args)
        self._languages = SubStorage(ContentLanguageRow, *args)
        self._content_ctags = SubStorage(ContentCtagsRow, *args)
        self._licenses = SubStorage(ContentLicenseRow, *args)
        self._content_metadata = SubStorage(ContentMetadataRow, *args)
        self._directory_intrinsic_metadata = SubStorage(
            DirectoryIntrinsicMetadataRow, *args
        )
        self._origin_intrinsic_metadata = SubStorage(OriginIntrinsicMetadataRow, *args)
        self._origin_extrinsic_metadata = SubStorage(OriginExtrinsicMetadataRow, *args)

    def check_config(self, *, check_write):
        return True

    def content_mimetype_missing(
        self, mimetypes: Iterable[Dict]
    ) -> List[Tuple[Sha1, int]]:
        return self._mimetypes.missing(mimetypes)

    def content_mimetype_get_partition(
        self,
        indexer_configuration_id: int,
        partition_id: int,
        nb_partitions: int,
        page_token: Optional[str] = None,
        limit: int = 1000,
    ) -> PagedResult[Sha1]:
        return self._mimetypes.get_partition(
            indexer_configuration_id, partition_id, nb_partitions, page_token, limit
        )

    def content_mimetype_add(
        self, mimetypes: List[ContentMimetypeRow]
    ) -> Dict[str, int]:
        added = self._mimetypes.add(mimetypes)
        return {"content_mimetype:add": added}

    def content_mimetype_get(self, ids: Iterable[Sha1]) -> List[ContentMimetypeRow]:
        return self._mimetypes.get(ids)

    def content_language_missing(
        self, languages: Iterable[Dict]
    ) -> List[Tuple[Sha1, int]]:
        return self._languages.missing(languages)

    def content_language_get(self, ids: Iterable[Sha1]) -> List[ContentLanguageRow]:
        return self._languages.get(ids)

    def content_language_add(
        self, languages: List[ContentLanguageRow]
    ) -> Dict[str, int]:
        added = self._languages.add(languages)
        return {"content_language:add": added}

    def content_ctags_missing(self, ctags: Iterable[Dict]) -> List[Tuple[Sha1, int]]:
        return self._content_ctags.missing(ctags)

    def content_ctags_get(self, ids: Iterable[Sha1]) -> List[ContentCtagsRow]:
        return self._content_ctags.get(ids)

    def content_ctags_add(self, ctags: List[ContentCtagsRow]) -> Dict[str, int]:
        added = self._content_ctags.add(ctags)
        return {"content_ctags:add": added}

    def content_ctags_search(
        self, expression: str, limit: int = 10, last_sha1: Optional[Sha1] = None
    ) -> List[ContentCtagsRow]:
        nb_matches = 0
        items_per_id: Dict[Tuple[Sha1Git, ToolId], List[ContentCtagsRow]] = {}
        for item in sorted(self._content_ctags.get_all()):
            if item.id <= (last_sha1 or bytes(0 for _ in range(SHA1_DIGEST_SIZE))):
                continue
            items_per_id.setdefault(
                (item.id, item.indexer_configuration_id), []
            ).append(item)

        results = []
        for items in items_per_id.values():
            for item in items:
                if item.name != expression:
                    continue
                nb_matches += 1
                if nb_matches > limit:
                    break
                results.append(item)

        return results

    def content_fossology_license_get(
        self, ids: Iterable[Sha1]
    ) -> List[ContentLicenseRow]:
        return self._licenses.get(ids)

    def content_fossology_license_add(
        self, licenses: List[ContentLicenseRow]
    ) -> Dict[str, int]:
        added = self._licenses.add(licenses)
        return {"content_fossology_license:add": added}

    def content_fossology_license_get_partition(
        self,
        indexer_configuration_id: int,
        partition_id: int,
        nb_partitions: int,
        page_token: Optional[str] = None,
        limit: int = 1000,
    ) -> PagedResult[Sha1]:
        return self._licenses.get_partition(
            indexer_configuration_id, partition_id, nb_partitions, page_token, limit
        )

    def content_metadata_missing(
        self, metadata: Iterable[Dict]
    ) -> List[Tuple[Sha1, int]]:
        return self._content_metadata.missing(metadata)

    def content_metadata_get(self, ids: Iterable[Sha1]) -> List[ContentMetadataRow]:
        return self._content_metadata.get(ids)

    def content_metadata_add(
        self, metadata: List[ContentMetadataRow]
    ) -> Dict[str, int]:
        added = self._content_metadata.add(metadata)
        return {"content_metadata:add": added}

    def directory_intrinsic_metadata_missing(
        self, metadata: Iterable[Dict]
    ) -> List[Tuple[Sha1, int]]:
        return self._directory_intrinsic_metadata.missing(metadata)

    def directory_intrinsic_metadata_get(
        self, ids: Iterable[Sha1]
    ) -> List[DirectoryIntrinsicMetadataRow]:
        return self._directory_intrinsic_metadata.get(ids)

    def directory_intrinsic_metadata_add(
        self, metadata: List[DirectoryIntrinsicMetadataRow]
    ) -> Dict[str, int]:
        added = self._directory_intrinsic_metadata.add(metadata)
        return {"directory_intrinsic_metadata:add": added}

    def origin_intrinsic_metadata_get(
        self, urls: Iterable[str]
    ) -> List[OriginIntrinsicMetadataRow]:
        return self._origin_intrinsic_metadata.get(urls)

    def origin_intrinsic_metadata_add(
        self, metadata: List[OriginIntrinsicMetadataRow]
    ) -> Dict[str, int]:
        added = self._origin_intrinsic_metadata.add(metadata)
        return {"origin_intrinsic_metadata:add": added}

    def origin_intrinsic_metadata_search_fulltext(
        self, conjunction: List[str], limit: int = 100
    ) -> List[OriginIntrinsicMetadataRow]:
        # A very crude fulltext search implementation, but that's enough
        # to work on English metadata
        tokens_re = re.compile("[a-zA-Z0-9]+")
        search_tokens = list(itertools.chain(*map(tokens_re.findall, conjunction)))

        def rank(data):
            # Tokenize the metadata
            text = json.dumps(data.metadata)
            text_tokens = tokens_re.findall(text)
            text_token_occurences = Counter(text_tokens)

            # Count the number of occurrences of search tokens in the text
            score = 0
            for search_token in search_tokens:
                if text_token_occurences[search_token] == 0:
                    # Search token is not in the text.
                    return 0
                score += text_token_occurences[search_token]

            # Normalize according to the text's length
            return score / math.log(len(text_tokens))

        results = [
            (rank(data), data) for data in self._origin_intrinsic_metadata.get_all()
        ]
        results = [(rank_, data) for (rank_, data) in results if rank_ > 0]
        results.sort(
            key=operator.itemgetter(0), reverse=True  # Don't try to order 'data'
        )
        return [result for (rank_, result) in results[:limit]]

    def origin_intrinsic_metadata_search_by_producer(
        self,
        page_token: str = "",
        limit: int = 100,
        ids_only: bool = False,
        mappings: Optional[List[str]] = None,
        tool_ids: Optional[List[int]] = None,
    ) -> PagedResult[Union[str, OriginIntrinsicMetadataRow]]:
        assert isinstance(page_token, str)
        nb_results = 0
        if mappings is not None:
            mapping_set = frozenset(mappings)
        if tool_ids is not None:
            tool_id_set = frozenset(tool_ids)
        rows = []

        # we go to limit+1 to check whether we should add next_page_token in
        # the response
        for entry in self._origin_intrinsic_metadata.get_all():
            if entry.id <= page_token:
                continue
            if nb_results >= (limit + 1):
                break
            if mappings and mapping_set.isdisjoint(entry.mappings):
                continue
            if tool_ids and entry.tool["id"] not in tool_id_set:
                continue
            rows.append(entry)
            nb_results += 1

        if len(rows) > limit:
            rows = rows[:limit]
            next_page_token = rows[-1].id
        else:
            next_page_token = None
        if ids_only:
            rows = [row.id for row in rows]
        return PagedResult(
            results=rows,
            next_page_token=next_page_token,
        )

    def origin_intrinsic_metadata_stats(self):
        mapping_count = {m: 0 for m in MAPPING_NAMES}
        total = non_empty = 0
        for data in self._origin_intrinsic_metadata.get_all():
            total += 1
            if set(data.metadata) - {"@context"}:
                non_empty += 1
            for mapping in data.mappings:
                mapping_count[mapping] += 1
        return {"per_mapping": mapping_count, "total": total, "non_empty": non_empty}

    def origin_extrinsic_metadata_get(
        self, urls: Iterable[str]
    ) -> List[OriginExtrinsicMetadataRow]:
        return self._origin_extrinsic_metadata.get(urls)

    def origin_extrinsic_metadata_add(
        self, metadata: List[OriginExtrinsicMetadataRow]
    ) -> Dict[str, int]:
        added = self._origin_extrinsic_metadata.add(metadata)
        return {"origin_extrinsic_metadata:add": added}

    def indexer_configuration_add(self, tools):
        inserted = []
        for tool in tools:
            tool = tool.copy()
            id_ = self._tool_key(tool)
            tool["id"] = id_
            self._tools[id_] = tool
            inserted.append(tool)
        return inserted

    def indexer_configuration_get(self, tool):
        return self._tools.get(self._tool_key(tool))

    def _tool_key(self, tool):
        return hash(
            (
                tool["tool_name"],
                tool["tool_version"],
                json.dumps(tool["tool_configuration"], sort_keys=True),
            )
        )
