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
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
)

from swh.core.collections import SortedList
from swh.model.hashutil import hash_to_bytes, hash_to_hex
from swh.model.model import SHA1_SIZE, Sha1Git
from swh.storage.utils import get_partition_bounds_bytes

from . import MAPPING_NAMES, check_id_duplicates, converters
from .exc import IndexerStorageArgumentException
from .interface import PagedResult, Sha1
from .model import (
    BaseRow,
    ContentCtagsRow,
    ContentLanguageRow,
    ContentLicenseRow,
    ContentMetadataRow,
    ContentMimetypeRow,
    OriginIntrinsicMetadataRow,
    RevisionIntrinsicMetadataRow,
)

SHA1_DIGEST_SIZE = 160


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


ToolId = int
TValue = TypeVar("TValue", bound=BaseRow)


class SubStorage(Generic[TValue]):
    """Implements common missing/get/add logic for each indexer type."""

    _data: Dict[Sha1, Dict[Tuple, Dict[str, Any]]]
    _tools_per_id: Dict[Sha1, Set[ToolId]]

    def __init__(self, row_class: Type[TValue], tools):
        self.row_class = row_class
        self._tools = tools
        self._sorted_ids = SortedList[bytes, Sha1]()
        self._data = defaultdict(dict)
        self._tools_per_id = defaultdict(set)

    def _key_from_dict(self, d) -> Tuple:
        """Like the global _key_from_dict, but filters out dict keys that don't
        belong in the unique key."""
        return _key_from_dict({k: d[k] for k in self.row_class.UNIQUE_KEY_FIELDS})

    def missing(self, keys: Iterable[Dict]) -> Iterator[Sha1]:
        """List data missing from storage.

        Args:
            data (iterable): dictionaries with keys:

                - **id** (bytes): sha1 identifier
                - **indexer_configuration_id** (int): tool used to compute
                  the results

        Yields:
            missing sha1s

        """
        for key in keys:
            tool_id = key["indexer_configuration_id"]
            id_ = key["id"]
            if tool_id not in self._tools_per_id.get(id_, set()):
                yield id_

    def get(self, ids: Iterable[Sha1]) -> Iterator[TValue]:
        """Retrieve data per id.

        Args:
            ids (iterable): sha1 checksums

        Yields:
            dict: dictionaries with the following keys:

              - **id** (bytes)
              - **tool** (dict): tool used to compute metadata
              - arbitrary data (as provided to `add`)

        """
        for id_ in ids:
            for entry in self._data[id_].values():
                entry = entry.copy()
                tool_id = entry.pop("indexer_configuration_id")
                yield self.row_class(
                    id=id_, tool=_transform_tool(self._tools[tool_id]), **entry,
                )

    def get_all(self) -> Iterator[TValue]:
        yield from self.get(self._sorted_ids)

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

    def add(self, data: Iterable[TValue], conflict_update: bool) -> int:
        """Add data not present in storage.

        Args:
            data (iterable): dictionaries with keys:

              - **id**: sha1
              - **indexer_configuration_id**: tool used to compute the
                results
              - arbitrary data

            conflict_update (bool): Flag to determine if we want to overwrite
              (true) or skip duplicates (false)

        """
        data = list(data)
        check_id_duplicates(data)
        count = 0
        for obj in data:
            item = obj.to_dict()
            id_ = item.pop("id")
            tool_id = item["indexer_configuration_id"]
            key = _key_from_dict(obj.unique_key())
            if not conflict_update and key in self._data[id_]:
                # Duplicate, should not be updated
                continue
            self._data[id_][key] = item
            self._tools_per_id[id_].add(tool_id)
            count += 1
            if id_ not in self._sorted_ids:
                self._sorted_ids.add(id_)
        return count

    def delete(self, entries: List[Dict]) -> int:
        """Delete entries and return the number of entries deleted.

        """
        deleted = 0
        for entry in entries:
            (id_, tool_id) = (entry["id"], entry["indexer_configuration_id"])
            if tool_id in self._tools_per_id[id_]:
                self._tools_per_id[id_].remove(tool_id)
            if id_ in self._data:
                key = self._key_from_dict(entry)
                if key in self._data[id_]:
                    deleted += 1
                    del self._data[id_][key]
        return deleted


class IndexerStorage:
    """In-memory SWH indexer storage."""

    def __init__(self):
        self._tools = {}
        self._mimetypes = SubStorage(ContentMimetypeRow, self._tools)
        self._languages = SubStorage(ContentLanguageRow, self._tools)
        self._content_ctags = SubStorage(ContentCtagsRow, self._tools)
        self._licenses = SubStorage(ContentLicenseRow, self._tools)
        self._content_metadata = SubStorage(ContentMetadataRow, self._tools)
        self._revision_intrinsic_metadata = SubStorage(
            RevisionIntrinsicMetadataRow, self._tools
        )
        self._origin_intrinsic_metadata = SubStorage(
            OriginIntrinsicMetadataRow, self._tools
        )

    def check_config(self, *, check_write):
        return True

    def content_mimetype_missing(self, mimetypes):
        yield from self._mimetypes.missing(mimetypes)

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
        self, mimetypes: List[Dict], conflict_update: bool = False
    ) -> Dict[str, int]:
        check_id_types(mimetypes)
        added = self._mimetypes.add(
            map(ContentMimetypeRow.from_dict, mimetypes), conflict_update
        )
        return {"content_mimetype:add": added}

    def content_mimetype_get(self, ids):
        yield from (obj.to_dict() for obj in self._mimetypes.get(ids))

    def content_language_missing(self, languages):
        yield from self._languages.missing(languages)

    def content_language_get(self, ids):
        yield from (obj.to_dict() for obj in self._languages.get(ids))

    def content_language_add(
        self, languages: List[Dict], conflict_update: bool = False
    ) -> Dict[str, int]:
        check_id_types(languages)
        added = self._languages.add(
            map(ContentLanguageRow.from_dict, languages), conflict_update
        )
        return {"content_language:add": added}

    def content_ctags_missing(self, ctags):
        yield from self._content_ctags.missing(ctags)

    def content_ctags_get(self, ids):
        for item in self._content_ctags.get(ids):
            yield {"id": item.id, "tool": item.tool, **item.to_dict()}

    def content_ctags_add(
        self, ctags: List[Dict], conflict_update: bool = False
    ) -> Dict[str, int]:
        check_id_types(ctags)
        added = self._content_ctags.add(
            map(
                ContentCtagsRow.from_dict,
                itertools.chain.from_iterable(map(converters.ctags_to_db, ctags)),
            ),
            conflict_update,
        )
        return {"content_ctags:add": added}

    def content_ctags_search(self, expression, limit=10, last_sha1=None):
        nb_matches = 0
        items_per_id: Dict[Tuple[Sha1Git, ToolId], List[ContentCtagsRow]] = {}
        for item in sorted(self._content_ctags.get_all()):
            if item.id <= (last_sha1 or bytes(0 for _ in range(SHA1_DIGEST_SIZE))):
                continue
            items_per_id.setdefault(
                (item.id, item.indexer_configuration_id), []
            ).append(item)

        for items in items_per_id.values():
            ctags = []
            for item in items:
                if item.name != expression:
                    continue
                nb_matches += 1
                if nb_matches > limit:
                    break
                item_dict = item.to_dict()
                id_ = item_dict.pop("id")
                tool = item_dict.pop("tool")
                ctags.append(item_dict)

            if ctags:
                for ctag in ctags:
                    yield {"id": id_, "tool": tool, **ctag}

    def content_fossology_license_get(self, ids):
        # Rewrites the output of SubStorage.get from the old format to
        # the new one. SubStorage.get should be updated once all other
        # *_get methods use the new format.
        # See: https://forge.softwareheritage.org/T1433
        for id_ in ids:
            items = {}
            for obj in self._licenses.get([id_]):
                items.setdefault(obj.tool["id"], (obj.tool, []))[1].append(obj.license)
            if items:
                yield {
                    id_: [
                        {"tool": tool, "licenses": licenses}
                        for (tool, licenses) in items.values()
                    ]
                }

    def content_fossology_license_add(
        self, licenses: List[Dict], conflict_update: bool = False
    ) -> Dict[str, int]:
        check_id_types(licenses)
        added = self._licenses.add(
            map(
                ContentLicenseRow.from_dict,
                itertools.chain.from_iterable(
                    map(converters.fossology_license_to_db, licenses)
                ),
            ),
            conflict_update,
        )
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

    def content_metadata_missing(self, metadata):
        yield from self._content_metadata.missing(metadata)

    def content_metadata_get(self, ids):
        yield from (obj.to_dict() for obj in self._content_metadata.get(ids))

    def content_metadata_add(
        self, metadata: List[Dict], conflict_update: bool = False
    ) -> Dict[str, int]:
        check_id_types(metadata)
        added = self._content_metadata.add(
            map(ContentMetadataRow.from_dict, metadata), conflict_update
        )
        return {"content_metadata:add": added}

    def revision_intrinsic_metadata_missing(self, metadata):
        yield from self._revision_intrinsic_metadata.missing(metadata)

    def revision_intrinsic_metadata_get(self, ids):
        yield from (obj.to_dict() for obj in self._revision_intrinsic_metadata.get(ids))

    def revision_intrinsic_metadata_add(
        self, metadata: List[Dict], conflict_update: bool = False
    ) -> Dict[str, int]:
        check_id_types(metadata)
        added = self._revision_intrinsic_metadata.add(
            map(RevisionIntrinsicMetadataRow.from_dict, metadata), conflict_update
        )
        return {"revision_intrinsic_metadata:add": added}

    def revision_intrinsic_metadata_delete(self, entries: List[Dict]) -> Dict:
        deleted = self._revision_intrinsic_metadata.delete(entries)
        return {"revision_intrinsic_metadata:del": deleted}

    def origin_intrinsic_metadata_get(self, ids):
        yield from (obj.to_dict() for obj in self._origin_intrinsic_metadata.get(ids))

    def origin_intrinsic_metadata_add(
        self, metadata: List[Dict], conflict_update: bool = False
    ) -> Dict[str, int]:
        added = self._origin_intrinsic_metadata.add(
            map(OriginIntrinsicMetadataRow.from_dict, metadata), conflict_update
        )
        return {"origin_intrinsic_metadata:add": added}

    def origin_intrinsic_metadata_delete(self, entries: List[Dict]) -> Dict:
        deleted = self._origin_intrinsic_metadata.delete(entries)
        return {"origin_intrinsic_metadata:del": deleted}

    def origin_intrinsic_metadata_search_fulltext(self, conjunction, limit=100):
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
        for (rank_, result) in results[:limit]:
            yield result.to_dict()

    def origin_intrinsic_metadata_search_by_producer(
        self, page_token="", limit=100, ids_only=False, mappings=None, tool_ids=None
    ):
        assert isinstance(page_token, str)
        nb_results = 0
        if mappings is not None:
            mappings = frozenset(mappings)
        if tool_ids is not None:
            tool_ids = frozenset(tool_ids)
        origins = []

        # we go to limit+1 to check whether we should add next_page_token in
        # the response
        for entry in self._origin_intrinsic_metadata.get_all():
            if entry.id <= page_token:
                continue
            if nb_results >= (limit + 1):
                break
            if mappings is not None and mappings.isdisjoint(entry.mappings):
                continue
            if tool_ids is not None and entry.tool["id"] not in tool_ids:
                continue
            origins.append(entry.to_dict())
            nb_results += 1

        result = {}
        if len(origins) > limit:
            origins = origins[:limit]
            result["next_page_token"] = origins[-1]["id"]
        if ids_only:
            origins = [origin["id"] for origin in origins]
        result["origins"] = origins
        return result

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
