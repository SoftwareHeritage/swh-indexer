# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import bisect
from collections import defaultdict, Counter
import itertools
import json
import operator
import math
import re
from typing import Any, Dict, List

from . import MAPPING_NAMES, check_id_duplicates
from .exc import IndexerStorageArgumentException

SHA1_DIGEST_SIZE = 160


def _transform_tool(tool):
    return {
        'id': tool['id'],
        'name': tool['tool_name'],
        'version': tool['tool_version'],
        'configuration': tool['tool_configuration'],
    }


def check_id_types(data: List[Dict[str, Any]]):
    """Checks all elements of the list have an 'id' whose type is 'bytes'."""
    if not all(isinstance(item.get('id'), bytes) for item in data):
        raise IndexerStorageArgumentException('identifiers must be bytes.')


class SubStorage:
    """Implements common missing/get/add logic for each indexer type."""
    def __init__(self, tools):
        self._tools = tools
        self._sorted_ids = []
        self._data = {}  # map (id_, tool_id) -> metadata_dict
        self._tools_per_id = defaultdict(set)  # map id_ -> Set[tool_id]

    def missing(self, ids):
        """List data missing from storage.

        Args:
            data (iterable): dictionaries with keys:

                - **id** (bytes): sha1 identifier
                - **indexer_configuration_id** (int): tool used to compute
                  the results

        Yields:
            missing sha1s

        """
        for id_ in ids:
            tool_id = id_['indexer_configuration_id']
            id_ = id_['id']
            if tool_id not in self._tools_per_id.get(id_, set()):
                yield id_

    def get(self, ids):
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
            for tool_id in self._tools_per_id.get(id_, set()):
                key = (id_, tool_id)
                yield {
                    'id': id_,
                    'tool': _transform_tool(self._tools[tool_id]),
                    **self._data[key],
                }

    def get_all(self):
        yield from self.get(self._sorted_ids)

    def get_range(self, start, end, indexer_configuration_id, limit):
        """Retrieve data within range [start, end] bound by limit.

        Args:
            **start** (bytes): Starting identifier range (expected smaller
                           than end)
            **end** (bytes): Ending identifier range (expected larger
                             than start)
            **indexer_configuration_id** (int): The tool used to index data
            **limit** (int): Limit result

        Raises:
            IndexerStorageArgumentException for limit to None

        Returns:
            a dict with keys:
            - **ids** [bytes]: iterable of content ids within the range.
            - **next** (Optional[bytes]): The next range of sha1 starts at
                                          this sha1 if any

        """
        if limit is None:
            raise IndexerStorageArgumentException('limit should not be None')
        from_index = bisect.bisect_left(self._sorted_ids, start)
        to_index = bisect.bisect_right(self._sorted_ids, end, lo=from_index)
        if to_index - from_index >= limit:
            return {
                'ids': self._sorted_ids[from_index:from_index+limit],
                'next': self._sorted_ids[from_index+limit],
            }
        else:
            return {
                'ids': self._sorted_ids[from_index:to_index],
                'next': None,
                }

    def add(self, data, conflict_update):
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
        for item in data:
            item = item.copy()
            tool_id = item.pop('indexer_configuration_id')
            id_ = item.pop('id')
            data = item
            if not conflict_update and \
                    tool_id in self._tools_per_id.get(id_, set()):
                # Duplicate, should not be updated
                continue
            key = (id_, tool_id)
            self._data[key] = data
            self._tools_per_id[id_].add(tool_id)
            if id_ not in self._sorted_ids:
                bisect.insort(self._sorted_ids, id_)

    def add_merge(self, new_data, conflict_update, merged_key):
        for new_item in new_data:
            id_ = new_item['id']
            tool_id = new_item['indexer_configuration_id']
            if conflict_update:
                all_subitems = []
            else:
                existing = list(self.get([id_]))
                all_subitems = [
                    old_subitem
                    for existing_item in existing
                    if existing_item['tool']['id'] == tool_id
                    for old_subitem in existing_item[merged_key]
                ]
            for new_subitem in new_item[merged_key]:
                if new_subitem not in all_subitems:
                    all_subitems.append(new_subitem)
            self.add([
                {
                    'id': id_,
                    'indexer_configuration_id': tool_id,
                    merged_key: all_subitems,
                }
            ], conflict_update=True)
            if id_ not in self._sorted_ids:
                bisect.insort(self._sorted_ids, id_)

    def delete(self, entries):
        for entry in entries:
            (id_, tool_id) = (entry['id'], entry['indexer_configuration_id'])
            key = (id_, tool_id)
            if tool_id in self._tools_per_id[id_]:
                self._tools_per_id[id_].remove(tool_id)
            if key in self._data:
                del self._data[key]


class IndexerStorage:
    """In-memory SWH indexer storage."""

    def __init__(self):
        self._tools = {}
        self._mimetypes = SubStorage(self._tools)
        self._languages = SubStorage(self._tools)
        self._content_ctags = SubStorage(self._tools)
        self._licenses = SubStorage(self._tools)
        self._content_metadata = SubStorage(self._tools)
        self._revision_intrinsic_metadata = SubStorage(self._tools)
        self._origin_intrinsic_metadata = SubStorage(self._tools)

    def check_config(self, *, check_write):
        return True

    def content_mimetype_missing(self, mimetypes):
        yield from self._mimetypes.missing(mimetypes)

    def content_mimetype_get_range(
            self, start, end, indexer_configuration_id, limit=1000):
        return self._mimetypes.get_range(
            start, end, indexer_configuration_id, limit)

    def content_mimetype_add(self, mimetypes, conflict_update=False):
        check_id_types(mimetypes)
        self._mimetypes.add(mimetypes, conflict_update)

    def content_mimetype_get(self, ids):
        yield from self._mimetypes.get(ids)

    def content_language_missing(self, languages):
        yield from self._languages.missing(languages)

    def content_language_get(self, ids):
        yield from self._languages.get(ids)

    def content_language_add(self, languages, conflict_update=False):
        check_id_types(languages)
        self._languages.add(languages, conflict_update)

    def content_ctags_missing(self, ctags):
        yield from self._content_ctags.missing(ctags)

    def content_ctags_get(self, ids):
        for item in self._content_ctags.get(ids):
            for item_ctags_item in item['ctags']:
                yield {
                    'id': item['id'],
                    'tool': item['tool'],
                    **item_ctags_item
                }

    def content_ctags_add(self, ctags, conflict_update=False):
        check_id_types(ctags)
        self._content_ctags.add_merge(ctags, conflict_update, 'ctags')

    def content_ctags_search(self, expression,
                             limit=10, last_sha1=None):
        nb_matches = 0
        for ((id_, tool_id), item) in \
                sorted(self._content_ctags._data.items()):
            if id_ <= (last_sha1 or bytes(0 for _ in range(SHA1_DIGEST_SIZE))):
                continue
            for ctags_item in item['ctags']:
                if ctags_item['name'] != expression:
                    continue
                nb_matches += 1
                yield {
                    'id': id_,
                    'tool': _transform_tool(self._tools[tool_id]),
                    **ctags_item
                }
                if nb_matches >= limit:
                    return

    def content_fossology_license_get(self, ids):
        # Rewrites the output of SubStorage.get from the old format to
        # the new one. SubStorage.get should be updated once all other
        # *_get methods use the new format.
        # See: https://forge.softwareheritage.org/T1433
        res = {}
        for d in self._licenses.get(ids):
            res.setdefault(d.pop('id'), []).append(d)
        for (id_, facts) in res.items():
            yield {id_: facts}

    def content_fossology_license_add(self, licenses, conflict_update=False):
        check_id_types(licenses)
        self._licenses.add_merge(licenses, conflict_update, 'licenses')

    def content_fossology_license_get_range(
            self, start, end, indexer_configuration_id, limit=1000):
        return self._licenses.get_range(
            start, end, indexer_configuration_id, limit)

    def content_metadata_missing(self, metadata):
        yield from self._content_metadata.missing(metadata)

    def content_metadata_get(self, ids):
        yield from self._content_metadata.get(ids)

    def content_metadata_add(self, metadata, conflict_update=False):
        check_id_types(metadata)
        self._content_metadata.add(metadata, conflict_update)

    def revision_intrinsic_metadata_missing(self, metadata):
        yield from self._revision_intrinsic_metadata.missing(metadata)

    def revision_intrinsic_metadata_get(self, ids):
        yield from self._revision_intrinsic_metadata.get(ids)

    def revision_intrinsic_metadata_add(self, metadata, conflict_update=False):
        check_id_types(metadata)
        self._revision_intrinsic_metadata.add(metadata, conflict_update)

    def revision_intrinsic_metadata_delete(self, entries):
        self._revision_intrinsic_metadata.delete(entries)

    def origin_intrinsic_metadata_get(self, ids):
        yield from self._origin_intrinsic_metadata.get(ids)

    def origin_intrinsic_metadata_add(self, metadata,
                                      conflict_update=False):
        self._origin_intrinsic_metadata.add(metadata, conflict_update)

    def origin_intrinsic_metadata_delete(self, entries):
        self._origin_intrinsic_metadata.delete(entries)

    def origin_intrinsic_metadata_search_fulltext(
            self, conjunction, limit=100):
        # A very crude fulltext search implementation, but that's enough
        # to work on English metadata
        tokens_re = re.compile('[a-zA-Z0-9]+')
        search_tokens = list(itertools.chain(
            *map(tokens_re.findall, conjunction)))

        def rank(data):
            # Tokenize the metadata
            text = json.dumps(data['metadata'])
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

        results = [(rank(data), data)
                   for data in self._origin_intrinsic_metadata.get_all()]
        results = [(rank_, data) for (rank_, data) in results if rank_ > 0]
        results.sort(key=operator.itemgetter(0),  # Don't try to order 'data'
                     reverse=True)
        for (rank_, result) in results[:limit]:
            yield result

    def origin_intrinsic_metadata_search_by_producer(
            self, page_token='', limit=100, ids_only=False,
            mappings=None, tool_ids=None):
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
            if entry['id'] <= page_token:
                continue
            if nb_results >= (limit + 1):
                break
            if mappings is not None and mappings.isdisjoint(entry['mappings']):
                continue
            if tool_ids is not None and entry['tool']['id'] not in tool_ids:
                continue
            origins.append(entry)
            nb_results += 1

        result = {}
        if len(origins) > limit:
            origins = origins[:limit]
            result['next_page_token'] = origins[-1]['id']
        if ids_only:
            origins = [origin['id'] for origin in origins]
        result['origins'] = origins
        return result

    def origin_intrinsic_metadata_stats(self):
        mapping_count = {m: 0 for m in MAPPING_NAMES}
        total = non_empty = 0
        for data in self._origin_intrinsic_metadata.get_all():
            total += 1
            if set(data['metadata']) - {'@context'}:
                non_empty += 1
            for mapping in data['mappings']:
                mapping_count[mapping] += 1
        return {
            'per_mapping': mapping_count,
            'total': total,
            'non_empty': non_empty
        }

    def indexer_configuration_add(self, tools):
        inserted = []
        for tool in tools:
            tool = tool.copy()
            id_ = self._tool_key(tool)
            tool['id'] = id_
            self._tools[id_] = tool
            inserted.append(tool)
        return inserted

    def indexer_configuration_get(self, tool):
        return self._tools.get(self._tool_key(tool))

    def _tool_key(self, tool):
        return hash((tool['tool_name'], tool['tool_version'],
                     json.dumps(tool['tool_configuration'], sort_keys=True)))
