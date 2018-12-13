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

SHA1_DIGEST_SIZE = 160


def _transform_tool(tool):
    return {
        'id': tool['id'],
        'name': tool['tool_name'],
        'version': tool['tool_version'],
        'configuration': tool['tool_configuration'],
    }


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
        yield from self.get(list(self._tools_per_id))

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
            ValueError for limit to None

        Returns:
            a dict with keys:
            - **ids** [bytes]: iterable of content ids within the range.
            - **next** (Optional[bytes]): The next range of sha1 starts at
                                          this sha1 if any

        """
        if limit is None:
            raise ValueError('Development error: limit should not be None')
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


class IndexerStorage:
    """In-memory SWH indexer storage."""

    def __init__(self):
        self._tools = {}
        self._mimetypes = SubStorage(self._tools)
        self._languages = SubStorage(self._tools)
        self._content_ctags = SubStorage(self._tools)
        self._licenses = SubStorage(self._tools)
        self._content_metadata = SubStorage(self._tools)
        self._revision_metadata = SubStorage(self._tools)
        self._origin_intrinsic_metadata = SubStorage(self._tools)

    def content_mimetype_missing(self, mimetypes):
        """Generate mimetypes missing from storage.

        Args:
            mimetypes (iterable): iterable of dict with keys:

              - **id** (bytes): sha1 identifier
              - **indexer_configuration_id** (int): tool used to compute the
                results

        Yields:
            tuple (id, indexer_configuration_id): missing id

        """
        yield from self._mimetypes.missing(mimetypes)

    def content_mimetype_get_range(
            self, start, end, indexer_configuration_id, limit=1000):
        """Retrieve mimetypes within range [start, end] bound by limit.

        Args:
            **start** (bytes): Starting identifier range (expected smaller
                           than end)
            **end** (bytes): Ending identifier range (expected larger
                             than start)
            **indexer_configuration_id** (int): The tool used to index data
            **limit** (int): Limit result (default to 1000)

        Raises:
            ValueError for limit to None

        Returns:
            a dict with keys:
            - **ids** [bytes]: iterable of content ids within the range.
            - **next** (Optional[bytes]): The next range of sha1 starts at
                                          this sha1 if any

        """
        return self._mimetypes.get_range(
            start, end, indexer_configuration_id, limit)

    def content_mimetype_add(self, mimetypes, conflict_update=False):
        """Add mimetypes not present in storage.

        Args:
            mimetypes (iterable): dictionaries with keys:

              - **id** (bytes): sha1 identifier
              - **mimetype** (bytes): raw content's mimetype
              - **encoding** (bytes): raw content's encoding
              - **indexer_configuration_id** (int): tool's id used to
                compute the results
              - **conflict_update** (bool): Flag to determine if we want to
                overwrite (``True``) or skip duplicates (``False``, the
                default)

        """
        if not all(isinstance(x['id'], bytes) for x in mimetypes):
            raise TypeError('identifiers must be bytes.')
        self._mimetypes.add(mimetypes, conflict_update)

    def content_mimetype_get(self, ids, db=None, cur=None):
        """Retrieve full content mimetype per ids.

        Args:
            ids (iterable): sha1 identifier

        Yields:
            mimetypes (iterable): dictionaries with keys:

                - **id** (bytes): sha1 identifier
                - **mimetype** (bytes): raw content's mimetype
                - **encoding** (bytes): raw content's encoding
                - **tool** (dict): Tool used to compute the language

        """
        yield from self._mimetypes.get(ids)

    def content_language_missing(self, languages):
        """List languages missing from storage.

        Args:
            languages (iterable): dictionaries with keys:

                - **id** (bytes): sha1 identifier
                - **indexer_configuration_id** (int): tool used to compute
                  the results

        Yields:
            an iterable of missing id for the tuple (id,
            indexer_configuration_id)

        """
        yield from self._languages.missing(languages)

    def content_language_get(self, ids):
        """Retrieve full content language per ids.

        Args:
            ids (iterable): sha1 identifier

        Yields:
            languages (iterable): dictionaries with keys:

                - **id** (bytes): sha1 identifier
                - **lang** (bytes): raw content's language
                - **tool** (dict): Tool used to compute the language

        """
        yield from self._languages.get(ids)

    def content_language_add(self, languages, conflict_update=False):
        """Add languages not present in storage.

        Args:
            languages (iterable): dictionaries with keys:

                - **id** (bytes): sha1
                - **lang** (bytes): language detected

            conflict_update (bool): Flag to determine if we want to
                overwrite (true) or skip duplicates (false, the
                default)

        """
        self._languages.add(languages, conflict_update)

    def content_ctags_missing(self, ctags):
        """List ctags missing from storage.

        Args:
            ctags (iterable): dicts with keys:

                - **id** (bytes): sha1 identifier
                - **indexer_configuration_id** (int): tool used to compute
                  the results

        Yields:
            an iterable of missing id for the tuple (id,
            indexer_configuration_id)

        """
        yield from self._content_ctags.missing(ctags)

    def content_ctags_get(self, ids):
        """Retrieve ctags per id.

        Args:
            ids (iterable): sha1 checksums

        Yields:
            Dictionaries with keys:

                - **id** (bytes): content's identifier
                - **name** (str): symbol's name
                - **kind** (str): symbol's kind
                - **lang** (str): language for that content
                - **tool** (dict): tool used to compute the ctags' info


        """
        for item in self._content_ctags.get(ids):
            for item_ctags_item in item['ctags']:
                yield {
                    'id': item['id'],
                    'tool': item['tool'],
                    **item_ctags_item
                }

    def content_ctags_add(self, ctags, conflict_update=False):
        """Add ctags not present in storage

        Args:
            ctags (iterable): dictionaries with keys:

              - **id** (bytes): sha1
              - **ctags** ([list): List of dictionary with keys: name, kind,
                  line, lang
              - **indexer_configuration_id**: tool used to compute the
                results

        """
        if not all(isinstance(x['id'], bytes) for x in ctags):
            raise TypeError('identifiers must be bytes.')
        self._content_ctags.add_merge(ctags, conflict_update, 'ctags')

    def content_ctags_search(self, expression,
                             limit=10, last_sha1=None, db=None, cur=None):
        """Search through content's raw ctags symbols.

        Args:
            expression (str): Expression to search for
            limit (int): Number of rows to return (default to 10).
            last_sha1 (str): Offset from which retrieving data (default to '').

        Yields:
            rows of ctags including id, name, lang, kind, line, etc...

        """
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
        """Retrieve licenses per id.

        Args:
            ids (iterable): sha1 checksums

        Yields:
            `{id: facts}` where `facts` is a dict with the following keys:

                - **licenses** ([str]): associated licenses for that content
                - **tool** (dict): Tool used to compute the license

        """
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
        """Add licenses not present in storage.

        Args:
            licenses (iterable): dictionaries with keys:

                - **id**: sha1
                - **licenses** ([bytes]): List of licenses associated to sha1
                - **tool** (str): nomossa

            conflict_update: Flag to determine if we want to overwrite (true)
                or skip duplicates (false, the default)

        Returns:
            list: content_license entries which failed due to unknown licenses

        """
        if not all(isinstance(x['id'], bytes) for x in licenses):
            raise TypeError('identifiers must be bytes.')
        self._licenses.add_merge(licenses, conflict_update, 'licenses')

    def content_fossology_license_get_range(
            self, start, end, indexer_configuration_id, limit=1000):
        """Retrieve licenses within range [start, end] bound by limit.

        Args:
            **start** (bytes): Starting identifier range (expected smaller
                           than end)
            **end** (bytes): Ending identifier range (expected larger
                             than start)
            **indexer_configuration_id** (int): The tool used to index data
            **limit** (int): Limit result (default to 1000)

        Raises:
            ValueError for limit to None

        Returns:
            a dict with keys:
            - **ids** [bytes]: iterable of content ids within the range.
            - **next** (Optional[bytes]): The next range of sha1 starts at
                                          this sha1 if any

        """
        return self._licenses.get_range(
            start, end, indexer_configuration_id, limit)

    def content_metadata_missing(self, metadata):
        """List metadata missing from storage.

        Args:
            metadata (iterable): dictionaries with keys:

              - **id** (bytes): sha1 identifier
              - **indexer_configuration_id** (int): tool used to compute
                the results

        Yields:
            missing sha1s

        """
        yield from self._content_metadata.missing(metadata)

    def content_metadata_get(self, ids):
        """Retrieve metadata per id.

        Args:
            ids (iterable): sha1 checksums

        Yields:
            dictionaries with the following keys:

              - **id** (bytes)
              - **translated_metadata** (str): associated metadata
              - **tool** (dict): tool used to compute metadata

        """
        yield from self._content_metadata.get(ids)

    def content_metadata_add(self, metadata, conflict_update=False):
        """Add metadata not present in storage.

        Args:
            metadata (iterable): dictionaries with keys:

              - **id**: sha1
              - **translated_metadata**: arbitrary dict
              - **indexer_configuration_id**: tool used to compute the
                results

            conflict_update: Flag to determine if we want to overwrite (true)
                or skip duplicates (false, the default)

        """
        if not all(isinstance(x['id'], bytes) for x in metadata):
            raise TypeError('identifiers must be bytes.')
        self._content_metadata.add(metadata, conflict_update)

    def revision_metadata_missing(self, metadata):
        """List metadata missing from storage.

        Args:
            metadata (iterable): dictionaries with keys:

              - **id** (bytes): sha1_git revision identifier
              - **indexer_configuration_id** (int): tool used to compute
                the results

        Yields:
            missing ids

        """
        yield from self._revision_metadata.missing(metadata)

    def revision_metadata_get(self, ids):
        """Retrieve revision metadata per id.

        Args:
            ids (iterable): sha1 checksums

        Yields:
            dictionaries with the following keys:

            - **id** (bytes)
            - **translated_metadata** (str): associated metadata
            - **tool** (dict): tool used to compute metadata

        """
        yield from self._revision_metadata.get(ids)

    def revision_metadata_add(self, metadata, conflict_update=False):
        """Add metadata not present in storage.

        Args:
            metadata (iterable): dictionaries with keys:

              - **id**: sha1_git of revision
              - **translated_metadata**: arbitrary dict
              - **indexer_configuration_id**: tool used to compute metadata

            conflict_update: Flag to determine if we want to overwrite (true)
              or skip duplicates (false, the default)

        """
        if not all(isinstance(x['id'], bytes) for x in metadata):
            raise TypeError('identifiers must be bytes.')
        self._revision_metadata.add(metadata, conflict_update)

    def origin_intrinsic_metadata_get(self, ids):
        """Retrieve origin metadata per id.

        Args:
            ids (iterable): origin identifiers

        Yields:
            list: dictionaries with the following keys:

                - **origin_id** (int)
                - **translated_metadata** (str): associated metadata
                - **tool** (dict): tool used to compute metadata

        """
        for item in self._origin_intrinsic_metadata.get(ids):
            item['origin_id'] = item.pop('id')
            yield item

    def origin_intrinsic_metadata_add(self, metadata,
                                      conflict_update=False):
        """Add origin metadata not present in storage.

        Args:
            metadata (iterable): dictionaries with keys:

                - **origin_id**: origin identifier
                - **from_revision**: sha1 id of the revision used to generate
                  these metadata.
                - **metadata**: arbitrary dict
                - **indexer_configuration_id**: tool used to compute metadata

            conflict_update: Flag to determine if we want to overwrite (true)
              or skip duplicates (false, the default)

        """

        for item in metadata:
            item = item.copy()
            item['id'] = item.pop('origin_id')
            self._origin_intrinsic_metadata.add([item], conflict_update)

    def origin_intrinsic_metadata_search_fulltext(
            self, conjunction, limit=100):
        """Returns the list of origins whose metadata contain all the terms.

        Args:
            conjunction (List[str]): List of terms to be searched for.
            limit (int): The maximum number of results to return

        Yields:
            list: dictionaries with the following keys:

                - **id** (int)
                - **metadata** (str): associated metadata
                - **tool** (dict): tool used to compute metadata

        """
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

            # Count the number of occurences of search tokens in the text
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
            result = result.copy()
            result['origin_id'] = result.pop('id')
            yield result

    def indexer_configuration_add(self, tools):
        """Add new tools to the storage.

        Args:
            tools ([dict]): List of dictionary representing tool to
              insert in the db. Dictionary with the following keys:

              - **tool_name** (str): tool's name
              - **tool_version** (str): tool's version
              - **tool_configuration** (dict): tool's configuration
                (free form dict)

        Returns:
            list: List of dict inserted in the db (holding the id key as
            well). The order of the list is not guaranteed to match
            the order of the initial list.

        """
        inserted = []
        for tool in tools:
            tool = tool.copy()
            id_ = self._tool_key(tool)
            tool['id'] = id_
            self._tools[id_] = tool
            inserted.append(tool)
        return inserted

    def indexer_configuration_get(self, tool):
        """Retrieve tool information.

        Args:
            tool (dict): Dictionary representing a tool with the
              following keys:

              - **tool_name** (str): tool's name
              - **tool_version** (str): tool's version
              - **tool_configuration** (dict): tool's configuration
                (free form dict)

        Returns:
            The same dictionary with an `id` key, None otherwise.

        """
        return self._tools.get(self._tool_key(tool))

    def _tool_key(self, tool):
        return (tool['tool_name'], tool['tool_version'],
                json.dumps(tool['tool_configuration'], sort_keys=True))
