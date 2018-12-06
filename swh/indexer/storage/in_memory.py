# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import defaultdict
import json

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


class IndexerStorage:
    """In-memory SWH indexer storage."""

    def __init__(self):
        self._tools = {}
        self._mimetypes = SubStorage(self._tools)
        self._content_ctags = SubStorage(self._tools)
        self._content_metadata = SubStorage(self._tools)
        self._revision_metadata = SubStorage(self._tools)

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
        for item in ctags:
            tool_id = item['indexer_configuration_id']
            if conflict_update:
                item_ctags = []
            else:
                # merge old ctags with new ctags
                existing = list(self._content_ctags.get([item['id']]))
                item_ctags = [
                    {
                        key: ctags_item[key]
                        for key in ('name', 'kind', 'line', 'lang')
                    }
                    for existing_item in existing
                    if existing_item['tool']['id'] == tool_id
                    for ctags_item in existing_item['ctags']
                ]
            for new_item_ctags in item['ctags']:
                if new_item_ctags not in item_ctags:
                    item_ctags.append(new_item_ctags)
            self._content_ctags.add([
                {
                    'id': item['id'],
                    'indexer_configuration_id': tool_id,
                    'ctags': item_ctags,
                }
            ], conflict_update=True)

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
            nb_matches += 1
            for ctags_item in item['ctags']:
                if ctags_item['name'] != expression:
                    continue
                yield {
                    'id': id_,
                    'tool': _transform_tool(self._tools[tool_id]),
                    **ctags_item
                }
            if nb_matches >= limit:
                return

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
        self._revision_metadata.add(metadata, conflict_update)

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
