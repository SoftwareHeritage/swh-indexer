# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import defaultdict
import json


class MetadataStorage:
    """Implements missing/get/add logic for both content_metadata and
    revision_metadata."""
    def __init__(self, tools):
        self._tools = tools
        self._metadata = {}  # map (id_, tool_id) -> metadata_dict
        self._tools_per_id = defaultdict(set)  # map id_ -> Set[tool_id]

    def _transform_tool(self, tool):
        return {
            'id': tool['id'],
            'name': tool['tool_name'],
            'version': tool['tool_version'],
            'configuration': tool['tool_configuration'],
        }

    def missing(self, ids):
        """List metadata missing from storage.

        Args:
            metadata (iterable): dictionaries with keys:

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
        """Retrieve metadata per id.

        Args:
            ids (iterable): sha1 checksums

        Yields:
            dictionaries with the following keys:

                id (bytes)
                translated_metadata (str): associated metadata
                tool (dict): tool used to compute metadata

        """
        for id_ in ids:
            for tool_id in self._tools_per_id.get(id_, set()):
                key = (id_, tool_id)
                yield {
                    'id': id_,
                    'tool': self._transform_tool(self._tools[tool_id]),
                    'translated_metadata': self._metadata[key],
                }

    def add(self, metadata, conflict_update):
        """Add metadata not present in storage.

        Args:
            metadata (iterable): dictionaries with keys:

                - **id**: sha1
                - **translated_metadata**: arbitrary dict
                - **indexer_configuration_id**: tool used to compute the
                                                results

            conflict_update: Flag to determine if we want to overwrite (true)
                or skip duplicates (false)

        """
        for item in metadata:
            tool_id = item['indexer_configuration_id']
            data = item['translated_metadata']
            id_ = item['id']
            if not conflict_update and \
                    tool_id in self._tools_per_id.get(id_, set()):
                # Duplicate, should not be updated
                continue
            key = (id_, tool_id)
            self._metadata[key] = data
            self._tools_per_id[id_].add(tool_id)


class IndexerStorage:
    """In-memory SWH indexer storage."""

    def __init__(self):
        self._tools = {}
        self._content_metadata = MetadataStorage(self._tools)
        self._revision_metadata = MetadataStorage(self._tools)

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

                id (bytes)
                translated_metadata (str): associated metadata
                tool (dict): tool used to compute metadata

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
            List of dict inserted in the db (holding the id key as
            well).  The order of the list is not guaranteed to match
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
