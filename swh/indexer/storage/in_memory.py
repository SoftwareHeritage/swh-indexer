# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json


class IndexerStorage:
    """In-memory SWH indexer storage."""

    def __init__(self):
        self._tools = {}

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
            tool['id'] = self._tool_key(tool)
            self._tools[tool['id']] = tool
            inserted.append(tool)
        return inserted

    def indexer_configuration_get(self, tool, db=None, cur=None):
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
