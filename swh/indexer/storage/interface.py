# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Dict, List, Optional, TypeVar

from swh.core.api import remote_api_endpoint
from swh.core.api.classes import PagedResult as CorePagedResult


TResult = TypeVar("TResult")
PagedResult = CorePagedResult[TResult, str]


Sha1 = bytes


class IndexerStorageInterface:
    @remote_api_endpoint("check_config")
    def check_config(self, *, check_write):
        """Check that the storage is configured and ready to go."""
        ...

    @remote_api_endpoint("content_mimetype/missing")
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
        ...

    @remote_api_endpoint("content_mimetype/range")
    def content_mimetype_get_partition(
        self,
        indexer_configuration_id: int,
        partition_id: int,
        nb_partitions: int,
        page_token: Optional[str] = None,
        limit: int = 1000,
    ) -> PagedResult[Sha1]:
        """Retrieve mimetypes within partition partition_id bound by limit.

        Args:
            **indexer_configuration_id**: The tool used to index data
            **partition_id**: index of the partition to fetch
            **nb_partitions**: total number of partitions to split into
            **page_token**: opaque token used for pagination
            **limit**: Limit result (default to 1000)

        Raises:
            IndexerStorageArgumentException for;
            - limit to None
            - wrong indexer_type provided

        Returns:
            PagedResult of Sha1. If next_page_token is None, there is no more data
            to fetch

        """
        ...

    @remote_api_endpoint("content_mimetype/add")
    def content_mimetype_add(
        self, mimetypes: List[Dict], conflict_update: bool = False
    ) -> Dict[str, int]:
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

        Returns:
            Dict summary of number of rows added

        """
        ...

    @remote_api_endpoint("content_mimetype")
    def content_mimetype_get(self, ids):
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
        ...

    @remote_api_endpoint("content_language/missing")
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
        ...

    @remote_api_endpoint("content_language")
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
        ...

    @remote_api_endpoint("content_language/add")
    def content_language_add(
        self, languages: List[Dict], conflict_update: bool = False
    ) -> Dict[str, int]:
        """Add languages not present in storage.

        Args:
            languages (iterable): dictionaries with keys:

                - **id** (bytes): sha1
                - **lang** (bytes): language detected

            conflict_update (bool): Flag to determine if we want to
                overwrite (true) or skip duplicates (false, the
                default)

        Returns:
            Dict summary of number of rows added

        """
        ...

    @remote_api_endpoint("content/ctags/missing")
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
        ...

    @remote_api_endpoint("content/ctags")
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
        ...

    @remote_api_endpoint("content/ctags/add")
    def content_ctags_add(
        self, ctags: List[Dict], conflict_update: bool = False
    ) -> Dict[str, int]:
        """Add ctags not present in storage

        Args:
            ctags (iterable): dictionaries with keys:

                - **id** (bytes): sha1
                - **ctags** ([list): List of dictionary with keys: name, kind,
                  line, lang

        Returns:
            Dict summary of number of rows added

        """
        ...

    @remote_api_endpoint("content/ctags/search")
    def content_ctags_search(self, expression, limit=10, last_sha1=None):
        """Search through content's raw ctags symbols.

        Args:
            expression (str): Expression to search for
            limit (int): Number of rows to return (default to 10).
            last_sha1 (str): Offset from which retrieving data (default to '').

        Yields:
            rows of ctags including id, name, lang, kind, line, etc...

        """
        ...

    @remote_api_endpoint("content/fossology_license")
    def content_fossology_license_get(self, ids):
        """Retrieve licenses per id.

        Args:
            ids (iterable): sha1 checksums

        Yields:
            dict: ``{id: facts}`` where ``facts`` is a dict with the
            following keys:

                - **licenses** ([str]): associated licenses for that content
                - **tool** (dict): Tool used to compute the license

        """
        ...

    @remote_api_endpoint("content/fossology_license/add")
    def content_fossology_license_add(
        self, licenses: List[Dict], conflict_update: bool = False
    ) -> Dict[str, int]:
        """Add licenses not present in storage.

        Args:
            licenses (iterable): dictionaries with keys:

                - **id**: sha1
                - **licenses** ([bytes]): List of licenses associated to sha1
                - **tool** (str): nomossa

            conflict_update: Flag to determine if we want to overwrite (true)
                or skip duplicates (false, the default)

        Returns:
            Dict summary of number of rows added

        """
        ...

    @remote_api_endpoint("content/fossology_license/range")
    def content_fossology_license_get_partition(
        self,
        indexer_configuration_id: int,
        partition_id: int,
        nb_partitions: int,
        page_token: Optional[str] = None,
        limit: int = 1000,
    ) -> PagedResult[Sha1]:
        """Retrieve licenses within the partition partition_id bound by limit.

        Args:
            **indexer_configuration_id**: The tool used to index data
            **partition_id**: index of the partition to fetch
            **nb_partitions**: total number of partitions to split into
            **page_token**: opaque token used for pagination
            **limit**: Limit result (default to 1000)

        Raises:
            IndexerStorageArgumentException for;
            - limit to None
            - wrong indexer_type provided

        Returns: PagedResult of Sha1. If next_page_token is None, there is no more data
            to fetch

        """
        ...

    @remote_api_endpoint("content_metadata/missing")
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
        ...

    @remote_api_endpoint("content_metadata")
    def content_metadata_get(self, ids):
        """Retrieve metadata per id.

        Args:
            ids (iterable): sha1 checksums

        Yields:
            dictionaries with the following keys:

                id (bytes)
                metadata (str): associated metadata
                tool (dict): tool used to compute metadata

        """
        ...

    @remote_api_endpoint("content_metadata/add")
    def content_metadata_add(
        self, metadata: List[Dict], conflict_update: bool = False
    ) -> Dict[str, int]:
        """Add metadata not present in storage.

        Args:
            metadata (iterable): dictionaries with keys:

                - **id**: sha1
                - **metadata**: arbitrary dict

            conflict_update: Flag to determine if we want to overwrite (true)
                or skip duplicates (false, the default)

        Returns:
            Dict summary of number of rows added

        """
        ...

    @remote_api_endpoint("revision_intrinsic_metadata/missing")
    def revision_intrinsic_metadata_missing(self, metadata):
        """List metadata missing from storage.

        Args:
            metadata (iterable): dictionaries with keys:

               - **id** (bytes): sha1_git revision identifier
               - **indexer_configuration_id** (int): tool used to compute
                 the results

        Yields:
            missing ids

        """
        ...

    @remote_api_endpoint("revision_intrinsic_metadata")
    def revision_intrinsic_metadata_get(self, ids):
        """Retrieve revision metadata per id.

        Args:
            ids (iterable): sha1 checksums

        Yields:
            : dictionaries with the following keys:

                - **id** (bytes)
                - **metadata** (str): associated metadata
                - **tool** (dict): tool used to compute metadata
                - **mappings** (List[str]): list of mappings used to translate
                  these metadata

        """
        ...

    @remote_api_endpoint("revision_intrinsic_metadata/add")
    def revision_intrinsic_metadata_add(
        self, metadata: List[Dict], conflict_update: bool = False
    ) -> Dict[str, int]:
        """Add metadata not present in storage.

        Args:
            metadata (iterable): dictionaries with keys:

                - **id**: sha1_git of revision
                - **metadata**: arbitrary dict
                - **indexer_configuration_id**: tool used to compute metadata
                - **mappings** (List[str]): list of mappings used to translate
                  these metadata

            conflict_update: Flag to determine if we want to overwrite (true)
              or skip duplicates (false, the default)

        Returns:
            Dict summary of number of rows added

        """
        ...

    @remote_api_endpoint("revision_intrinsic_metadata/delete")
    def revision_intrinsic_metadata_delete(self, entries: List[Dict]) -> Dict:
        """Remove revision metadata from the storage.

        Args:
            entries (dict): dictionaries with the following keys:

                - **id** (bytes): revision identifier
                - **indexer_configuration_id** (int): tool used to compute
                  metadata

        Returns:
            Summary of number of rows deleted
        """
        ...

    @remote_api_endpoint("origin_intrinsic_metadata")
    def origin_intrinsic_metadata_get(self, ids):
        """Retrieve origin metadata per id.

        Args:
            ids (iterable): origin identifiers

        Yields:
            list: dictionaries with the following keys:

                - **id** (str): origin url
                - **from_revision** (bytes): which revision this metadata
                  was extracted from
                - **metadata** (str): associated metadata
                - **tool** (dict): tool used to compute metadata
                - **mappings** (List[str]): list of mappings used to translate
                  these metadata

        """
        ...

    @remote_api_endpoint("origin_intrinsic_metadata/add")
    def origin_intrinsic_metadata_add(
        self, metadata: List[Dict], conflict_update: bool = False
    ) -> Dict[str, int]:
        """Add origin metadata not present in storage.

        Args:
            metadata (iterable): dictionaries with keys:

                - **id**: origin urls
                - **from_revision**: sha1 id of the revision used to generate
                  these metadata.
                - **metadata**: arbitrary dict
                - **indexer_configuration_id**: tool used to compute metadata
                - **mappings** (List[str]): list of mappings used to translate
                  these metadata

            conflict_update: Flag to determine if we want to overwrite (true)
              or skip duplicates (false, the default)

        Returns:
            Dict summary of number of rows added

        """
        ...

    @remote_api_endpoint("origin_intrinsic_metadata/delete")
    def origin_intrinsic_metadata_delete(self, entries: List[Dict]) -> Dict:
        """Remove origin metadata from the storage.

        Args:
            entries (dict): dictionaries with the following keys:

                - **id** (str): origin urls
                - **indexer_configuration_id** (int): tool used to compute
                  metadata

        Returns:
            Summary of number of rows deleted
        """
        ...

    @remote_api_endpoint("origin_intrinsic_metadata/search/fulltext")
    def origin_intrinsic_metadata_search_fulltext(self, conjunction, limit=100):
        """Returns the list of origins whose metadata contain all the terms.

        Args:
            conjunction (List[str]): List of terms to be searched for.
            limit (int): The maximum number of results to return

        Yields:
            list: dictionaries with the following keys:

                - **id** (str): origin urls
                - **from_revision**: sha1 id of the revision used to generate
                  these metadata.
                - **metadata** (str): associated metadata
                - **tool** (dict): tool used to compute metadata
                - **mappings** (List[str]): list of mappings used to translate
                  these metadata

        """
        ...

    @remote_api_endpoint("origin_intrinsic_metadata/search/by_producer")
    def origin_intrinsic_metadata_search_by_producer(
        self, page_token="", limit=100, ids_only=False, mappings=None, tool_ids=None
    ):
        """Returns the list of origins whose metadata contain all the terms.

        Args:
            page_token (str): Opaque token used for pagination.
            limit (int): The maximum number of results to return
            ids_only (bool): Determines whether only origin urls are
                returned or the content as well
            mappings (List[str]): Returns origins whose intrinsic metadata
                were generated using at least one of these mappings.

        Returns:
            dict: dict with the following keys:
              - **next_page_token** (str, optional): opaque token to be used as
                `page_token` for retrieving the next page. If absent, there is
                no more pages to gather.
              - **origins** (list): list of origin url (str) if `ids_only=True`
                else dictionaries with the following keys:

                - **id** (str): origin urls
                - **from_revision**: sha1 id of the revision used to generate
                  these metadata.
                - **metadata** (str): associated metadata
                - **tool** (dict): tool used to compute metadata
                - **mappings** (List[str]): list of mappings used to translate
                  these metadata

        """
        ...

    @remote_api_endpoint("origin_intrinsic_metadata/stats")
    def origin_intrinsic_metadata_stats(self):
        """Returns counts of indexed metadata per origins, broken down
        into metadata types.

        Returns:
            dict: dictionary with keys:

                - total (int): total number of origins that were indexed
                  (possibly yielding an empty metadata dictionary)
                - non_empty (int): total number of origins that we extracted
                  a non-empty metadata dictionary from
                - per_mapping (dict): a dictionary with mapping names as
                  keys and number of origins whose indexing used this
                  mapping. Note that indexing a given origin may use
                  0, 1, or many mappings.
        """
        ...

    @remote_api_endpoint("indexer_configuration/add")
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
        ...

    @remote_api_endpoint("indexer_configuration/data")
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
        ...
