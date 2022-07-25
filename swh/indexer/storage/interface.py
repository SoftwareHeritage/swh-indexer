# Copyright (C) 2015-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Dict, Iterable, List, Optional, Tuple, TypeVar, Union

from typing_extensions import Protocol, runtime_checkable

from swh.core.api import remote_api_endpoint
from swh.core.api.classes import PagedResult as CorePagedResult
from swh.indexer.storage.model import (
    ContentLicenseRow,
    ContentMetadataRow,
    ContentMimetypeRow,
    DirectoryIntrinsicMetadataRow,
    OriginExtrinsicMetadataRow,
    OriginIntrinsicMetadataRow,
)

TResult = TypeVar("TResult")
PagedResult = CorePagedResult[TResult, str]


Sha1 = bytes


@runtime_checkable
class IndexerStorageInterface(Protocol):
    @remote_api_endpoint("check_config")
    def check_config(self, *, check_write):
        """Check that the storage is configured and ready to go."""
        ...

    @remote_api_endpoint("content_mimetype/missing")
    def content_mimetype_missing(
        self, mimetypes: Iterable[Dict]
    ) -> List[Tuple[Sha1, int]]:
        """Generate mimetypes missing from storage.

        Args:
            mimetypes (iterable): iterable of dict with keys:

              - **id** (bytes): sha1 identifier
              - **indexer_configuration_id** (int): tool used to compute the
                results

        Returns:
            list of tuple (id, indexer_configuration_id) missing

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
        self, mimetypes: List[ContentMimetypeRow]
    ) -> Dict[str, int]:
        """Add mimetypes not present in storage.

        Args:
            mimetypes: mimetype rows to be added, with their `tool` attribute set to
            None.
            overwrite (``True``) or skip duplicates (``False``, the
            default)

        Returns:
            Dict summary of number of rows added

        """
        ...

    @remote_api_endpoint("content_mimetype")
    def content_mimetype_get(self, ids: Iterable[Sha1]) -> List[ContentMimetypeRow]:
        """Retrieve full content mimetype per ids.

        Args:
            ids: sha1 identifiers

        Returns:
            mimetype row objects

        """
        ...

    @remote_api_endpoint("content/fossology_license")
    def content_fossology_license_get(
        self, ids: Iterable[Sha1]
    ) -> List[ContentLicenseRow]:
        """Retrieve licenses per id.

        Args:
            ids: sha1 identifiers

        Yields:
            license rows; possibly more than one per (sha1, tool_id) if there
            are multiple licenses.

        """
        ...

    @remote_api_endpoint("content/fossology_license/add")
    def content_fossology_license_add(
        self, licenses: List[ContentLicenseRow]
    ) -> Dict[str, int]:
        """Add licenses not present in storage.

        Args:
            license: license rows to be added, with their `tool` attribute set to
            None.

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
    def content_metadata_missing(
        self, metadata: Iterable[Dict]
    ) -> List[Tuple[Sha1, int]]:
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
    def content_metadata_get(self, ids: Iterable[Sha1]) -> List[ContentMetadataRow]:
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
        self, metadata: List[ContentMetadataRow]
    ) -> Dict[str, int]:
        """Add metadata not present in storage.

        Args:
            metadata (iterable): dictionaries with keys:

                - **id**: sha1
                - **metadata**: arbitrary dict

        Returns:
            Dict summary of number of rows added

        """
        ...

    @remote_api_endpoint("directory_intrinsic_metadata/missing")
    def directory_intrinsic_metadata_missing(
        self, metadata: Iterable[Dict]
    ) -> List[Tuple[Sha1, int]]:
        """List metadata missing from storage.

        Args:
            metadata (iterable): dictionaries with keys:

               - **id** (bytes): sha1_git directory identifier
               - **indexer_configuration_id** (int): tool used to compute
                 the results

        Returns:
            missing ids

        """
        ...

    @remote_api_endpoint("directory_intrinsic_metadata")
    def directory_intrinsic_metadata_get(
        self, ids: Iterable[Sha1]
    ) -> List[DirectoryIntrinsicMetadataRow]:
        """Retrieve directory metadata per id.

        Args:
            ids (iterable): sha1 checksums

        Returns:
            ContentMetadataRow objects

        """
        ...

    @remote_api_endpoint("directory_intrinsic_metadata/add")
    def directory_intrinsic_metadata_add(
        self,
        metadata: List[DirectoryIntrinsicMetadataRow],
    ) -> Dict[str, int]:
        """Add metadata not present in storage.

        Args:
            metadata: ContentMetadataRow objects

        Returns:
            Dict summary of number of rows added

        """
        ...

    @remote_api_endpoint("origin_intrinsic_metadata")
    def origin_intrinsic_metadata_get(
        self, urls: Iterable[str]
    ) -> List[OriginIntrinsicMetadataRow]:
        """Retrieve origin metadata per id.

        Args:
            urls (iterable): origin URLs

        Returns: list of OriginIntrinsicMetadataRow
        """
        ...

    @remote_api_endpoint("origin_intrinsic_metadata/add")
    def origin_intrinsic_metadata_add(
        self, metadata: List[OriginIntrinsicMetadataRow]
    ) -> Dict[str, int]:
        """Add origin metadata not present in storage.

        Args:
            metadata: list of OriginIntrinsicMetadataRow objects

        Returns:
            Dict summary of number of rows added

        """
        ...

    @remote_api_endpoint("origin_intrinsic_metadata/search/fulltext")
    def origin_intrinsic_metadata_search_fulltext(
        self, conjunction: List[str], limit: int = 100
    ) -> List[OriginIntrinsicMetadataRow]:
        """Returns the list of origins whose metadata contain all the terms.

        Args:
            conjunction: List of terms to be searched for.
            limit: The maximum number of results to return

        Returns:
            list of OriginIntrinsicMetadataRow

        """
        ...

    @remote_api_endpoint("origin_intrinsic_metadata/search/by_producer")
    def origin_intrinsic_metadata_search_by_producer(
        self,
        page_token: str = "",
        limit: int = 100,
        ids_only: bool = False,
        mappings: Optional[List[str]] = None,
        tool_ids: Optional[List[int]] = None,
    ) -> PagedResult[Union[str, OriginIntrinsicMetadataRow]]:
        """Returns the list of origins whose metadata contain all the terms.

        Args:
            page_token (str): Opaque token used for pagination.
            limit (int): The maximum number of results to return
            ids_only (bool): Determines whether only origin urls are
                returned or the content as well
            mappings (List[str]): Returns origins whose intrinsic metadata
                were generated using at least one of these mappings.

        Returns:
            OriginIntrinsicMetadataRow objects

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

    @remote_api_endpoint("origin_extrinsic_metadata")
    def origin_extrinsic_metadata_get(
        self, urls: Iterable[str]
    ) -> List[OriginExtrinsicMetadataRow]:
        """Retrieve origin metadata per id.

        Args:
            urls (iterable): origin URLs

        Returns: list of OriginExtrinsicMetadataRow
        """
        ...

    @remote_api_endpoint("origin_extrinsic_metadata/add")
    def origin_extrinsic_metadata_add(
        self, metadata: List[OriginExtrinsicMetadataRow]
    ) -> Dict[str, int]:
        """Add origin metadata not present in storage.

        Args:
            metadata: list of OriginExtrinsicMetadataRow objects

        Returns:
            Dict summary of number of rows added

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
