# Copyright (C) 2015-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import Counter
import json
from typing import Dict, Iterable, List, Optional, Tuple, Union
import warnings

import attr
import psycopg2
import psycopg2.pool

from swh.core.db.common import db_transaction
from swh.indexer.storage.interface import IndexerStorageInterface
from swh.model.hashutil import hash_to_bytes, hash_to_hex
from swh.model.model import SHA1_SIZE
from swh.storage.exc import StorageDBError
from swh.storage.utils import get_partition_bounds_bytes

from . import converters
from .db import Db
from .exc import DuplicateId, IndexerStorageArgumentException
from .interface import PagedResult, Sha1
from .metrics import process_metrics, send_metric, timed
from .model import (
    ContentLicenseRow,
    ContentMetadataRow,
    ContentMimetypeRow,
    DirectoryIntrinsicMetadataRow,
    OriginExtrinsicMetadataRow,
    OriginIntrinsicMetadataRow,
)
from .writer import JournalWriter

INDEXER_CFG_KEY = "indexer_storage"


MAPPING_NAMES = ["cff", "codemeta", "gemspec", "maven", "npm", "pkg-info"]


def sanitize_json(doc):
    """Recursively replaces NUL characters, as postgresql does not allow
    them in text fields."""
    if isinstance(doc, str):
        return doc.replace("\x00", "")
    elif not hasattr(doc, "__iter__"):
        return doc
    elif isinstance(doc, dict):
        return {sanitize_json(k): sanitize_json(v) for (k, v) in doc.items()}
    elif isinstance(doc, (list, tuple)):
        return [sanitize_json(v) for v in doc]
    else:
        raise TypeError(f"Unexpected object type in sanitize_json: {doc}")


def get_indexer_storage(cls: str, **kwargs) -> IndexerStorageInterface:
    """Instantiate an indexer storage implementation of class `cls` with arguments
    `kwargs`.

    Args:
        cls: indexer storage class (local, remote or memory)
        kwargs: dictionary of arguments passed to the
            indexer storage class constructor

    Returns:
        an instance of swh.indexer.storage

    Raises:
        ValueError if passed an unknown storage class.

    """
    from swh.core.config import get_swh_backend_module

    if "args" in kwargs:
        warnings.warn(
            'Explicit "args" key is deprecated, use keys directly instead.',
            DeprecationWarning,
        )
        kwargs = kwargs["args"]

    _, BackendClass = get_swh_backend_module(INDEXER_CFG_KEY, cls)
    assert BackendClass is not None
    check_config = kwargs.pop("check_config", {})
    idx_storage = BackendClass(**kwargs)
    if check_config:
        if not idx_storage.check_config(**check_config):
            raise EnvironmentError("Indexer storage check config failed")
    return idx_storage


def check_id_duplicates(data):
    """
    If any two row models in `data` have the same unique key, raises
    a `ValueError`.

    Values associated to the key must be hashable.

    Args:
        data (List[dict]): List of dictionaries to be inserted

    >>> tool1 = {"name": "foo", "version": "1.2.3", "configuration": {}}
    >>> tool2 = {"name": "foo", "version": "1.2.4", "configuration": {}}
    >>> check_id_duplicates([
    ...     ContentLicenseRow(id=b'foo', tool=tool1, license="GPL"),
    ...     ContentLicenseRow(id=b'foo', tool=tool2, license="GPL"),
    ... ])
    >>> check_id_duplicates([
    ...     ContentLicenseRow(id=b'foo', tool=tool1, license="AGPL"),
    ...     ContentLicenseRow(id=b'foo', tool=tool1, license="AGPL"),
    ... ])
    Traceback (most recent call last):
    ...
    swh.indexer.storage.exc.DuplicateId: [{'id': b'foo', 'license': 'AGPL', 'tool_configuration': '{}', 'tool_name': 'foo', 'tool_version': '1.2.3'}]

    """  # noqa
    counter = Counter(tuple(sorted(item.unique_key().items())) for item in data)
    duplicates = [id_ for (id_, count) in counter.items() if count >= 2]
    if duplicates:
        raise DuplicateId(list(map(dict, duplicates)))


class IndexerStorage:
    """SWH Indexer Storage Datastore"""

    current_version = 137

    def __init__(self, db, min_pool_conns=1, max_pool_conns=10, journal_writer=None):
        """
        Args:
            db: either a libpq connection string, or a psycopg2 connection
            journal_writer: configuration passed to
                            `swh.journal.writer.get_journal_writer`

        """
        self.journal_writer = JournalWriter(journal_writer)
        try:
            if isinstance(db, psycopg2.extensions.connection):
                self._pool = None
                self._db = Db(db)
            else:
                self._pool = psycopg2.pool.ThreadedConnectionPool(
                    min_pool_conns, max_pool_conns, db
                )
                self._db = None
        except psycopg2.OperationalError as e:
            raise StorageDBError(e)

    def get_db(self):
        if self._db:
            return self._db
        return Db.from_pool(self._pool)

    def put_db(self, db):
        if db is not self._db:
            db.put_conn()

    def _join_indexer_configuration(self, entries, db, cur):
        """Replaces ``entry.indexer_configuration_id`` with a full tool dict
        in ``entry.tool``."""
        joined_entries = []

        # usually, all the additions in a batch are from the same indexer,
        # so this cache allows doing a single query for all the entries.
        tool_cache = {}

        for entry in entries:
            # get the tool used to generate this addition
            tool_id = entry.indexer_configuration_id
            assert tool_id
            if tool_id not in tool_cache:
                tool_cache[tool_id] = dict(
                    self._tool_get_from_id(tool_id, db=db, cur=cur)
                )
                del tool_cache[tool_id]["id"]
            entry = attr.evolve(
                entry, tool=tool_cache[tool_id], indexer_configuration_id=None
            )

            joined_entries.append(entry)

        return joined_entries

    @timed
    @db_transaction()
    def check_config(self, *, check_write, db=None, cur=None):
        # Check permissions on one of the tables
        if check_write:
            check = "INSERT"
        else:
            check = "SELECT"

        cur.execute(
            "select has_table_privilege(current_user, 'content_mimetype', %s)",  # noqa
            (check,),
        )
        return cur.fetchone()[0]

    @timed
    @db_transaction()
    def content_mimetype_missing(
        self, mimetypes: Iterable[Dict], db=None, cur=None
    ) -> List[Tuple[Sha1, int]]:
        return [obj[0] for obj in db.content_mimetype_missing_from_list(mimetypes, cur)]

    @timed
    @db_transaction()
    def get_partition(
        self,
        indexer_type: str,
        indexer_configuration_id: int,
        partition_id: int,
        nb_partitions: int,
        page_token: Optional[str] = None,
        limit: int = 1000,
        with_textual_data=False,
        db=None,
        cur=None,
    ) -> PagedResult[Sha1]:
        """Retrieve ids of content with `indexer_type` within within partition partition_id
        bound by limit.

        Args:
            **indexer_type**: Type of data content to index (mimetype, etc...)
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
        if indexer_type not in db.content_indexer_names:
            err = f"Wrong type. Should be one of [{','.join(db.content_indexer_names)}]"
            raise IndexerStorageArgumentException(err)

        start, end = get_partition_bounds_bytes(partition_id, nb_partitions, SHA1_SIZE)
        if page_token is not None:
            start = hash_to_bytes(page_token)
        if end is None:
            end = b"\xff" * SHA1_SIZE

        next_page_token: Optional[str] = None
        ids = [
            row[0]
            for row in db.content_get_range(
                indexer_type,
                start,
                end,
                indexer_configuration_id,
                limit=limit + 1,
                with_textual_data=with_textual_data,
                cur=cur,
            )
        ]

        if len(ids) >= limit:
            next_page_token = hash_to_hex(ids[-1])
            ids = ids[:limit]

        assert len(ids) <= limit
        return PagedResult(results=ids, next_page_token=next_page_token)

    @timed
    @db_transaction()
    def content_mimetype_get_partition(
        self,
        indexer_configuration_id: int,
        partition_id: int,
        nb_partitions: int,
        page_token: Optional[str] = None,
        limit: int = 1000,
        db=None,
        cur=None,
    ) -> PagedResult[Sha1]:
        return self.get_partition(
            "mimetype",
            indexer_configuration_id,
            partition_id,
            nb_partitions,
            page_token=page_token,
            limit=limit,
            db=db,
            cur=cur,
        )

    @timed
    @process_metrics
    @db_transaction()
    def content_mimetype_add(
        self,
        mimetypes: List[ContentMimetypeRow],
        db=None,
        cur=None,
    ) -> Dict[str, int]:
        mimetypes_with_tools = self._join_indexer_configuration(
            mimetypes, db=db, cur=cur
        )
        check_id_duplicates(mimetypes_with_tools)
        self.journal_writer.write_additions("content_mimetype", mimetypes_with_tools)
        db.mktemp_content_mimetype(cur)
        db.copy_to(
            [m.to_dict() for m in mimetypes],
            "tmp_content_mimetype",
            ["id", "mimetype", "encoding", "indexer_configuration_id"],
            cur,
        )
        count = db.content_mimetype_add_from_temp(cur)
        return {"content_mimetype:add": count}

    @timed
    @db_transaction()
    def content_mimetype_get(
        self, ids: Iterable[Sha1], db=None, cur=None
    ) -> List[ContentMimetypeRow]:
        return [
            ContentMimetypeRow.from_dict(
                converters.db_to_mimetype(dict(zip(db.content_mimetype_cols, c)))
            )
            for c in db.content_mimetype_get_from_list(ids, cur)
        ]

    @timed
    @db_transaction()
    def content_fossology_license_get(
        self, ids: Iterable[Sha1], db=None, cur=None
    ) -> List[ContentLicenseRow]:
        return [
            ContentLicenseRow.from_dict(
                converters.db_to_fossology_license(
                    dict(zip(db.content_fossology_license_cols, c))
                )
            )
            for c in db.content_fossology_license_get_from_list(ids, cur)
        ]

    @timed
    @process_metrics
    @db_transaction()
    def content_fossology_license_add(
        self,
        licenses: List[ContentLicenseRow],
        db=None,
        cur=None,
    ) -> Dict[str, int]:
        licenses_with_tools = self._join_indexer_configuration(licenses, db=db, cur=cur)
        check_id_duplicates(licenses_with_tools)
        self.journal_writer.write_additions(
            "content_fossology_license", licenses_with_tools
        )
        db.mktemp_content_fossology_license(cur)
        db.copy_to(
            [license.to_dict() for license in licenses],
            tblname="tmp_content_fossology_license",
            columns=["id", "license", "indexer_configuration_id"],
            cur=cur,
        )
        count = db.content_fossology_license_add_from_temp(cur)
        return {"content_fossology_license:add": count}

    @timed
    @db_transaction()
    def content_fossology_license_get_partition(
        self,
        indexer_configuration_id: int,
        partition_id: int,
        nb_partitions: int,
        page_token: Optional[str] = None,
        limit: int = 1000,
        db=None,
        cur=None,
    ) -> PagedResult[Sha1]:
        return self.get_partition(
            "fossology_license",
            indexer_configuration_id,
            partition_id,
            nb_partitions,
            page_token=page_token,
            limit=limit,
            with_textual_data=True,
            db=db,
            cur=cur,
        )

    @timed
    @db_transaction()
    def content_metadata_missing(
        self, metadata: Iterable[Dict], db=None, cur=None
    ) -> List[Tuple[Sha1, int]]:
        return [obj[0] for obj in db.content_metadata_missing_from_list(metadata, cur)]

    @timed
    @db_transaction()
    def content_metadata_get(
        self, ids: Iterable[Sha1], db=None, cur=None
    ) -> List[ContentMetadataRow]:
        return [
            ContentMetadataRow.from_dict(
                converters.db_to_metadata(dict(zip(db.content_metadata_cols, c)))
            )
            for c in db.content_metadata_get_from_list(ids, cur)
        ]

    @timed
    @process_metrics
    @db_transaction()
    def content_metadata_add(
        self,
        metadata: List[ContentMetadataRow],
        db=None,
        cur=None,
    ) -> Dict[str, int]:
        metadata_with_tools = self._join_indexer_configuration(metadata, db=db, cur=cur)
        check_id_duplicates(metadata_with_tools)
        self.journal_writer.write_additions("content_metadata", metadata_with_tools)

        db.mktemp_content_metadata(cur)

        rows = [m.to_dict() for m in metadata]
        for row in rows:
            row["metadata"] = sanitize_json(row["metadata"])

        db.copy_to(
            rows,
            "tmp_content_metadata",
            ["id", "metadata", "indexer_configuration_id"],
            cur,
        )
        count = db.content_metadata_add_from_temp(cur)
        return {
            "content_metadata:add": count,
        }

    @timed
    @db_transaction()
    def directory_intrinsic_metadata_missing(
        self, metadata: Iterable[Dict], db=None, cur=None
    ) -> List[Tuple[Sha1, int]]:
        return [
            obj[0]
            for obj in db.directory_intrinsic_metadata_missing_from_list(metadata, cur)
        ]

    @timed
    @db_transaction()
    def directory_intrinsic_metadata_get(
        self, ids: Iterable[Sha1], db=None, cur=None
    ) -> List[DirectoryIntrinsicMetadataRow]:
        return [
            DirectoryIntrinsicMetadataRow.from_dict(
                converters.db_to_metadata(
                    dict(zip(db.directory_intrinsic_metadata_cols, c))
                )
            )
            for c in db.directory_intrinsic_metadata_get_from_list(ids, cur)
        ]

    @timed
    @process_metrics
    @db_transaction()
    def directory_intrinsic_metadata_add(
        self,
        metadata: List[DirectoryIntrinsicMetadataRow],
        db=None,
        cur=None,
    ) -> Dict[str, int]:
        metadata_with_tools = self._join_indexer_configuration(metadata, db=db, cur=cur)
        check_id_duplicates(metadata_with_tools)
        self.journal_writer.write_additions(
            "directory_intrinsic_metadata", metadata_with_tools
        )

        db.mktemp_directory_intrinsic_metadata(cur)

        rows = [m.to_dict() for m in metadata]
        for row in rows:
            row["metadata"] = sanitize_json(row["metadata"])

        db.copy_to(
            rows,
            "tmp_directory_intrinsic_metadata",
            ["id", "metadata", "mappings", "indexer_configuration_id"],
            cur,
        )
        count = db.directory_intrinsic_metadata_add_from_temp(cur)
        return {
            "directory_intrinsic_metadata:add": count,
        }

    @timed
    @db_transaction()
    def origin_intrinsic_metadata_get(
        self, urls: Iterable[str], db=None, cur=None
    ) -> List[OriginIntrinsicMetadataRow]:
        return [
            OriginIntrinsicMetadataRow.from_dict(
                converters.db_to_metadata(
                    dict(zip(db.origin_intrinsic_metadata_cols, c))
                )
            )
            for c in db.origin_intrinsic_metadata_get_from_list(urls, cur)
        ]

    @timed
    @process_metrics
    @db_transaction()
    def origin_intrinsic_metadata_add(
        self,
        metadata: List[OriginIntrinsicMetadataRow],
        db=None,
        cur=None,
    ) -> Dict[str, int]:
        metadata_with_tools = self._join_indexer_configuration(metadata, db=db, cur=cur)
        check_id_duplicates(metadata_with_tools)
        self.journal_writer.write_additions(
            "origin_intrinsic_metadata", metadata_with_tools
        )

        db.mktemp_origin_intrinsic_metadata(cur)

        rows = [m.to_dict() for m in metadata]
        for row in rows:
            row["metadata"] = sanitize_json(row["metadata"])

        db.copy_to(
            rows,
            "tmp_origin_intrinsic_metadata",
            [
                "id",
                "metadata",
                "indexer_configuration_id",
                "from_directory",
                "mappings",
            ],
            cur,
        )
        count = db.origin_intrinsic_metadata_add_from_temp(cur)
        return {
            "origin_intrinsic_metadata:add": count,
        }

    @timed
    @db_transaction()
    def origin_intrinsic_metadata_search_fulltext(
        self, conjunction: List[str], limit: int = 100, db=None, cur=None
    ) -> List[OriginIntrinsicMetadataRow]:
        return [
            OriginIntrinsicMetadataRow.from_dict(
                converters.db_to_metadata(
                    dict(zip(db.origin_intrinsic_metadata_cols, c))
                )
            )
            for c in db.origin_intrinsic_metadata_search_fulltext(
                conjunction, limit=limit, cur=cur
            )
        ]

    @timed
    @db_transaction()
    def origin_intrinsic_metadata_search_by_producer(
        self,
        page_token: str = "",
        limit: int = 100,
        ids_only: bool = False,
        mappings: Optional[List[str]] = None,
        tool_ids: Optional[List[int]] = None,
        db=None,
        cur=None,
    ) -> PagedResult[Union[str, OriginIntrinsicMetadataRow]]:
        assert isinstance(page_token, str)
        # we go to limit+1 to check whether we should add next_page_token in
        # the response
        rows = db.origin_intrinsic_metadata_search_by_producer(
            page_token, limit + 1, ids_only, mappings, tool_ids, cur
        )
        next_page_token = None
        if ids_only:
            results = [origin for (origin,) in rows]
            if len(results) > limit:
                results[limit:] = []
                next_page_token = results[-1]
        else:
            results = [
                OriginIntrinsicMetadataRow.from_dict(
                    converters.db_to_metadata(
                        dict(zip(db.origin_intrinsic_metadata_cols, row))
                    )
                )
                for row in rows
            ]
            if len(results) > limit:
                results[limit:] = []
                next_page_token = results[-1].id

        return PagedResult(
            results=results,
            next_page_token=next_page_token,
        )

    @timed
    @db_transaction()
    def origin_intrinsic_metadata_stats(self, db=None, cur=None):
        mapping_names = [m for m in MAPPING_NAMES]
        select_parts = []

        # Count rows for each mapping
        for mapping_name in mapping_names:
            select_parts.append(
                (
                    "sum(case when (mappings @> ARRAY['%s']) "
                    "         then 1 else 0 end)"
                )
                % mapping_name
            )

        # Total
        select_parts.append("sum(1)")

        # Rows whose metadata has at least one key that is not '@context'
        select_parts.append(
            "sum(case when ('{}'::jsonb @> (metadata - '@context')) "
            "         then 0 else 1 end)"
        )
        cur.execute(
            "select " + ", ".join(select_parts) + " from origin_intrinsic_metadata"
        )
        results = dict(zip(mapping_names + ["total", "non_empty"], cur.fetchone()))
        return {
            "total": results.pop("total"),
            "non_empty": results.pop("non_empty"),
            "per_mapping": results,
        }

    @timed
    @db_transaction()
    def origin_extrinsic_metadata_get(
        self, urls: Iterable[str], db=None, cur=None
    ) -> List[OriginExtrinsicMetadataRow]:
        return [
            OriginExtrinsicMetadataRow.from_dict(
                converters.db_to_metadata(
                    dict(zip(db.origin_extrinsic_metadata_cols, c))
                )
            )
            for c in db.origin_extrinsic_metadata_get_from_list(urls, cur)
        ]

    @timed
    @process_metrics
    @db_transaction()
    def origin_extrinsic_metadata_add(
        self,
        metadata: List[OriginExtrinsicMetadataRow],
        db=None,
        cur=None,
    ) -> Dict[str, int]:
        metadata_with_tools = self._join_indexer_configuration(metadata, db=db, cur=cur)
        check_id_duplicates(metadata_with_tools)
        self.journal_writer.write_additions(
            "origin_extrinsic_metadata", metadata_with_tools
        )

        db.mktemp_origin_extrinsic_metadata(cur)

        rows = [m.to_dict() for m in metadata]
        for row in rows:
            row["metadata"] = sanitize_json(row["metadata"])

        db.copy_to(
            rows,
            "tmp_origin_extrinsic_metadata",
            [
                "id",
                "metadata",
                "indexer_configuration_id",
                "from_remd_id",
                "mappings",
            ],
            cur,
        )
        count = db.origin_extrinsic_metadata_add_from_temp(cur)
        return {
            "origin_extrinsic_metadata:add": count,
        }

    @timed
    @db_transaction()
    def indexer_configuration_add(self, tools, db=None, cur=None):
        db.mktemp_indexer_configuration(cur)
        db.copy_to(
            tools,
            "tmp_indexer_configuration",
            ["tool_name", "tool_version", "tool_configuration"],
            cur,
        )

        tools = db.indexer_configuration_add_from_temp(cur)
        results = [dict(zip(db.indexer_configuration_cols, line)) for line in tools]
        send_metric(
            "indexer_configuration:add",
            len(results),
            method_name="indexer_configuration_add",
        )
        return results

    @timed
    @db_transaction()
    def indexer_configuration_get(self, tool, db=None, cur=None):
        tool_conf = tool["tool_configuration"]
        if isinstance(tool_conf, dict):
            tool_conf = json.dumps(tool_conf)
        idx = db.indexer_configuration_get(
            tool["tool_name"], tool["tool_version"], tool_conf
        )
        if not idx:
            return None
        return dict(zip(db.indexer_configuration_cols, idx))

    @db_transaction()
    def _tool_get_from_id(self, id_, db, cur):
        tool = dict(
            zip(
                db.indexer_configuration_cols,
                db.indexer_configuration_get_from_id(id_, cur),
            )
        )
        return {
            "id": tool["id"],
            "name": tool["tool_name"],
            "version": tool["tool_version"],
            "configuration": tool["tool_configuration"],
        }
