# Copyright (C) 2015-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import Counter
from importlib import import_module
import json
from typing import Dict, Iterable, List, Optional, Tuple, Union
import warnings

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

INDEXER_CFG_KEY = "indexer_storage"


MAPPING_NAMES = ["cff", "codemeta", "gemspec", "maven", "npm", "pkg-info"]


SERVER_IMPLEMENTATIONS: Dict[str, str] = {
    "postgresql": ".IndexerStorage",
    "remote": ".api.client.RemoteStorage",
    "memory": ".in_memory.IndexerStorage",
    # deprecated
    "local": ".IndexerStorage",
}


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
    if "args" in kwargs:
        warnings.warn(
            'Explicit "args" key is deprecated, use keys directly instead.',
            DeprecationWarning,
        )
        kwargs = kwargs["args"]

    class_path = SERVER_IMPLEMENTATIONS.get(cls)
    if class_path is None:
        raise ValueError(
            f"Unknown indexer storage class `{cls}`. "
            f"Supported: {', '.join(SERVER_IMPLEMENTATIONS)}"
        )

    (module_path, class_name) = class_path.rsplit(".", 1)
    module = import_module(module_path if module_path else ".", package=__package__)
    BackendClass = getattr(module, class_name)
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

    >>> check_id_duplicates([
    ...     ContentLanguageRow(id=b'foo', indexer_configuration_id=42, lang="python"),
    ...     ContentLanguageRow(id=b'foo', indexer_configuration_id=32, lang="python"),
    ... ])
    >>> check_id_duplicates([
    ...     ContentLanguageRow(id=b'foo', indexer_configuration_id=42, lang="python"),
    ...     ContentLanguageRow(id=b'foo', indexer_configuration_id=42, lang="python"),
    ... ])
    Traceback (most recent call last):
      ...
    swh.indexer.storage.exc.DuplicateId: [{'id': b'foo', 'indexer_configuration_id': 42}]
    """  # noqa
    counter = Counter(tuple(sorted(item.unique_key().items())) for item in data)
    duplicates = [id_ for (id_, count) in counter.items() if count >= 2]
    if duplicates:
        raise DuplicateId(list(map(dict, duplicates)))


class IndexerStorage:
    """SWH Indexer Storage Datastore"""

    current_version = 135

    def __init__(self, db, min_pool_conns=1, max_pool_conns=10, journal_writer=None):
        """
        Args:
            db: either a libpq connection string, or a psycopg2 connection
            journal_writer: configuration passed to
                            `swh.journal.writer.get_journal_writer`

        """
        self.journal_writer = JournalWriter(self._tool_get_from_id, journal_writer)
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
        check_id_duplicates(mimetypes)
        mimetypes.sort(key=lambda m: m.id)
        self.journal_writer.write_additions("content_mimetype", mimetypes)
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
    def content_language_missing(
        self, languages: Iterable[Dict], db=None, cur=None
    ) -> List[Tuple[Sha1, int]]:
        return [obj[0] for obj in db.content_language_missing_from_list(languages, cur)]

    @timed
    @db_transaction()
    def content_language_get(
        self, ids: Iterable[Sha1], db=None, cur=None
    ) -> List[ContentLanguageRow]:
        return [
            ContentLanguageRow.from_dict(
                converters.db_to_language(dict(zip(db.content_language_cols, c)))
            )
            for c in db.content_language_get_from_list(ids, cur)
        ]

    @timed
    @process_metrics
    @db_transaction()
    def content_language_add(
        self,
        languages: List[ContentLanguageRow],
        db=None,
        cur=None,
    ) -> Dict[str, int]:
        check_id_duplicates(languages)
        languages.sort(key=lambda m: m.id)
        self.journal_writer.write_additions("content_language", languages)
        db.mktemp_content_language(cur)
        # empty language is mapped to 'unknown'
        db.copy_to(
            (
                {
                    "id": lang.id,
                    "lang": lang.lang or "unknown",
                    "indexer_configuration_id": lang.indexer_configuration_id,
                }
                for lang in languages
            ),
            "tmp_content_language",
            ["id", "lang", "indexer_configuration_id"],
            cur,
        )

        count = db.content_language_add_from_temp(cur)
        return {"content_language:add": count}

    @timed
    @db_transaction()
    def content_ctags_missing(
        self, ctags: Iterable[Dict], db=None, cur=None
    ) -> List[Tuple[Sha1, int]]:
        return [obj[0] for obj in db.content_ctags_missing_from_list(ctags, cur)]

    @timed
    @db_transaction()
    def content_ctags_get(
        self, ids: Iterable[Sha1], db=None, cur=None
    ) -> List[ContentCtagsRow]:
        return [
            ContentCtagsRow.from_dict(
                converters.db_to_ctags(dict(zip(db.content_ctags_cols, c)))
            )
            for c in db.content_ctags_get_from_list(ids, cur)
        ]

    @timed
    @process_metrics
    @db_transaction()
    def content_ctags_add(
        self,
        ctags: List[ContentCtagsRow],
        db=None,
        cur=None,
    ) -> Dict[str, int]:
        check_id_duplicates(ctags)
        ctags.sort(key=lambda m: m.id)
        self.journal_writer.write_additions("content_ctags", ctags)

        db.mktemp_content_ctags(cur)
        db.copy_to(
            [ctag.to_dict() for ctag in ctags],
            tblname="tmp_content_ctags",
            columns=["id", "name", "kind", "line", "lang", "indexer_configuration_id"],
            cur=cur,
        )

        count = db.content_ctags_add_from_temp(cur)
        return {"content_ctags:add": count}

    @timed
    @db_transaction()
    def content_ctags_search(
        self,
        expression: str,
        limit: int = 10,
        last_sha1: Optional[Sha1] = None,
        db=None,
        cur=None,
    ) -> List[ContentCtagsRow]:
        return [
            ContentCtagsRow.from_dict(
                converters.db_to_ctags(dict(zip(db.content_ctags_cols, obj)))
            )
            for obj in db.content_ctags_search(expression, last_sha1, limit, cur=cur)
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
        check_id_duplicates(licenses)
        licenses.sort(key=lambda m: m.id)
        self.journal_writer.write_additions("content_fossology_license", licenses)
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
        check_id_duplicates(metadata)
        metadata.sort(key=lambda m: m.id)
        self.journal_writer.write_additions("content_metadata", metadata)

        db.mktemp_content_metadata(cur)

        db.copy_to(
            [m.to_dict() for m in metadata],
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
        check_id_duplicates(metadata)
        metadata.sort(key=lambda m: m.id)
        self.journal_writer.write_additions("directory_intrinsic_metadata", metadata)

        db.mktemp_directory_intrinsic_metadata(cur)

        db.copy_to(
            [m.to_dict() for m in metadata],
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
        check_id_duplicates(metadata)
        metadata.sort(key=lambda m: m.id)
        self.journal_writer.write_additions("origin_intrinsic_metadata", metadata)

        db.mktemp_origin_intrinsic_metadata(cur)

        db.copy_to(
            [m.to_dict() for m in metadata],
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
        check_id_duplicates(metadata)
        metadata.sort(key=lambda m: m.id)
        self.journal_writer.write_additions("origin_extrinsic_metadata", metadata)

        db.mktemp_origin_extrinsic_metadata(cur)

        db.copy_to(
            [m.to_dict() for m in metadata],
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
