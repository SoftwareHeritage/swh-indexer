# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import json
import psycopg2
import psycopg2.pool

from collections import defaultdict, Counter
from typing import Dict, List, Optional

from swh.core.db.common import db_transaction_generator, db_transaction
from swh.model.hashutil import hash_to_bytes, hash_to_hex
from swh.model.model import SHA1_SIZE
from swh.storage.exc import StorageDBError
from swh.storage.utils import get_partition_bounds_bytes


from .interface import PagedResult, Sha1
from . import converters
from .db import Db
from .exc import IndexerStorageArgumentException, DuplicateId
from .metrics import process_metrics, send_metric, timed


INDEXER_CFG_KEY = "indexer_storage"


MAPPING_NAMES = ["codemeta", "gemspec", "maven", "npm", "pkg-info"]


def get_indexer_storage(cls, args):
    """Get an indexer storage object of class `storage_class` with
    arguments `storage_args`.

    Args:
        cls (str): storage's class, either 'local' or 'remote'
        args (dict): dictionary of arguments passed to the
            storage class constructor

    Returns:
        an instance of swh.indexer's storage (either local or remote)

    Raises:
        ValueError if passed an unknown storage class.

    """
    if cls == "remote":
        from .api.client import RemoteStorage as IndexerStorage
    elif cls == "local":
        from . import IndexerStorage
    elif cls == "memory":
        from .in_memory import IndexerStorage
    else:
        raise ValueError("Unknown indexer storage class `%s`" % cls)

    return IndexerStorage(**args)


def check_id_duplicates(data):
    """
    If any two dictionaries in `data` have the same id, raises
    a `ValueError`.

    Values associated to the key must be hashable.

    Args:
        data (List[dict]): List of dictionaries to be inserted

    >>> check_id_duplicates([
    ...     {'id': 'foo', 'data': 'spam'},
    ...     {'id': 'bar', 'data': 'egg'},
    ... ])
    >>> check_id_duplicates([
    ...     {'id': 'foo', 'data': 'spam'},
    ...     {'id': 'foo', 'data': 'egg'},
    ... ])
    Traceback (most recent call last):
      ...
    swh.indexer.storage.exc.DuplicateId: ['foo']
    """
    counter = Counter(item["id"] for item in data)
    duplicates = [id_ for (id_, count) in counter.items() if count >= 2]
    if duplicates:
        raise DuplicateId(duplicates)


class IndexerStorage:
    """SWH Indexer Storage

    """

    def __init__(self, db, min_pool_conns=1, max_pool_conns=10):
        """
        Args:
            db_conn: either a libpq connection string, or a psycopg2 connection

        """
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
    @db_transaction_generator()
    def content_mimetype_missing(self, mimetypes, db=None, cur=None):
        for obj in db.content_mimetype_missing_from_list(mimetypes, cur):
            yield obj[0]

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
        self, mimetypes: List[Dict], conflict_update: bool = False, db=None, cur=None
    ) -> Dict[str, int]:
        """Add mimetypes to the storage (if conflict_update is True, this will
           override existing data if any).

        Returns:
            A dict with the number of new elements added to the storage.

        """
        check_id_duplicates(mimetypes)
        mimetypes.sort(key=lambda m: m["id"])
        db.mktemp_content_mimetype(cur)
        db.copy_to(
            mimetypes,
            "tmp_content_mimetype",
            ["id", "mimetype", "encoding", "indexer_configuration_id"],
            cur,
        )
        count = db.content_mimetype_add_from_temp(conflict_update, cur)
        return {"content_mimetype:add": count}

    @timed
    @db_transaction_generator()
    def content_mimetype_get(self, ids, db=None, cur=None):
        for c in db.content_mimetype_get_from_list(ids, cur):
            yield converters.db_to_mimetype(dict(zip(db.content_mimetype_cols, c)))

    @timed
    @db_transaction_generator()
    def content_language_missing(self, languages, db=None, cur=None):
        for obj in db.content_language_missing_from_list(languages, cur):
            yield obj[0]

    @timed
    @db_transaction_generator()
    def content_language_get(self, ids, db=None, cur=None):
        for c in db.content_language_get_from_list(ids, cur):
            yield converters.db_to_language(dict(zip(db.content_language_cols, c)))

    @timed
    @process_metrics
    @db_transaction()
    def content_language_add(
        self, languages: List[Dict], conflict_update: bool = False, db=None, cur=None
    ) -> Dict[str, int]:
        check_id_duplicates(languages)
        languages.sort(key=lambda m: m["id"])
        db.mktemp_content_language(cur)
        # empty language is mapped to 'unknown'
        db.copy_to(
            (
                {
                    "id": lang["id"],
                    "lang": "unknown" if not lang["lang"] else lang["lang"],
                    "indexer_configuration_id": lang["indexer_configuration_id"],
                }
                for lang in languages
            ),
            "tmp_content_language",
            ["id", "lang", "indexer_configuration_id"],
            cur,
        )

        count = db.content_language_add_from_temp(conflict_update, cur)
        return {"content_language:add": count}

    @timed
    @db_transaction_generator()
    def content_ctags_missing(self, ctags, db=None, cur=None):
        for obj in db.content_ctags_missing_from_list(ctags, cur):
            yield obj[0]

    @timed
    @db_transaction_generator()
    def content_ctags_get(self, ids, db=None, cur=None):
        for c in db.content_ctags_get_from_list(ids, cur):
            yield converters.db_to_ctags(dict(zip(db.content_ctags_cols, c)))

    @timed
    @process_metrics
    @db_transaction()
    def content_ctags_add(
        self, ctags: List[Dict], conflict_update: bool = False, db=None, cur=None
    ) -> Dict[str, int]:
        check_id_duplicates(ctags)
        ctags.sort(key=lambda m: m["id"])

        def _convert_ctags(__ctags):
            """Convert ctags dict to list of ctags.

            """
            for ctags in __ctags:
                yield from converters.ctags_to_db(ctags)

        db.mktemp_content_ctags(cur)
        db.copy_to(
            list(_convert_ctags(ctags)),
            tblname="tmp_content_ctags",
            columns=["id", "name", "kind", "line", "lang", "indexer_configuration_id"],
            cur=cur,
        )

        count = db.content_ctags_add_from_temp(conflict_update, cur)
        return {"content_ctags:add": count}

    @timed
    @db_transaction_generator()
    def content_ctags_search(
        self, expression, limit=10, last_sha1=None, db=None, cur=None
    ):
        for obj in db.content_ctags_search(expression, last_sha1, limit, cur=cur):
            yield converters.db_to_ctags(dict(zip(db.content_ctags_cols, obj)))

    @timed
    @db_transaction_generator()
    def content_fossology_license_get(self, ids, db=None, cur=None):
        d = defaultdict(list)
        for c in db.content_fossology_license_get_from_list(ids, cur):
            license = dict(zip(db.content_fossology_license_cols, c))

            id_ = license["id"]
            d[id_].append(converters.db_to_fossology_license(license))

        for id_, facts in d.items():
            yield {id_: facts}

    @timed
    @process_metrics
    @db_transaction()
    def content_fossology_license_add(
        self, licenses: List[Dict], conflict_update: bool = False, db=None, cur=None
    ) -> Dict[str, int]:
        check_id_duplicates(licenses)
        licenses.sort(key=lambda m: m["id"])
        db.mktemp_content_fossology_license(cur)
        db.copy_to(
            (
                {
                    "id": sha1["id"],
                    "indexer_configuration_id": sha1["indexer_configuration_id"],
                    "license": license,
                }
                for sha1 in licenses
                for license in sha1["licenses"]
            ),
            tblname="tmp_content_fossology_license",
            columns=["id", "license", "indexer_configuration_id"],
            cur=cur,
        )
        count = db.content_fossology_license_add_from_temp(conflict_update, cur)
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
    @db_transaction_generator()
    def content_metadata_missing(self, metadata, db=None, cur=None):
        for obj in db.content_metadata_missing_from_list(metadata, cur):
            yield obj[0]

    @timed
    @db_transaction_generator()
    def content_metadata_get(self, ids, db=None, cur=None):
        for c in db.content_metadata_get_from_list(ids, cur):
            yield converters.db_to_metadata(dict(zip(db.content_metadata_cols, c)))

    @timed
    @process_metrics
    @db_transaction()
    def content_metadata_add(
        self, metadata: List[Dict], conflict_update: bool = False, db=None, cur=None
    ) -> Dict[str, int]:
        check_id_duplicates(metadata)
        metadata.sort(key=lambda m: m["id"])

        db.mktemp_content_metadata(cur)

        db.copy_to(
            metadata,
            "tmp_content_metadata",
            ["id", "metadata", "indexer_configuration_id"],
            cur,
        )
        count = db.content_metadata_add_from_temp(conflict_update, cur)
        return {
            "content_metadata:add": count,
        }

    @timed
    @db_transaction_generator()
    def revision_intrinsic_metadata_missing(self, metadata, db=None, cur=None):
        for obj in db.revision_intrinsic_metadata_missing_from_list(metadata, cur):
            yield obj[0]

    @timed
    @db_transaction_generator()
    def revision_intrinsic_metadata_get(self, ids, db=None, cur=None):
        for c in db.revision_intrinsic_metadata_get_from_list(ids, cur):
            yield converters.db_to_metadata(
                dict(zip(db.revision_intrinsic_metadata_cols, c))
            )

    @timed
    @process_metrics
    @db_transaction()
    def revision_intrinsic_metadata_add(
        self, metadata: List[Dict], conflict_update: bool = False, db=None, cur=None
    ) -> Dict[str, int]:
        check_id_duplicates(metadata)
        metadata.sort(key=lambda m: m["id"])

        db.mktemp_revision_intrinsic_metadata(cur)

        db.copy_to(
            metadata,
            "tmp_revision_intrinsic_metadata",
            ["id", "metadata", "mappings", "indexer_configuration_id"],
            cur,
        )
        count = db.revision_intrinsic_metadata_add_from_temp(conflict_update, cur)
        return {
            "revision_intrinsic_metadata:add": count,
        }

    @timed
    @process_metrics
    @db_transaction()
    def revision_intrinsic_metadata_delete(
        self, entries: List[Dict], db=None, cur=None
    ) -> Dict:
        count = db.revision_intrinsic_metadata_delete(entries, cur)
        return {"revision_intrinsic_metadata:del": count}

    @timed
    @db_transaction_generator()
    def origin_intrinsic_metadata_get(self, ids, db=None, cur=None):
        for c in db.origin_intrinsic_metadata_get_from_list(ids, cur):
            yield converters.db_to_metadata(
                dict(zip(db.origin_intrinsic_metadata_cols, c))
            )

    @timed
    @process_metrics
    @db_transaction()
    def origin_intrinsic_metadata_add(
        self, metadata: List[Dict], conflict_update: bool = False, db=None, cur=None
    ) -> Dict[str, int]:
        check_id_duplicates(metadata)
        metadata.sort(key=lambda m: m["id"])

        db.mktemp_origin_intrinsic_metadata(cur)

        db.copy_to(
            metadata,
            "tmp_origin_intrinsic_metadata",
            ["id", "metadata", "indexer_configuration_id", "from_revision", "mappings"],
            cur,
        )
        count = db.origin_intrinsic_metadata_add_from_temp(conflict_update, cur)
        return {
            "origin_intrinsic_metadata:add": count,
        }

    @timed
    @process_metrics
    @db_transaction()
    def origin_intrinsic_metadata_delete(
        self, entries: List[Dict], db=None, cur=None
    ) -> Dict:
        count = db.origin_intrinsic_metadata_delete(entries, cur)
        return {
            "origin_intrinsic_metadata:del": count,
        }

    @timed
    @db_transaction_generator()
    def origin_intrinsic_metadata_search_fulltext(
        self, conjunction, limit=100, db=None, cur=None
    ):
        for c in db.origin_intrinsic_metadata_search_fulltext(
            conjunction, limit=limit, cur=cur
        ):
            yield converters.db_to_metadata(
                dict(zip(db.origin_intrinsic_metadata_cols, c))
            )

    @timed
    @db_transaction()
    def origin_intrinsic_metadata_search_by_producer(
        self,
        page_token="",
        limit=100,
        ids_only=False,
        mappings=None,
        tool_ids=None,
        db=None,
        cur=None,
    ):
        assert isinstance(page_token, str)
        # we go to limit+1 to check whether we should add next_page_token in
        # the response
        res = db.origin_intrinsic_metadata_search_by_producer(
            page_token, limit + 1, ids_only, mappings, tool_ids, cur
        )
        result = {}
        if ids_only:
            result["origins"] = [origin for (origin,) in res]
            if len(result["origins"]) > limit:
                result["origins"][limit:] = []
                result["next_page_token"] = result["origins"][-1]
        else:
            result["origins"] = [
                converters.db_to_metadata(
                    dict(zip(db.origin_intrinsic_metadata_cols, c))
                )
                for c in res
            ]
            if len(result["origins"]) > limit:
                result["origins"][limit:] = []
                result["next_page_token"] = result["origins"][-1]["id"]
        return result

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
    @db_transaction_generator()
    def indexer_configuration_add(self, tools, db=None, cur=None):
        db.mktemp_indexer_configuration(cur)
        db.copy_to(
            tools,
            "tmp_indexer_configuration",
            ["tool_name", "tool_version", "tool_configuration"],
            cur,
        )

        tools = db.indexer_configuration_add_from_temp(cur)
        count = 0
        for line in tools:
            yield dict(zip(db.indexer_configuration_cols, line))
            count += 1
        send_metric(
            "indexer_configuration:add", count, method_name="indexer_configuration_add"
        )

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
