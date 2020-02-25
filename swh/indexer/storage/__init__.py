# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import json
import psycopg2
import psycopg2.pool

from collections import defaultdict, Counter

from swh.storage.common import db_transaction_generator, db_transaction
from swh.storage.exc import StorageDBError

from . import converters
from .db import Db
from .exc import IndexerStorageArgumentException, DuplicateId


INDEXER_CFG_KEY = 'indexer_storage'


MAPPING_NAMES = ['codemeta', 'gemspec', 'maven', 'npm', 'pkg-info']


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
    if cls == 'remote':
        from .api.client import RemoteStorage as IndexerStorage
    elif cls == 'local':
        from . import IndexerStorage
    elif cls == 'memory':
        from .in_memory import IndexerStorage
    else:
        raise ValueError('Unknown indexer storage class `%s`' % cls)

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
    counter = Counter(item['id'] for item in data)
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

    @db_transaction()
    def check_config(self, *, check_write, db=None, cur=None):
        # Check permissions on one of the tables
        if check_write:
            check = 'INSERT'
        else:
            check = 'SELECT'

        cur.execute(
            "select has_table_privilege(current_user, 'content_mimetype', %s)",  # noqa
            (check,)
        )
        return cur.fetchone()[0]

    @db_transaction_generator()
    def content_mimetype_missing(self, mimetypes, db=None, cur=None):
        for obj in db.content_mimetype_missing_from_list(mimetypes, cur):
            yield obj[0]

    def _content_get_range(self, content_type, start, end,
                           indexer_configuration_id, limit=1000,
                           with_textual_data=False,
                           db=None, cur=None):
        if limit is None:
            raise IndexerStorageArgumentException('limit should not be None')
        if content_type not in db.content_indexer_names:
            err = 'Wrong type. Should be one of [%s]' % (
                ','.join(db.content_indexer_names))
            raise IndexerStorageArgumentException(err)

        ids = []
        next_id = None
        for counter, obj in enumerate(db.content_get_range(
                content_type, start, end, indexer_configuration_id,
                limit=limit+1, with_textual_data=with_textual_data, cur=cur)):
            _id = obj[0]
            if counter >= limit:
                next_id = _id
                break

            ids.append(_id)

        return {
            'ids': ids,
            'next': next_id
        }

    @db_transaction()
    def content_mimetype_get_range(self, start, end, indexer_configuration_id,
                                   limit=1000, db=None, cur=None):
        return self._content_get_range('mimetype', start, end,
                                       indexer_configuration_id, limit=limit,
                                       db=db, cur=cur)

    @db_transaction()
    def content_mimetype_add(self, mimetypes, conflict_update=False, db=None,
                             cur=None):
        check_id_duplicates(mimetypes)
        mimetypes.sort(key=lambda m: m['id'])
        db.mktemp_content_mimetype(cur)
        db.copy_to(mimetypes, 'tmp_content_mimetype',
                   ['id', 'mimetype', 'encoding', 'indexer_configuration_id'],
                   cur)
        db.content_mimetype_add_from_temp(conflict_update, cur)

    @db_transaction_generator()
    def content_mimetype_get(self, ids, db=None, cur=None):
        for c in db.content_mimetype_get_from_list(ids, cur):
            yield converters.db_to_mimetype(
                dict(zip(db.content_mimetype_cols, c)))

    @db_transaction_generator()
    def content_language_missing(self, languages, db=None, cur=None):
        for obj in db.content_language_missing_from_list(languages, cur):
            yield obj[0]

    @db_transaction_generator()
    def content_language_get(self, ids, db=None, cur=None):
        for c in db.content_language_get_from_list(ids, cur):
            yield converters.db_to_language(
                dict(zip(db.content_language_cols, c)))

    @db_transaction()
    def content_language_add(self, languages, conflict_update=False, db=None,
                             cur=None):
        check_id_duplicates(languages)
        languages.sort(key=lambda m: m['id'])
        db.mktemp_content_language(cur)
        # empty language is mapped to 'unknown'
        db.copy_to(
            ({
                'id': l['id'],
                'lang': 'unknown' if not l['lang'] else l['lang'],
                'indexer_configuration_id': l['indexer_configuration_id'],
            } for l in languages),
            'tmp_content_language',
            ['id', 'lang', 'indexer_configuration_id'], cur)

        db.content_language_add_from_temp(conflict_update, cur)

    @db_transaction_generator()
    def content_ctags_missing(self, ctags, db=None, cur=None):
        for obj in db.content_ctags_missing_from_list(ctags, cur):
            yield obj[0]

    @db_transaction_generator()
    def content_ctags_get(self, ids, db=None, cur=None):
        for c in db.content_ctags_get_from_list(ids, cur):
            yield converters.db_to_ctags(dict(zip(db.content_ctags_cols, c)))

    @db_transaction()
    def content_ctags_add(self, ctags, conflict_update=False, db=None,
                          cur=None):
        check_id_duplicates(ctags)
        ctags.sort(key=lambda m: m['id'])

        def _convert_ctags(__ctags):
            """Convert ctags dict to list of ctags.

            """
            for ctags in __ctags:
                yield from converters.ctags_to_db(ctags)

        db.mktemp_content_ctags(cur)
        db.copy_to(list(_convert_ctags(ctags)),
                   tblname='tmp_content_ctags',
                   columns=['id', 'name', 'kind', 'line',
                            'lang', 'indexer_configuration_id'],
                   cur=cur)

        db.content_ctags_add_from_temp(conflict_update, cur)

    @db_transaction_generator()
    def content_ctags_search(self, expression,
                             limit=10, last_sha1=None, db=None, cur=None):
        for obj in db.content_ctags_search(expression, last_sha1, limit,
                                           cur=cur):
            yield converters.db_to_ctags(dict(zip(db.content_ctags_cols, obj)))

    @db_transaction_generator()
    def content_fossology_license_get(self, ids, db=None, cur=None):
        d = defaultdict(list)
        for c in db.content_fossology_license_get_from_list(ids, cur):
            license = dict(zip(db.content_fossology_license_cols, c))

            id_ = license['id']
            d[id_].append(converters.db_to_fossology_license(license))

        for id_, facts in d.items():
            yield {id_: facts}

    @db_transaction()
    def content_fossology_license_add(self, licenses, conflict_update=False,
                                      db=None, cur=None):
        check_id_duplicates(licenses)
        licenses.sort(key=lambda m: m['id'])
        db.mktemp_content_fossology_license(cur)
        db.copy_to(
            ({
                'id': sha1['id'],
                'indexer_configuration_id': sha1['indexer_configuration_id'],
                'license': license,
              } for sha1 in licenses
                for license in sha1['licenses']),
            tblname='tmp_content_fossology_license',
            columns=['id', 'license', 'indexer_configuration_id'],
            cur=cur)
        db.content_fossology_license_add_from_temp(conflict_update, cur)

    @db_transaction()
    def content_fossology_license_get_range(
            self, start, end, indexer_configuration_id,
            limit=1000, db=None, cur=None):
        return self._content_get_range('fossology_license', start, end,
                                       indexer_configuration_id, limit=limit,
                                       with_textual_data=True, db=db, cur=cur)

    @db_transaction_generator()
    def content_metadata_missing(self, metadata, db=None, cur=None):
        for obj in db.content_metadata_missing_from_list(metadata, cur):
            yield obj[0]

    @db_transaction_generator()
    def content_metadata_get(self, ids, db=None, cur=None):
        for c in db.content_metadata_get_from_list(ids, cur):
            yield converters.db_to_metadata(
                dict(zip(db.content_metadata_cols, c)))

    @db_transaction()
    def content_metadata_add(self, metadata, conflict_update=False, db=None,
                             cur=None):
        check_id_duplicates(metadata)
        metadata.sort(key=lambda m: m['id'])

        db.mktemp_content_metadata(cur)

        db.copy_to(metadata, 'tmp_content_metadata',
                   ['id', 'metadata', 'indexer_configuration_id'],
                   cur)
        db.content_metadata_add_from_temp(conflict_update, cur)

    @db_transaction_generator()
    def revision_intrinsic_metadata_missing(self, metadata, db=None, cur=None):
        for obj in db.revision_intrinsic_metadata_missing_from_list(
                metadata, cur):
            yield obj[0]

    @db_transaction_generator()
    def revision_intrinsic_metadata_get(self, ids, db=None, cur=None):
        for c in db.revision_intrinsic_metadata_get_from_list(ids, cur):
            yield converters.db_to_metadata(
                dict(zip(db.revision_intrinsic_metadata_cols, c)))

    @db_transaction()
    def revision_intrinsic_metadata_add(self, metadata, conflict_update=False,
                                        db=None, cur=None):
        check_id_duplicates(metadata)
        metadata.sort(key=lambda m: m['id'])

        db.mktemp_revision_intrinsic_metadata(cur)

        db.copy_to(metadata, 'tmp_revision_intrinsic_metadata',
                   ['id', 'metadata', 'mappings',
                    'indexer_configuration_id'],
                   cur)
        db.revision_intrinsic_metadata_add_from_temp(conflict_update, cur)

    @db_transaction()
    def revision_intrinsic_metadata_delete(self, entries, db=None, cur=None):
        db.revision_intrinsic_metadata_delete(entries, cur)

    @db_transaction_generator()
    def origin_intrinsic_metadata_get(self, ids, db=None, cur=None):
        for c in db.origin_intrinsic_metadata_get_from_list(ids, cur):
            yield converters.db_to_metadata(
                dict(zip(db.origin_intrinsic_metadata_cols, c)))

    @db_transaction()
    def origin_intrinsic_metadata_add(self, metadata,
                                      conflict_update=False, db=None,
                                      cur=None):
        check_id_duplicates(metadata)
        metadata.sort(key=lambda m: m['id'])

        db.mktemp_origin_intrinsic_metadata(cur)

        db.copy_to(metadata, 'tmp_origin_intrinsic_metadata',
                   ['id', 'metadata',
                    'indexer_configuration_id',
                    'from_revision', 'mappings'],
                   cur)
        db.origin_intrinsic_metadata_add_from_temp(conflict_update, cur)

    @db_transaction()
    def origin_intrinsic_metadata_delete(
            self, entries, db=None, cur=None):
        db.origin_intrinsic_metadata_delete(entries, cur)

    @db_transaction_generator()
    def origin_intrinsic_metadata_search_fulltext(
            self, conjunction, limit=100, db=None, cur=None):
        for c in db.origin_intrinsic_metadata_search_fulltext(
                conjunction, limit=limit, cur=cur):
            yield converters.db_to_metadata(
                dict(zip(db.origin_intrinsic_metadata_cols, c)))

    @db_transaction()
    def origin_intrinsic_metadata_search_by_producer(
            self, page_token='', limit=100, ids_only=False,
            mappings=None, tool_ids=None,
            db=None, cur=None):
        assert isinstance(page_token, str)
        # we go to limit+1 to check whether we should add next_page_token in
        # the response
        res = db.origin_intrinsic_metadata_search_by_producer(
            page_token, limit + 1, ids_only, mappings, tool_ids, cur)
        result = {}
        if ids_only:
            result['origins'] = [origin for (origin,) in res]
            if len(result['origins']) > limit:
                result['origins'][limit:] = []
                result['next_page_token'] = result['origins'][-1]
        else:
            result['origins'] = [converters.db_to_metadata(
                dict(zip(db.origin_intrinsic_metadata_cols, c)))for c in res]
            if len(result['origins']) > limit:
                result['origins'][limit:] = []
                result['next_page_token'] = result['origins'][-1]['id']
        return result

    @db_transaction()
    def origin_intrinsic_metadata_stats(
            self, db=None, cur=None):
        mapping_names = [m for m in MAPPING_NAMES]
        select_parts = []

        # Count rows for each mapping
        for mapping_name in mapping_names:
            select_parts.append((
                "sum(case when (mappings @> ARRAY['%s']) "
                "         then 1 else 0 end)"
                ) % mapping_name)

        # Total
        select_parts.append("sum(1)")

        # Rows whose metadata has at least one key that is not '@context'
        select_parts.append(
            "sum(case when ('{}'::jsonb @> (metadata - '@context')) "
            "         then 0 else 1 end)")
        cur.execute('select ' + ', '.join(select_parts)
                    + ' from origin_intrinsic_metadata')
        results = dict(zip(mapping_names + ['total', 'non_empty'],
                           cur.fetchone()))
        return {
            'total': results.pop('total'),
            'non_empty': results.pop('non_empty'),
            'per_mapping': results,
        }

    @db_transaction_generator()
    def indexer_configuration_add(self, tools, db=None, cur=None):
        db.mktemp_indexer_configuration(cur)
        db.copy_to(tools, 'tmp_indexer_configuration',
                   ['tool_name', 'tool_version', 'tool_configuration'],
                   cur)

        tools = db.indexer_configuration_add_from_temp(cur)
        for line in tools:
            yield dict(zip(db.indexer_configuration_cols, line))

    @db_transaction()
    def indexer_configuration_get(self, tool, db=None, cur=None):
        tool_conf = tool['tool_configuration']
        if isinstance(tool_conf, dict):
            tool_conf = json.dumps(tool_conf)
        idx = db.indexer_configuration_get(tool['tool_name'],
                                           tool['tool_version'],
                                           tool_conf)
        if not idx:
            return None
        return dict(zip(db.indexer_configuration_cols, idx))
