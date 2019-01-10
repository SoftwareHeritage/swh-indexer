# Copyright (C) 2015-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.model import hashutil

from swh.storage.db import BaseDb, stored_procedure, cursor_to_bytes
from swh.storage.db import line_to_bytes, execute_values_to_bytes


class Db(BaseDb):
    """Proxy to the SWH Indexer DB, with wrappers around stored procedures

    """
    content_mimetype_hash_keys = ['id', 'indexer_configuration_id']

    def _missing_from_list(self, table, data, hash_keys, cur=None):
        """Read from table the data with hash_keys that are missing.

        Args:
            table (str): Table name (e.g content_mimetype, content_language,
              etc...)
            data (dict): Dict of data to read from
            hash_keys ([str]): List of keys to read in the data dict.

        Yields:
            The data which is missing from the db.

        """
        cur = self._cursor(cur)
        keys = ', '.join(hash_keys)
        equality = ' AND '.join(
            ('t.%s = c.%s' % (key, key)) for key in hash_keys
        )
        yield from execute_values_to_bytes(
            cur, """
            select %s from (values %%s) as t(%s)
            where not exists (
                select 1 from %s c
                where %s
            )
            """ % (keys, keys, table, equality),
            (tuple(m[k] for k in hash_keys) for m in data)
        )

    def content_mimetype_missing_from_list(self, mimetypes, cur=None):
        """List missing mimetypes.

        """
        yield from self._missing_from_list(
            'content_mimetype', mimetypes, self.content_mimetype_hash_keys,
            cur=cur)

    content_mimetype_cols = [
        'id', 'mimetype', 'encoding',
        'tool_id', 'tool_name', 'tool_version', 'tool_configuration']

    @stored_procedure('swh_mktemp_content_mimetype')
    def mktemp_content_mimetype(self, cur=None): pass

    def content_mimetype_add_from_temp(self, conflict_update, cur=None):
        self._cursor(cur).execute("SELECT swh_content_mimetype_add(%s)",
                                  (conflict_update, ))

    def _convert_key(self, key, main_table='c'):
        """Convert keys according to specific use in the module.

        Args:
            key (str): Key expression to change according to the alias
              used in the query
            main_table (str): Alias to use for the main table. Default
              to c for content_{something}.

        Expected:
            Tables content_{something} being aliased as 'c' (something
            in {language, mimetype, ...}), table indexer_configuration
            being aliased as 'i'.

        """
        if key == 'id':
            return '%s.id' % main_table
        elif key == 'tool_id':
            return 'i.id as tool_id'
        elif key == 'licenses':
            return '''
                array(select name
                      from fossology_license
                      where id = ANY(
                         array_agg(%s.license_id))) as licenses''' % main_table
        return key

    def _get_from_list(self, table, ids, cols, cur=None, id_col='id'):
        """Fetches entries from the `table` such that their `id` field
        (or whatever is given to `id_col`) is in `ids`.
        Returns the columns `cols`.
        The `cur`sor is used to connect to the database.
        """
        cur = self._cursor(cur)
        keys = map(self._convert_key, cols)
        query = """
            select {keys}
            from (values %s) as t(id)
            inner join {table} c
                on c.{id_col}=t.id
            inner join indexer_configuration i
                on c.indexer_configuration_id=i.id;
            """.format(
                keys=', '.join(keys),
                id_col=id_col,
                table=table)
        yield from execute_values_to_bytes(
            cur, query,
            ((_id,) for _id in ids)
        )

    content_indexer_names = {
        'mimetype': 'content_mimetype',
        'fossology_license': 'content_fossology_license',
    }

    def content_get_range(self, content_type, start, end,
                          indexer_configuration_id, limit=1000,
                          with_textual_data=False, cur=None):
        """Retrieve contents with content_type, within range [start, end]
           bound by limit and associated to the given indexer
           configuration id.

           When asking to work on textual content, that filters on the
           mimetype table with any mimetype that is not binary.

        """
        cur = self._cursor(cur)
        table = self.content_indexer_names[content_type]
        if with_textual_data:
            extra = """inner join content_mimetype cm
                         on (t.id=cm.id and cm.mimetype like 'text/%%')"""
        else:
            extra = ""
        query = """select t.id
                   from %s t
                   inner join indexer_configuration ic
                   on t.indexer_configuration_id=ic.id
                   %s
                   where ic.id=%%s and
                         %%s <= t.id and t.id <= %%s
                   order by t.indexer_configuration_id, t.id
                   limit %%s""" % (table, extra)
        cur.execute(query, (indexer_configuration_id, start, end, limit))
        yield from cursor_to_bytes(cur)

    def content_mimetype_get_from_list(self, ids, cur=None):
        yield from self._get_from_list(
            'content_mimetype', ids, self.content_mimetype_cols, cur=cur)

    content_language_hash_keys = ['id', 'indexer_configuration_id']

    def content_language_missing_from_list(self, languages, cur=None):
        """List missing languages.

        """
        yield from self._missing_from_list(
            'content_language', languages, self.content_language_hash_keys,
            cur=cur)

    content_language_cols = [
        'id', 'lang',
        'tool_id', 'tool_name', 'tool_version', 'tool_configuration']

    @stored_procedure('swh_mktemp_content_language')
    def mktemp_content_language(self, cur=None): pass

    def content_language_add_from_temp(self, conflict_update, cur=None):
        self._cursor(cur).execute("SELECT swh_content_language_add(%s)",
                                  (conflict_update, ))

    def content_language_get_from_list(self, ids, cur=None):
        yield from self._get_from_list(
            'content_language', ids, self.content_language_cols, cur=cur)

    content_ctags_hash_keys = ['id', 'indexer_configuration_id']

    def content_ctags_missing_from_list(self, ctags, cur=None):
        """List missing ctags.

        """
        yield from self._missing_from_list(
            'content_ctags', ctags, self.content_ctags_hash_keys,
            cur=cur)

    content_ctags_cols = [
        'id', 'name', 'kind', 'line', 'lang',
        'tool_id', 'tool_name', 'tool_version', 'tool_configuration']

    @stored_procedure('swh_mktemp_content_ctags')
    def mktemp_content_ctags(self, cur=None): pass

    def content_ctags_add_from_temp(self, conflict_update, cur=None):
        self._cursor(cur).execute("SELECT swh_content_ctags_add(%s)",
                                  (conflict_update, ))

    def content_ctags_get_from_list(self, ids, cur=None):
        cur = self._cursor(cur)
        keys = map(self._convert_key, self.content_ctags_cols)
        yield from execute_values_to_bytes(
            cur, """
            select %s
            from (values %%s) as t(id)
            inner join content_ctags c
                on c.id=t.id
            inner join indexer_configuration i
                on c.indexer_configuration_id=i.id
            order by line
            """ % ', '.join(keys),
            ((_id,) for _id in ids)
         )

    def content_ctags_search(self, expression, last_sha1, limit, cur=None):
        cur = self._cursor(cur)
        if not last_sha1:
            query = """SELECT %s
                       FROM swh_content_ctags_search(%%s, %%s)""" % (
                           ','.join(self.content_ctags_cols))
            cur.execute(query, (expression, limit))
        else:
            if last_sha1 and isinstance(last_sha1, bytes):
                last_sha1 = '\\x%s' % hashutil.hash_to_hex(last_sha1)
            elif last_sha1:
                last_sha1 = '\\x%s' % last_sha1

            query = """SELECT %s
                       FROM swh_content_ctags_search(%%s, %%s, %%s)""" % (
                           ','.join(self.content_ctags_cols))
            cur.execute(query, (expression, limit, last_sha1))

        yield from cursor_to_bytes(cur)

    content_fossology_license_cols = [
        'id', 'tool_id', 'tool_name', 'tool_version', 'tool_configuration',
        'licenses']

    @stored_procedure('swh_mktemp_content_fossology_license')
    def mktemp_content_fossology_license(self, cur=None): pass

    def content_fossology_license_add_from_temp(self, conflict_update,
                                                cur=None):
        """Add new licenses per content.

        """
        self._cursor(cur).execute(
            "SELECT swh_content_fossology_license_add(%s)",
            (conflict_update, ))

    def content_fossology_license_get_from_list(self, ids, cur=None):
        """Retrieve licenses per id.

        """
        cur = self._cursor(cur)
        keys = map(self._convert_key, self.content_fossology_license_cols)
        yield from execute_values_to_bytes(
            cur, """
            select %s
            from (values %%s) as t(id)
            inner join content_fossology_license c on t.id=c.id
            inner join indexer_configuration i
                on i.id=c.indexer_configuration_id
            group by c.id, i.id, i.tool_name, i.tool_version,
                     i.tool_configuration;
            """ % ', '.join(keys),
            ((_id,) for _id in ids)
        )

    content_metadata_hash_keys = ['id', 'indexer_configuration_id']

    def content_metadata_missing_from_list(self, metadata, cur=None):
        """List missing metadata.

        """
        yield from self._missing_from_list(
            'content_metadata', metadata, self.content_metadata_hash_keys,
            cur=cur)

    content_metadata_cols = [
        'id', 'translated_metadata',
        'tool_id', 'tool_name', 'tool_version', 'tool_configuration']

    @stored_procedure('swh_mktemp_content_metadata')
    def mktemp_content_metadata(self, cur=None): pass

    def content_metadata_add_from_temp(self, conflict_update, cur=None):
        self._cursor(cur).execute("SELECT swh_content_metadata_add(%s)",
                                  (conflict_update, ))

    def content_metadata_get_from_list(self, ids, cur=None):
        yield from self._get_from_list(
            'content_metadata', ids, self.content_metadata_cols, cur=cur)

    revision_metadata_hash_keys = ['id', 'indexer_configuration_id']

    def revision_metadata_missing_from_list(self, metadata, cur=None):
        """List missing metadata.

        """
        yield from self._missing_from_list(
            'revision_metadata', metadata, self.revision_metadata_hash_keys,
            cur=cur)

    revision_metadata_cols = [
        'id', 'translated_metadata',
        'tool_id', 'tool_name', 'tool_version', 'tool_configuration']

    @stored_procedure('swh_mktemp_revision_metadata')
    def mktemp_revision_metadata(self, cur=None): pass

    def revision_metadata_add_from_temp(self, conflict_update, cur=None):
        self._cursor(cur).execute("SELECT swh_revision_metadata_add(%s)",
                                  (conflict_update, ))

    def revision_metadata_get_from_list(self, ids, cur=None):
        yield from self._get_from_list(
            'revision_metadata', ids, self.revision_metadata_cols, cur=cur)

    origin_intrinsic_metadata_cols = [
        'origin_id', 'metadata', 'from_revision',
        'tool_id', 'tool_name', 'tool_version', 'tool_configuration']

    origin_intrinsic_metadata_regconfig = 'pg_catalog.simple'
    """The dictionary used to normalize 'metadata' and queries.
    'pg_catalog.simple' provides no stopword, so it should be suitable
    for proper names and non-English content.
    When updating this value, make sure to add a new index on
    origin_intrinsic_metadata.metadata."""

    @stored_procedure('swh_mktemp_origin_intrinsic_metadata')
    def mktemp_origin_intrinsic_metadata(self, cur=None): pass

    def origin_intrinsic_metadata_add_from_temp(
            self, conflict_update, cur=None):
        cur = self._cursor(cur)
        cur.execute(
                "SELECT swh_origin_intrinsic_metadata_add(%s)",
                (conflict_update, ))

    def origin_intrinsic_metadata_get_from_list(self, orig_ids, cur=None):
        yield from self._get_from_list(
            'origin_intrinsic_metadata', orig_ids,
            self.origin_intrinsic_metadata_cols, cur=cur,
            id_col='origin_id')

    def origin_intrinsic_metadata_search_fulltext(self, terms, *, limit,
                                                  cur=None):
        regconfig = self.origin_intrinsic_metadata_regconfig
        tsquery_template = ' && '.join("plainto_tsquery('%s', %%s)" % regconfig
                                       for _ in terms)
        tsquery_args = [(term,) for term in terms]
        keys = map(self._convert_key, self.origin_intrinsic_metadata_cols)
        query = ("SELECT {keys} FROM origin_intrinsic_metadata AS oim "
                 "INNER JOIN indexer_configuration AS i "
                 "ON oim.indexer_configuration_id=i.id "
                 "JOIN LATERAL (SELECT {tsquery_template}) AS s(tsq) ON true "
                 "WHERE to_tsvector('{regconfig}', metadata) @@ tsq "
                 "ORDER BY ts_rank(oim.metadata_tsvector, tsq, 1) DESC "
                 "LIMIT %s;"
                 ).format(keys=', '.join(keys),
                          regconfig=regconfig,
                          tsquery_template=tsquery_template)
        cur.execute(query, tsquery_args + [limit])
        yield from cursor_to_bytes(cur)

    indexer_configuration_cols = ['id', 'tool_name', 'tool_version',
                                  'tool_configuration']

    @stored_procedure('swh_mktemp_indexer_configuration')
    def mktemp_indexer_configuration(self, cur=None):
        pass

    def indexer_configuration_add_from_temp(self, cur=None):
        cur = self._cursor(cur)
        cur.execute("SELECT %s from swh_indexer_configuration_add()" % (
            ','.join(self.indexer_configuration_cols), ))
        yield from cursor_to_bytes(cur)

    def indexer_configuration_get(self, tool_name,
                                  tool_version, tool_configuration, cur=None):
        cur = self._cursor(cur)
        cur.execute('''select %s
                       from indexer_configuration
                       where tool_name=%%s and
                             tool_version=%%s and
                             tool_configuration=%%s''' % (
                                 ','.join(self.indexer_configuration_cols)),
                    (tool_name, tool_version, tool_configuration))

        data = cur.fetchone()
        if not data:
            return None
        return line_to_bytes(data)
