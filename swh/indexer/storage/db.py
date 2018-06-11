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
    @stored_procedure('swh_mktemp_bytea')
    def mktemp_bytea(self, cur=None): pass

    def store_tmp_bytea(self, ids, cur=None):
        """Store the given identifiers in a new tmp_bytea table"""
        cur = self._cursor(cur)

        self.mktemp_bytea(cur)
        self.copy_to(({'id': elem} for elem in ids), 'tmp_bytea',
                     ['id'], cur)

    content_mimetype_hash_keys = ['id', 'indexer_configuration_id']

    def content_mimetype_missing_from_list(self, mimetypes, cur=None):
        """List missing mimetypes.

        """
        cur = self._cursor(cur)
        keys = ', '.join(self.content_mimetype_hash_keys)
        equality = ' AND '.join(
            ('t.%s = c.%s' % (key, key))
            for key in self.content_mimetype_hash_keys
        )
        yield from execute_values_to_bytes(
            cur, """
            select %s from (values %%s) as t(%s)
            where not exists (
                select 1 from content_mimetype c
                where %s
            )
            """ % (keys, keys, equality),
            (tuple(m[k] for k in self.content_mimetype_hash_keys)
             for m in mimetypes)
        )

    content_mimetype_cols = [
        'id', 'mimetype', 'encoding',
        'tool_id', 'tool_name', 'tool_version', 'tool_configuration']

    @stored_procedure('swh_mktemp_content_mimetype')
    def mktemp_content_mimetype(self, cur=None): pass

    def content_mimetype_add_from_temp(self, conflict_update, cur=None):
        self._cursor(cur).execute("SELECT swh_content_mimetype_add(%s)",
                                  (conflict_update, ))

    def content_mimetype_get_from_temp(self, cur=None):
        cur = self._cursor(cur)
        query = "SELECT %s FROM swh_content_mimetype_get()" % (
            ','.join(self.content_mimetype_cols))
        cur.execute(query)
        yield from cursor_to_bytes(cur)

    content_language_cols = [
        'id', 'lang',
        'tool_id', 'tool_name', 'tool_version', 'tool_configuration']

    @stored_procedure('swh_mktemp_content_language')
    def mktemp_content_language(self, cur=None): pass

    @stored_procedure('swh_mktemp_content_language_missing')
    def mktemp_content_language_missing(self, cur=None): pass

    def content_language_missing_from_temp(self, cur=None):
        """List missing languages.

        """
        cur = self._cursor(cur)
        cur.execute("SELECT * FROM swh_content_language_missing()")
        yield from cursor_to_bytes(cur)

    def content_language_add_from_temp(self, conflict_update, cur=None):
        self._cursor(cur).execute("SELECT swh_content_language_add(%s)",
                                  (conflict_update, ))

    def content_language_get_from_temp(self, cur=None):
        cur = self._cursor(cur)
        query = "SELECT %s FROM swh_content_language_get()" % (
            ','.join(self.content_language_cols))
        cur.execute(query)
        yield from cursor_to_bytes(cur)

    content_ctags_cols = [
        'id', 'name', 'kind', 'line', 'lang',
        'tool_id', 'tool_name', 'tool_version', 'tool_configuration']

    @stored_procedure('swh_mktemp_content_ctags')
    def mktemp_content_ctags(self, cur=None): pass

    @stored_procedure('swh_mktemp_content_ctags_missing')
    def mktemp_content_ctags_missing(self, cur=None): pass

    def content_ctags_missing_from_temp(self, cur=None):
        """List missing ctags.

        """
        cur = self._cursor(cur)
        cur.execute("SELECT * FROM swh_content_ctags_missing()")
        yield from cursor_to_bytes(cur)

    def content_ctags_add_from_temp(self, conflict_update, cur=None):
        self._cursor(cur).execute("SELECT swh_content_ctags_add(%s)",
                                  (conflict_update, ))

    def content_ctags_get_from_temp(self, cur=None):
        cur = self._cursor(cur)
        query = "SELECT %s FROM swh_content_ctags_get()" % (
            ','.join(self.content_ctags_cols))
        cur.execute(query)
        yield from cursor_to_bytes(cur)

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

    def content_fossology_license_get_from_temp(self, cur=None):
        """Retrieve licenses per content.

        """
        cur = self._cursor(cur)
        query = "SELECT %s FROM swh_content_fossology_license_get()" % (
            ','.join(self.content_fossology_license_cols))
        cur.execute(query)
        yield from cursor_to_bytes(cur)

    content_metadata_cols = [
        'id', 'translated_metadata',
        'tool_id', 'tool_name', 'tool_version', 'tool_configuration']

    @stored_procedure('swh_mktemp_content_metadata')
    def mktemp_content_metadata(self, cur=None): pass

    @stored_procedure('swh_mktemp_content_metadata_missing')
    def mktemp_content_metadata_missing(self, cur=None): pass

    def content_metadata_missing_from_temp(self, cur=None):
        """List missing metadatas.

        """
        cur = self._cursor(cur)
        cur.execute("SELECT * FROM swh_content_metadata_missing()")
        yield from cursor_to_bytes(cur)

    def content_metadata_add_from_temp(self, conflict_update, cur=None):
        self._cursor(cur).execute("SELECT swh_content_metadata_add(%s)",
                                  (conflict_update, ))

    def content_metadata_get_from_temp(self, cur=None):
        cur = self._cursor(cur)
        query = "SELECT %s FROM swh_content_metadata_get()" % (
            ','.join(self.content_metadata_cols))
        cur.execute(query)
        yield from cursor_to_bytes(cur)

    revision_metadata_cols = [
        'id', 'translated_metadata',
        'tool_id', 'tool_name', 'tool_version', 'tool_configuration']

    @stored_procedure('swh_mktemp_revision_metadata')
    def mktemp_revision_metadata(self, cur=None): pass

    @stored_procedure('swh_mktemp_revision_metadata_missing')
    def mktemp_revision_metadata_missing(self, cur=None): pass

    def revision_metadata_missing_from_temp(self, cur=None):
        """List missing metadatas.

        """
        cur = self._cursor(cur)
        cur.execute("SELECT * FROM swh_revision_metadata_missing()")
        yield from cursor_to_bytes(cur)

    def revision_metadata_add_from_temp(self, conflict_update, cur=None):
        self._cursor(cur).execute("SELECT swh_revision_metadata_add(%s)",
                                  (conflict_update, ))

    def revision_metadata_get_from_temp(self, cur=None):
        cur = self._cursor(cur)
        query = "SELECT %s FROM swh_revision_metadata_get()" % (
            ','.join(self.revision_metadata_cols))
        cur.execute(query)
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
