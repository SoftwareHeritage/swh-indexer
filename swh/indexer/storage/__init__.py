# Copyright (C) 2015-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import json
import dateutil.parser
import psycopg2

from swh.storage.common import db_transaction_generator, db_transaction
from swh.storage.exc import StorageDBError
from .db import Db

from . import converters


INDEXER_CFG_KEY = 'indexer_storage'


def get_indexer_storage(cls, args):
    """Get an indexer storage object of class `storage_class` with
    arguments `storage_args`.

    Args:
        args (dict): dictionary with keys:
        - cls (str): storage's class, either 'local' or 'remote'
        - args (dict): dictionary with keys

    Returns:
        an instance of swh.indexer's storage (either local or remote)

    Raises:
        ValueError if passed an unknown storage class.

    """
    if cls == 'remote':
        from .api.client import RemoteStorage as IndexerStorage
    elif cls == 'local':
        from . import IndexerStorage
    else:
        raise ValueError('Unknown indexer storage class `%s`' % cls)

    return IndexerStorage(**args)


class IndexerStorage():
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

    def check_config(self, *, check_write):
        """Check that the storage is configured and ready to go."""
        # Check permissions on one of the tables
        with self.get_db().transaction() as cur:
            if check_write:
                check = 'INSERT'
            else:
                check = 'SELECT'

            cur.execute(
                "select has_table_privilege(current_user, 'content_mimetype', %s)",  # noqa
                (check,)
            )
            return cur.fetchone()[0]

        return True

    @db_transaction_generator()
    def content_mimetype_missing(self, mimetypes, db=None, cur=None):
        """List mimetypes missing from storage.

        Args:
            mimetypes (iterable): iterable of dict with keys:

                - id (bytes): sha1 identifier
                - indexer_configuration_id (int): tool used to compute
                  the results

        Yields:
            an iterable of missing id for the tuple (id,
            indexer_configuration_id)

        """
        for obj in db.content_mimetype_missing_from_list(mimetypes, cur):
            yield obj[0]

    @db_transaction()
    def content_mimetype_add(self, mimetypes, conflict_update=False, db=None,
                             cur=None):
        """Add mimetypes not present in storage.

        Args:
            mimetypes (iterable): dictionaries with keys:

                - id (bytes): sha1 identifier
                - mimetype (bytes): raw content's mimetype
                - encoding (bytes): raw content's encoding
                - indexer_configuration_id (int): tool's id used to
                  compute the results
                - conflict_update: Flag to determine if we want to
                  overwrite (true) or skip duplicates (false, the default)

        """
        db.mktemp_content_mimetype(cur)
        db.copy_to(mimetypes, 'tmp_content_mimetype',
                   ['id', 'mimetype', 'encoding', 'indexer_configuration_id'],
                   cur)
        db.content_mimetype_add_from_temp(conflict_update, cur)

    @db_transaction_generator()
    def content_mimetype_get(self, ids, db=None, cur=None):
        """Retrieve full content mimetype per ids.

        Args:
            ids (iterable): sha1 identifier

        Yields:
            mimetypes (iterable): dictionaries with keys:

                id (bytes): sha1 identifier
                mimetype (bytes): raw content's mimetype
                encoding (bytes): raw content's encoding
                tool_id (id): tool's id used to compute the results
                tool_name (str): tool's name
                tool_version (str):  tool's version
                tool_configuration: tool's configuration

        """
        for c in db.content_mimetype_get_from_list(ids, cur):
            yield converters.db_to_mimetype(
                dict(zip(db.content_mimetype_cols, c)))

    @db_transaction_generator()
    def content_language_missing(self, languages, db=None, cur=None):
        """List languages missing from storage.

        Args:
            languages (iterable): dictionaries with keys:

                id (bytes): sha1 identifier
                indexer_configuration_id (int): tool used to compute
                the results

        Yields:
            an iterable of missing id for the tuple (id,
            indexer_configuration_id)

        """
        for obj in db.content_language_missing_from_list(languages, cur):
            yield obj[0]

    @db_transaction_generator()
    def content_language_get(self, ids, db=None, cur=None):
        """Retrieve full content language per ids.

        Args:
            ids (iterable): sha1 identifier

        Yields:
            languages (iterable): dictionaries with keys:

                id (bytes): sha1 identifier
                lang (bytes): raw content's language
                tool_id (id): tool's id used to compute the results
                tool_name (str): tool's name
                tool_version (str):  tool's version
                tool_configuration: tool's configuration

        """
        for c in db.content_language_get_from_list(ids, cur):
            yield converters.db_to_language(
                dict(zip(db.content_language_cols, c)))

    @db_transaction()
    def content_language_add(self, languages, conflict_update=False, db=None,
                             cur=None):
        """Add languages not present in storage.

        Args:
            languages (iterable): dictionaries with keys:

                - id: sha1
                - lang: bytes

            conflict_update: Flag to determine if we want to overwrite (true)
                or skip duplicates (false, the default)

        """
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
        """List ctags missing from storage.

        Args:
            ctags (iterable): dicts with keys:

                id (bytes): sha1 identifier
                indexer_configuration_id (int): tool used to compute
                the results

        Yields:
            an iterable of missing id for the tuple (id,
            indexer_configuration_id)

        """
        for obj in db.content_ctags_missing_from_list(ctags, cur):
            yield obj[0]

    @db_transaction_generator()
    def content_ctags_get(self, ids, db=None, cur=None):
        """Retrieve ctags per id.

        Args:
            ids (iterable): sha1 checksums

        """
        db.store_tmp_bytea(ids, cur)
        for c in db.content_ctags_get_from_temp():
            yield converters.db_to_ctags(dict(zip(db.content_ctags_cols, c)))

    @db_transaction()
    def content_ctags_add(self, ctags, conflict_update=False, db=None,
                          cur=None):
        """Add ctags not present in storage

        Args:
            ctags (iterable): dictionaries with keys:

                - id (bytes): sha1
                - ctags ([list): List of dictionary with keys: name, kind,
                  line, language

        """
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
        """Search through content's raw ctags symbols.

        Args:
            expression (str): Expression to search for
            limit (int): Number of rows to return (default to 10).
            last_sha1 (str): Offset from which retrieving data (default to '').

        Yields:
            rows of ctags including id, name, lang, kind, line, etc...

        """
        for obj in db.content_ctags_search(expression, last_sha1, limit,
                                           cur=cur):
            yield converters.db_to_ctags(dict(zip(db.content_ctags_cols, obj)))

    @db_transaction_generator()
    def content_fossology_license_get(self, ids, db=None, cur=None):
        """Retrieve licenses per id.

        Args:
            ids (iterable): sha1 checksums

        Yields:
            list: dictionaries with the following keys:

            - id (bytes)
            - licenses ([str]): associated licenses for that content

        """
        db.store_tmp_bytea(ids, cur)

        for c in db.content_fossology_license_get_from_temp():
            license = dict(zip(db.content_fossology_license_cols, c))
            yield converters.db_to_fossology_license(license)

    @db_transaction()
    def content_fossology_license_add(self, licenses, conflict_update=False,
                                      db=None, cur=None):
        """Add licenses not present in storage.

        Args:
            licenses (iterable): dictionaries with keys:

                - id: sha1
                - license ([bytes]): List of licenses associated to sha1
                - tool (str): nomossa

            conflict_update: Flag to determine if we want to overwrite (true)
                or skip duplicates (false, the default)

        Returns:
            list: content_license entries which failed due to unknown licenses

        """
        # Then, we add the correct ones
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

    @db_transaction_generator()
    def content_metadata_missing(self, metadatas, db=None, cur=None):
        """List metadatas missing from storage.

        Args:
            metadatas (iterable): dictionaries with keys:

                - id (bytes): sha1 identifier
                - tool_name (str): tool used to compute the results
                - tool_version (str): associated tool's version

        Returns:
            iterable: missing ids

        """
        db.mktemp_content_metadata_missing(cur)
        db.copy_to(metadatas, 'tmp_content_metadata_missing',
                   ['id', 'indexer_configuration_id'], cur)
        for obj in db.content_metadata_missing_from_temp(cur):
            yield obj[0]

    @db_transaction_generator()
    def content_metadata_get(self, ids, db=None, cur=None):
        db.store_tmp_bytea(ids, cur)
        for c in db.content_metadata_get_from_temp():
            yield converters.db_to_metadata(
                dict(zip(db.content_metadata_cols, c)))

    @db_transaction()
    def content_metadata_add(self, metadatas, conflict_update=False, db=None,
                             cur=None):
        """Add metadatas not present in storage.

        Args:
            metadatas (iterable): dictionaries with keys:

                - id: sha1
                - translated_metadata: bytes / jsonb ?

            conflict_update: Flag to determine if we want to overwrite (true)
                or skip duplicates (false, the default)

        """
        db.mktemp_content_metadata(cur)
        # empty metadata is mapped to 'unknown'

        db.copy_to(metadatas, 'tmp_content_metadata',
                   ['id', 'translated_metadata', 'indexer_configuration_id'],
                   cur)
        db.content_metadata_add_from_temp(conflict_update, cur)

    @db_transaction_generator()
    def revision_metadata_missing(self, metadatas, db=None, cur=None):
        """List metadatas missing from storage.

        Args:
            metadatas (iterable): dictionaries with keys:

               - id (bytes): sha1_git revision identifier
               - tool_name (str): tool used to compute the results
               - tool_version (str): associated tool's version

        Returns:
            iterable: missing ids

        """
        db.mktemp_revision_metadata_missing(cur)
        db.copy_to(metadatas, 'tmp_revision_metadata_missing',
                   ['id', 'indexer_configuration_id'], cur)
        for obj in db.revision_metadata_missing_from_temp(cur):
            yield obj[0]

    @db_transaction_generator()
    def revision_metadata_get(self, ids, db=None, cur=None):
        db.store_tmp_bytea(ids, cur)
        for c in db.revision_metadata_get_from_temp():
            yield converters.db_to_metadata(
                dict(zip(db.revision_metadata_cols, c)))

    @db_transaction()
    def revision_metadata_add(self, metadatas, conflict_update=False, db=None,
                              cur=None):
        """Add metadatas not present in storage.

        Args:
            metadatas (iterable): dictionaries with keys:

                - id: sha1_git of revision
                - translated_metadata: bytes / jsonb ?

            conflict_update: Flag to determine if we want to overwrite (true)
              or skip duplicates (false, the default)

        """
        db.mktemp_revision_metadata(cur)
        # empty metadata is mapped to 'unknown'

        db.copy_to(metadatas, 'tmp_revision_metadata',
                   ['id', 'translated_metadata', 'indexer_configuration_id'],
                   cur)
        db.revision_metadata_add_from_temp(conflict_update, cur)

    @db_transaction()
    def origin_metadata_add(self, origin_id, ts, provider, tool, metadata,
                            db=None, cur=None):
        """ Add an origin_metadata for the origin at ts with provenance and
        metadata.

        Args:
            origin_id (int): the origin's id for which the metadata is added
            ts (datetime): timestamp of the found metadata
            provider (int): the provider of metadata (ex:'hal')
            tool (int): tool used to extract metadata
            metadata (jsonb): the metadata retrieved at the time and location

        Returns:
            id (int): the origin_metadata unique id
        """
        if isinstance(ts, str):
            ts = dateutil.parser.parse(ts)

        return db.origin_metadata_add(origin_id, ts, provider, tool,
                                      metadata, cur)

    @db_transaction_generator()
    def origin_metadata_get_by(self, origin_id, provider_type=None, db=None,
                               cur=None):
        """Retrieve list of all origin_metadata entries for the origin_id

        Args:
            origin_id (int): the unique origin identifier
            provider_type (str): (optional) type of provider

        Returns:
            list of dicts: the origin_metadata dictionary with the keys:

            - id (int): origin_metadata's id
            - origin_id (int): origin's id
            - discovery_date (datetime): timestamp of discovery
            - tool_id (int): metadata's extracting tool
            - metadata (jsonb)
            - provider_id (int): metadata's provider
            - provider_name (str)
            - provider_type (str)
            - provider_url (str)

        """
        for line in db.origin_metadata_get_by(origin_id, provider_type, cur):
            yield dict(zip(db.origin_metadata_get_cols, line))

    @db_transaction_generator()
    def indexer_configuration_add(self, tools, db=None, cur=None):
        """Add new tools to the storage.

        Args:
            tools ([dict]): List of dictionary representing tool to
            insert in the db. Dictionary with the following keys::

                tool_name (str): tool's name
                tool_version (str): tool's version
                tool_configuration (dict): tool's configuration (free form
                                           dict)

        Returns:
            List of dict inserted in the db (holding the id key as
            well).  The order of the list is not guaranteed to match
            the order of the initial list.

        """
        db.mktemp_indexer_configuration(cur)
        db.copy_to(tools, 'tmp_indexer_configuration',
                   ['tool_name', 'tool_version', 'tool_configuration'],
                   cur)

        tools = db.indexer_configuration_add_from_temp(cur)
        for line in tools:
            yield dict(zip(db.indexer_configuration_cols, line))

    @db_transaction()
    def indexer_configuration_get(self, tool, db=None, cur=None):
        """Retrieve tool information.

        Args:
            tool (dict): Dictionary representing a tool with the
            following keys::

                tool_name (str): tool's name
                tool_version (str): tool's version
                tool_configuration (dict): tool's configuration (free form
                                           dict)

        Returns:
            The identifier of the tool if it exists, None otherwise.

        """
        tool_conf = tool['tool_configuration']
        if isinstance(tool_conf, dict):
            tool_conf = json.dumps(tool_conf)
        idx = db.indexer_configuration_get(tool['tool_name'],
                                           tool['tool_version'],
                                           tool_conf)
        if not idx:
            return None
        return dict(zip(db.indexer_configuration_cols, idx))
