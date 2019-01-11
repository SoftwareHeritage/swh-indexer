# Copyright (C) 2015-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import json
import psycopg2

from collections import defaultdict

from swh.core.api import remote_api_endpoint
from swh.storage.common import db_transaction_generator, db_transaction
from swh.storage.exc import StorageDBError
from .db import Db

from . import converters


INDEXER_CFG_KEY = 'indexer_storage'


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

    @remote_api_endpoint('check_config')
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

    @remote_api_endpoint('content_mimetype/missing')
    @db_transaction_generator()
    def content_mimetype_missing(self, mimetypes, db=None, cur=None):
        """Generate mimetypes missing from storage.

        Args:
            mimetypes (iterable): iterable of dict with keys:

              - **id** (bytes): sha1 identifier
              - **indexer_configuration_id** (int): tool used to compute the
                results

        Yields:
            tuple (id, indexer_configuration_id): missing id

        """
        for obj in db.content_mimetype_missing_from_list(mimetypes, cur):
            yield obj[0]

    def _content_get_range(self, content_type, start, end,
                           indexer_configuration_id, limit=1000,
                           with_textual_data=False,
                           db=None, cur=None):
        """Retrieve ids of type content_type within range [start, end] bound
           by limit.

        Args:
            **content_type** (str): content's type (mimetype, language, etc...)
            **start** (bytes): Starting identifier range (expected smaller
                           than end)
            **end** (bytes): Ending identifier range (expected larger
                             than start)
            **indexer_configuration_id** (int): The tool used to index data
            **limit** (int): Limit result (default to 1000)
            **with_textual_data** (bool): Deal with only textual
                                          content (True) or all
                                          content (all contents by
                                          defaults, False)

        Raises:
            ValueError for;
            - limit to None
            - wrong content_type provided

        Returns:
            a dict with keys:
            - **ids** [bytes]: iterable of content ids within the range.
            - **next** (Optional[bytes]): The next range of sha1 starts at
                                          this sha1 if any

        """
        if limit is None:
            raise ValueError('Development error: limit should not be None')
        if content_type not in db.content_indexer_names:
            err = 'Development error: Wrong type. Should be one of [%s]' % (
                ','.join(db.content_indexer_names))
            raise ValueError(err)

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

    @remote_api_endpoint('content_mimetype/range')
    @db_transaction()
    def content_mimetype_get_range(self, start, end, indexer_configuration_id,
                                   limit=1000, db=None, cur=None):
        """Retrieve mimetypes within range [start, end] bound by limit.

        Args:
            **start** (bytes): Starting identifier range (expected smaller
                           than end)
            **end** (bytes): Ending identifier range (expected larger
                             than start)
            **indexer_configuration_id** (int): The tool used to index data
            **limit** (int): Limit result (default to 1000)

        Raises:
            ValueError for limit to None

        Returns:
            a dict with keys:
            - **ids** [bytes]: iterable of content ids within the range.
            - **next** (Optional[bytes]): The next range of sha1 starts at
                                          this sha1 if any

        """
        return self._content_get_range('mimetype', start, end,
                                       indexer_configuration_id, limit=limit,
                                       db=db, cur=cur)

    @remote_api_endpoint('content_mimetype/add')
    @db_transaction()
    def content_mimetype_add(self, mimetypes, conflict_update=False, db=None,
                             cur=None):
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

        """
        db.mktemp_content_mimetype(cur)
        db.copy_to(mimetypes, 'tmp_content_mimetype',
                   ['id', 'mimetype', 'encoding', 'indexer_configuration_id'],
                   cur)
        db.content_mimetype_add_from_temp(conflict_update, cur)

    @remote_api_endpoint('content_mimetype')
    @db_transaction_generator()
    def content_mimetype_get(self, ids, db=None, cur=None):
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
        for c in db.content_mimetype_get_from_list(ids, cur):
            yield converters.db_to_mimetype(
                dict(zip(db.content_mimetype_cols, c)))

    @remote_api_endpoint('content_language/missing')
    @db_transaction_generator()
    def content_language_missing(self, languages, db=None, cur=None):
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
        for obj in db.content_language_missing_from_list(languages, cur):
            yield obj[0]

    @remote_api_endpoint('content_language')
    @db_transaction_generator()
    def content_language_get(self, ids, db=None, cur=None):
        """Retrieve full content language per ids.

        Args:
            ids (iterable): sha1 identifier

        Yields:
            languages (iterable): dictionaries with keys:

                - **id** (bytes): sha1 identifier
                - **lang** (bytes): raw content's language
                - **tool** (dict): Tool used to compute the language

        """
        for c in db.content_language_get_from_list(ids, cur):
            yield converters.db_to_language(
                dict(zip(db.content_language_cols, c)))

    @remote_api_endpoint('content_language/add')
    @db_transaction()
    def content_language_add(self, languages, conflict_update=False, db=None,
                             cur=None):
        """Add languages not present in storage.

        Args:
            languages (iterable): dictionaries with keys:

                - **id** (bytes): sha1
                - **lang** (bytes): language detected

            conflict_update (bool): Flag to determine if we want to
                overwrite (true) or skip duplicates (false, the
                default)

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

    @remote_api_endpoint('content/ctags/missing')
    @db_transaction_generator()
    def content_ctags_missing(self, ctags, db=None, cur=None):
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
        for obj in db.content_ctags_missing_from_list(ctags, cur):
            yield obj[0]

    @remote_api_endpoint('content/ctags')
    @db_transaction_generator()
    def content_ctags_get(self, ids, db=None, cur=None):
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
        for c in db.content_ctags_get_from_list(ids, cur):
            yield converters.db_to_ctags(dict(zip(db.content_ctags_cols, c)))

    @remote_api_endpoint('content/ctags/add')
    @db_transaction()
    def content_ctags_add(self, ctags, conflict_update=False, db=None,
                          cur=None):
        """Add ctags not present in storage

        Args:
            ctags (iterable): dictionaries with keys:

                - **id** (bytes): sha1
                - **ctags** ([list): List of dictionary with keys: name, kind,
                  line, lang

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

    @remote_api_endpoint('content/ctags/search')
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

    @remote_api_endpoint('content/fossology_license')
    @db_transaction_generator()
    def content_fossology_license_get(self, ids, db=None, cur=None):
        """Retrieve licenses per id.

        Args:
            ids (iterable): sha1 checksums

        Yields:
            `{id: facts}` where `facts` is a dict with the following keys:

                - **licenses** ([str]): associated licenses for that content
                - **tool** (dict): Tool used to compute the license

        """
        d = defaultdict(list)
        for c in db.content_fossology_license_get_from_list(ids, cur):
            license = dict(zip(db.content_fossology_license_cols, c))

            id_ = license['id']
            d[id_].append(converters.db_to_fossology_license(license))

        for id_, facts in d.items():
            yield {id_: facts}

    @remote_api_endpoint('content/fossology_license/add')
    @db_transaction()
    def content_fossology_license_add(self, licenses, conflict_update=False,
                                      db=None, cur=None):
        """Add licenses not present in storage.

        Args:
            licenses (iterable): dictionaries with keys:

                - **id**: sha1
                - **licenses** ([bytes]): List of licenses associated to sha1
                - **tool** (str): nomossa

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

    @remote_api_endpoint('content/fossology_license/range')
    @db_transaction()
    def content_fossology_license_get_range(
            self, start, end, indexer_configuration_id,
            limit=1000, db=None, cur=None):
        """Retrieve licenses within range [start, end] bound by limit.

        Args:
            **start** (bytes): Starting identifier range (expected smaller
                           than end)
            **end** (bytes): Ending identifier range (expected larger
                             than start)
            **indexer_configuration_id** (int): The tool used to index data
            **limit** (int): Limit result (default to 1000)

        Raises:
            ValueError for limit to None

        Returns:
            a dict with keys:
            - **ids** [bytes]: iterable of content ids within the range.
            - **next** (Optional[bytes]): The next range of sha1 starts at
                                          this sha1 if any

        """
        return self._content_get_range('fossology_license', start, end,
                                       indexer_configuration_id, limit=limit,
                                       with_textual_data=True, db=db, cur=cur)

    @remote_api_endpoint('content_metadata/missing')
    @db_transaction_generator()
    def content_metadata_missing(self, metadata, db=None, cur=None):
        """List metadata missing from storage.

        Args:
            metadata (iterable): dictionaries with keys:

                - **id** (bytes): sha1 identifier
                - **indexer_configuration_id** (int): tool used to compute
                  the results

        Yields:
            missing sha1s

        """
        for obj in db.content_metadata_missing_from_list(metadata, cur):
            yield obj[0]

    @remote_api_endpoint('content_metadata')
    @db_transaction_generator()
    def content_metadata_get(self, ids, db=None, cur=None):
        """Retrieve metadata per id.

        Args:
            ids (iterable): sha1 checksums

        Yields:
            dictionaries with the following keys:

                id (bytes)
                translated_metadata (str): associated metadata
                tool (dict): tool used to compute metadata

        """
        for c in db.content_metadata_get_from_list(ids, cur):
            yield converters.db_to_metadata(
                dict(zip(db.content_metadata_cols, c)))

    @remote_api_endpoint('content_metadata/add')
    @db_transaction()
    def content_metadata_add(self, metadata, conflict_update=False, db=None,
                             cur=None):
        """Add metadata not present in storage.

        Args:
            metadata (iterable): dictionaries with keys:

                - **id**: sha1
                - **translated_metadata**: arbitrary dict

            conflict_update: Flag to determine if we want to overwrite (true)
                or skip duplicates (false, the default)

        """
        db.mktemp_content_metadata(cur)

        db.copy_to(metadata, 'tmp_content_metadata',
                   ['id', 'translated_metadata', 'indexer_configuration_id'],
                   cur)
        db.content_metadata_add_from_temp(conflict_update, cur)

    @remote_api_endpoint('revision_metadata/missing')
    @db_transaction_generator()
    def revision_metadata_missing(self, metadata, db=None, cur=None):
        """List metadata missing from storage.

        Args:
            metadata (iterable): dictionaries with keys:

               - **id** (bytes): sha1_git revision identifier
               - **indexer_configuration_id** (int): tool used to compute
                 the results

        Yields:
            missing ids

        """
        for obj in db.revision_metadata_missing_from_list(metadata, cur):
            yield obj[0]

    @remote_api_endpoint('revision_metadata')
    @db_transaction_generator()
    def revision_metadata_get(self, ids, db=None, cur=None):
        """Retrieve revision metadata per id.

        Args:
            ids (iterable): sha1 checksums

        Yields:
            dictionaries with the following keys:

                - **id** (bytes)
                - **translated_metadata** (str): associated metadata
                - **tool** (dict): tool used to compute metadata

        """
        for c in db.revision_metadata_get_from_list(ids, cur):
            yield converters.db_to_metadata(
                dict(zip(db.revision_metadata_cols, c)))

    @remote_api_endpoint('revision_metadata/add')
    @db_transaction()
    def revision_metadata_add(self, metadata, conflict_update=False, db=None,
                              cur=None):
        """Add metadata not present in storage.

        Args:
            metadata (iterable): dictionaries with keys:

                - **id**: sha1_git of revision
                - **translated_metadata**: arbitrary dict
                - **indexer_configuration_id**: tool used to compute metadata

            conflict_update: Flag to determine if we want to overwrite (true)
              or skip duplicates (false, the default)

        """
        db.mktemp_revision_metadata(cur)

        db.copy_to(metadata, 'tmp_revision_metadata',
                   ['id', 'translated_metadata', 'indexer_configuration_id'],
                   cur)
        db.revision_metadata_add_from_temp(conflict_update, cur)

    @remote_api_endpoint('origin_intrinsic_metadata')
    @db_transaction_generator()
    def origin_intrinsic_metadata_get(self, ids, db=None, cur=None):
        """Retrieve origin metadata per id.

        Args:
            ids (iterable): origin identifiers

        Yields:
            list: dictionaries with the following keys:

                - **origin_id** (int)
                - **metadata** (str): associated metadata
                - **tool** (dict): tool used to compute metadata

        """
        for c in db.origin_intrinsic_metadata_get_from_list(ids, cur):
            yield converters.db_to_metadata(
                dict(zip(db.origin_intrinsic_metadata_cols, c)))

    @remote_api_endpoint('origin_intrinsic_metadata/add')
    @db_transaction()
    def origin_intrinsic_metadata_add(self, metadata,
                                      conflict_update=False, db=None,
                                      cur=None):
        """Add origin metadata not present in storage.

        Args:
            metadata (iterable): dictionaries with keys:

                - **origin_id**: origin identifier
                - **from_revision**: sha1 id of the revision used to generate
                  these metadata.
                - **metadata**: arbitrary dict
                - **indexer_configuration_id**: tool used to compute metadata

            conflict_update: Flag to determine if we want to overwrite (true)
              or skip duplicates (false, the default)

        """
        db.mktemp_origin_intrinsic_metadata(cur)

        db.copy_to(metadata, 'tmp_origin_intrinsic_metadata',
                   ['origin_id', 'metadata', 'indexer_configuration_id',
                       'from_revision'],
                   cur)
        db.origin_intrinsic_metadata_add_from_temp(conflict_update, cur)

    @remote_api_endpoint('origin_intrinsic_metadata/search/fulltext')
    @db_transaction_generator()
    def origin_intrinsic_metadata_search_fulltext(
            self, conjunction, limit=100, db=None, cur=None):
        """Returns the list of origins whose metadata contain all the terms.

        Args:
            conjunction (List[str]): List of terms to be searched for.
            limit (int): The maximum number of results to return

        Yields:
            list: dictionaries with the following keys:

                - **id** (int)
                - **metadata** (str): associated metadata
                - **tool** (dict): tool used to compute metadata

        """
        for c in db.origin_intrinsic_metadata_search_fulltext(
                conjunction, limit=limit, cur=cur):
            yield converters.db_to_metadata(
                dict(zip(db.origin_intrinsic_metadata_cols, c)))

    @remote_api_endpoint('indexer_configuration/add')
    @db_transaction_generator()
    def indexer_configuration_add(self, tools, db=None, cur=None):
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
        db.mktemp_indexer_configuration(cur)
        db.copy_to(tools, 'tmp_indexer_configuration',
                   ['tool_name', 'tool_version', 'tool_configuration'],
                   cur)

        tools = db.indexer_configuration_add_from_temp(cur)
        for line in tools:
            yield dict(zip(db.indexer_configuration_cols, line))

    @remote_api_endpoint('indexer_configuration/data')
    @db_transaction()
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
        tool_conf = tool['tool_configuration']
        if isinstance(tool_conf, dict):
            tool_conf = json.dumps(tool_conf)
        idx = db.indexer_configuration_get(tool['tool_name'],
                                           tool['tool_version'],
                                           tool_conf)
        if not idx:
            return None
        return dict(zip(db.indexer_configuration_cols, idx))
