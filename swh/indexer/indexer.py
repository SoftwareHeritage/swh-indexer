# Copyright (C) 2016-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import abc
import os
import logging
import shutil
import tempfile
import datetime
from copy import deepcopy
from contextlib import contextmanager

from swh.scheduler import get_scheduler
from swh.scheduler import CONFIG as SWH_CONFIG

from swh.storage import get_storage
from swh.core.config import SWHConfig
from swh.objstorage import get_objstorage
from swh.objstorage.exc import ObjNotFoundError
from swh.indexer.storage import get_indexer_storage, INDEXER_CFG_KEY
from swh.model import hashutil
from swh.core import utils


@contextmanager
def write_to_temp(filename, data, working_directory):
    """Write the sha1's content in a temporary file.

    Args:
        filename (str): one of sha1's many filenames
        data (bytes): the sha1's content to write in temporary
          file

    Returns:
        The path to the temporary file created. That file is
        filled in with the raw content's data.

    """
    os.makedirs(working_directory, exist_ok=True)
    temp_dir = tempfile.mkdtemp(dir=working_directory)
    content_path = os.path.join(temp_dir, filename)

    with open(content_path, 'wb') as f:
        f.write(data)

    yield content_path
    shutil.rmtree(temp_dir)


class BaseIndexer(SWHConfig, metaclass=abc.ABCMeta):
    """Base class for indexers to inherit from.

    The main entry point is the :func:`run` function which is in
    charge of triggering the computations on the batch dict/ids
    received.

    Indexers can:

    - filter out ids whose data has already been indexed.
    - retrieve ids data from storage or objstorage
    - index this data depending on the object and store the result in
      storage.

    To implement a new object type indexer, inherit from the
    BaseIndexer and implement indexing:

    :meth:`~BaseIndexer.run`:
      object_ids are different depending on object. For example: sha1 for
      content, sha1_git for revision, directory, release, and id for origin

    To implement a new concrete indexer, inherit from the object level
    classes: :class:`ContentIndexer`, :class:`RevisionIndexer`,
    :class:`OriginIndexer`.

    Then you need to implement the following functions:

    :meth:`~BaseIndexer.filter`:
      filter out data already indexed (in storage).

    :meth:`~BaseIndexer.index_object`:
      compute index on id with data (retrieved from the storage or the
      objstorage by the id key) and return the resulting index computation.

    :meth:`~BaseIndexer.persist_index_computations`:
      persist the results of multiple index computations in the storage.

    The new indexer implementation can also override the following functions:

    :meth:`~BaseIndexer.prepare`:
      Configuration preparation for the indexer.  When overriding, this must
      call the `super().prepare()` instruction.

    :meth:`~BaseIndexer.check`:
      Configuration check for the indexer.  When overriding, this must call the
      `super().check()` instruction.

    :meth:`~BaseIndexer.register_tools`:
      This should return a dict of the tool(s) to use when indexing or
      filtering.

    """
    CONFIG = 'indexer/base'

    DEFAULT_CONFIG = {
        INDEXER_CFG_KEY: ('dict', {
            'cls': 'remote',
            'args': {
                'url': 'http://localhost:5007/'
            }
        }),
        'storage': ('dict', {
            'cls': 'remote',
            'args': {
                'url': 'http://localhost:5002/',
            }
        }),
        'objstorage': ('dict', {
            'cls': 'remote',
            'args': {
                'url': 'http://localhost:5003/',
            }
        })
    }

    ADDITIONAL_CONFIG = {}

    USE_TOOLS = True

    catch_exceptions = True
    """Prevents exceptions in `index()` from raising too high. Set to False
    in tests to properly catch all exceptions."""

    def __init__(self, config=None, **kw):
        """Prepare and check that the indexer is ready to run.

        """
        super().__init__()
        if config is not None:
            self.config = config
        elif SWH_CONFIG:
            self.config = SWH_CONFIG.copy()
        else:
            config_keys = ('base_filename', 'config_filename',
                           'additional_configs', 'global_config')
            config_args = {k: v for k, v in kw.items() if k in config_keys}
            if self.ADDITIONAL_CONFIG:
                config_args.setdefault('additional_configs', []).append(
                    self.ADDITIONAL_CONFIG)
            self.config = self.parse_config_file(**config_args)
        self.prepare()
        self.check()
        self.log.debug('%s: config=%s', self, self.config)

    def prepare(self):
        """Prepare the indexer's needed runtime configuration.
           Without this step, the indexer cannot possibly run.

        """
        config_storage = self.config.get('storage')
        if config_storage:
            self.storage = get_storage(**config_storage)

        objstorage = self.config['objstorage']
        self.objstorage = get_objstorage(objstorage['cls'],
                                         objstorage['args'])

        idx_storage = self.config[INDEXER_CFG_KEY]
        self.idx_storage = get_indexer_storage(**idx_storage)

        _log = logging.getLogger('requests.packages.urllib3.connectionpool')
        _log.setLevel(logging.WARN)
        self.log = logging.getLogger('swh.indexer')

        if self.USE_TOOLS:
            self.tools = list(self.register_tools(
                self.config.get('tools', [])))
        self.results = []

    @property
    def tool(self):
        return self.tools[0]

    def check(self):
        """Check the indexer's configuration is ok before proceeding.
           If ok, does nothing. If not raise error.

        """
        if self.USE_TOOLS and not self.tools:
            raise ValueError('Tools %s is unknown, cannot continue' %
                             self.tools)

    def _prepare_tool(self, tool):
        """Prepare the tool dict to be compliant with the storage api.

        """
        return {'tool_%s' % key: value for key, value in tool.items()}

    def register_tools(self, tools):
        """Permit to register tools to the storage.

           Add a sensible default which can be overridden if not
           sufficient.  (For now, all indexers use only one tool)

           Expects the self.config['tools'] property to be set with
           one or more tools.

        Args:
            tools (dict/[dict]): Either a dict or a list of dict.

        Returns:
            list: List of dicts with additional id key.

        Raises:
            ValueError: if not a list nor a dict.

        """
        if isinstance(tools, list):
            tools = list(map(self._prepare_tool, tools))
        elif isinstance(tools, dict):
            tools = [self._prepare_tool(tools)]
        else:
            raise ValueError('Configuration tool(s) must be a dict or list!')

        if tools:
            return self.idx_storage.indexer_configuration_add(tools)
        else:
            return []

    def index(self, id, data):
        """Index computation for the id and associated raw data.

        Args:
            id (bytes): identifier
            data (bytes): id's data from storage or objstorage depending on
              object type

        Returns:
            dict: a dict that makes sense for the
            :meth:`.persist_index_computations` method.

        """
        raise NotImplementedError()

    def filter(self, ids):
        """Filter missing ids for that particular indexer.

        Args:
            ids ([bytes]): list of ids

        Yields:
            iterator of missing ids

        """
        yield from ids

    @abc.abstractmethod
    def persist_index_computations(self, results, policy_update):
        """Persist the computation resulting from the index.

        Args:

            results ([result]): List of results. One result is the
              result of the index function.
            policy_update ([str]): either 'update-dups' or 'ignore-dups' to
              respectively update duplicates or ignore them

        Returns:
            None

        """
        pass

    def next_step(self, results, task):
        """Do something else with computations results (e.g. send to another
        queue, ...).

        (This is not an abstractmethod since it is optional).

        Args:
            results ([result]): List of results (dict) as returned
              by index function.
            task (dict): a dict in the form expected by
              `scheduler.backend.SchedulerBackend.create_tasks`
              without `next_run`, plus an optional `result_name` key.

        Returns:
            None

        """
        if task:
            if getattr(self, 'scheduler', None):
                scheduler = self.scheduler
            else:
                scheduler = get_scheduler(**self.config['scheduler'])
            task = deepcopy(task)
            result_name = task.pop('result_name', None)
            task['next_run'] = datetime.datetime.now()
            if result_name:
                task['arguments']['kwargs'][result_name] = self.results
            scheduler.create_tasks([task])

    @abc.abstractmethod
    def run(self, ids, policy_update,
            next_step=None, **kwargs):
        """Given a list of ids:

        - retrieves the data from the storage
        - executes the indexing computations
        - stores the results (according to policy_update)

        Args:
            ids ([bytes]): id's identifier list
            policy_update (str): either 'update-dups' or 'ignore-dups' to
              respectively update duplicates or ignore them
            next_step (dict): a dict in the form expected by
              `scheduler.backend.SchedulerBackend.create_tasks`
              without `next_run`, plus a `result_name` key.
            **kwargs: passed to the `index` method

        """
        pass


class ContentIndexer(BaseIndexer):
    """A content indexer working on a list of ids directly.

    To work on indexer range, use the :class:`ContentRangeIndexer`
    instead.

    Note: :class:`ContentIndexer` is not an instantiable object. To
    use it, one should inherit from this class and override the
    methods mentioned in the :class:`BaseIndexer` class.

    """

    def run(self, ids, policy_update,
            next_step=None, **kwargs):
        """Given a list of ids:

        - retrieve the content from the storage
        - execute the indexing computations
        - store the results (according to policy_update)

        Args:
            ids (Iterable[Union[bytes, str]]): sha1's identifier list
            policy_update (str): either 'update-dups' or 'ignore-dups' to
                                 respectively update duplicates or ignore
                                 them
            next_step (dict): a dict in the form expected by
                        `scheduler.backend.SchedulerBackend.create_tasks`
                        without `next_run`, plus an optional `result_name` key.
            **kwargs: passed to the `index` method

        """
        ids = [hashutil.hash_to_bytes(id_) if isinstance(id_, str) else id_
               for id_ in ids]
        results = []
        try:
            for sha1 in ids:
                try:
                    raw_content = self.objstorage.get(sha1)
                except ObjNotFoundError:
                    self.log.warning('Content %s not found in objstorage' %
                                     hashutil.hash_to_hex(sha1))
                    continue
                res = self.index(sha1, raw_content, **kwargs)
                if res:  # If no results, skip it
                    results.append(res)

            self.persist_index_computations(results, policy_update)
            self.results = results
            return self.next_step(results, task=next_step)
        except Exception:
            if not self.catch_exceptions:
                raise
            self.log.exception(
                'Problem when reading contents metadata.')


class ContentRangeIndexer(BaseIndexer):
    """A content range indexer.

    This expects as input a range of ids to index.

    To work on a list of ids, use the :class:`ContentIndexer` instead.

    Note: :class:`ContentRangeIndexer` is not an instantiable
    object. To use it, one should inherit from this class and override
    the methods mentioned in the :class:`BaseIndexer` class.

    """
    @abc.abstractmethod
    def indexed_contents_in_range(self, start, end):
        """Retrieve indexed contents within range [start, end].

        Args:
            start (bytes): Starting bound from range identifier
            end (bytes): End range identifier

        Yields:
            bytes: Content identifier present in the range ``[start, end]``

        """
        pass

    def _list_contents_to_index(self, start, end, indexed):
        """Compute from storage the new contents to index in the range [start,
           end]. The already indexed contents are skipped.

        Args:
            start (bytes): Starting bound from range identifier
            end (bytes): End range identifier
            indexed (Set[bytes]): Set of content already indexed.

        Yields:
            bytes: Identifier of contents to index.

        """
        if not isinstance(start, bytes) or not isinstance(end, bytes):
            raise TypeError('identifiers must be bytes, not %r and %r.' %
                            (start, end))
        while start:
            result = self.storage.content_get_range(start, end)
            contents = result['contents']
            for c in contents:
                _id = hashutil.hash_to_bytes(c['sha1'])
                if _id in indexed:
                    continue
                yield _id
            start = result['next']

    def _index_contents(self, start, end, indexed, **kwargs):
        """Index the contents from within range [start, end]

        Args:
            start (bytes): Starting bound from range identifier
            end (bytes): End range identifier
            indexed (Set[bytes]): Set of content already indexed.

        Yields:
            dict: Data indexed to persist using the indexer storage

        """
        for sha1 in self._list_contents_to_index(start, end, indexed):
            try:
                raw_content = self.objstorage.get(sha1)
            except ObjNotFoundError:
                self.log.warning('Content %s not found in objstorage' %
                                 hashutil.hash_to_hex(sha1))
                continue
            res = self.index(sha1, raw_content, **kwargs)
            if res:
                if not isinstance(res['id'], bytes):
                    raise TypeError(
                        '%r.index should return ids as bytes, not %r' %
                        (self.__class__.__name__, res['id']))
                yield res

    def _index_with_skipping_already_done(self, start, end):
        """Index not already indexed contents in range [start, end].

        Args:
            start** (Union[bytes, str]): Starting range identifier
            end (Union[bytes, str]): Ending range identifier

        Yields:
            bytes: Content identifier present in the range
            ``[start, end]`` which are not already indexed.

        """
        while start:
            indexed_page = self.indexed_contents_in_range(start, end)
            contents = indexed_page['ids']
            _end = contents[-1] if contents else end
            yield from self._index_contents(
                    start, _end, contents)
            start = indexed_page['next']

    def run(self, start, end, skip_existing=True, **kwargs):
        """Given a range of content ids, compute the indexing computations on
           the contents within. Either the indexer is incremental
           (filter out existing computed data) or not (compute
           everything from scratch).

        Args:
            start (Union[bytes, str]): Starting range identifier
            end (Union[bytes, str]): Ending range identifier
            skip_existing (bool): Skip existing indexed data
              (default) or not
            **kwargs: passed to the `index` method

        Returns:
            bool: True if data was indexed, False otherwise.

        """
        with_indexed_data = False
        try:
            if isinstance(start, str):
                start = hashutil.hash_to_bytes(start)
            if isinstance(end, str):
                end = hashutil.hash_to_bytes(end)

            if skip_existing:
                gen = self._index_with_skipping_already_done(start, end)
            else:
                gen = self._index_contents(start, end, indexed=[])

            for results in utils.grouper(gen,
                                         n=self.config['write_batch_size']):
                self.persist_index_computations(
                    results, policy_update='update-dups')
                with_indexed_data = True
        except Exception:
            if not self.catch_exceptions:
                raise
            self.log.exception(
                'Problem when computing metadata.')
        finally:
            return with_indexed_data


class OriginIndexer(BaseIndexer):
    """An object type indexer, inherits from the :class:`BaseIndexer` and
    implements Origin indexing using the run method

    Note: the :class:`OriginIndexer` is not an instantiable object.
    To use it in another context one should inherit from this class
    and override the methods mentioned in the :class:`BaseIndexer`
    class.

    """
    def run(self, origin_urls, policy_update='update-dups',
            next_step=None, **kwargs):
        """Given a list of origin ids:

        - retrieve origins from storage
        - execute the indexing computations
        - store the results (according to policy_update)

        Args:
            ids ([Union[int, Tuple[str, bytes]]]): list of origin ids or
              (type, url) tuples.
            policy_update (str): either 'update-dups' or 'ignore-dups' to
              respectively update duplicates (default) or ignore them
            next_step (dict): a dict in the form expected by
              `scheduler.backend.SchedulerBackend.create_tasks` without
              `next_run`, plus an optional `result_name` key.
            parse_ids (bool): Do we need to parse id or not (default)
            **kwargs: passed to the `index` method

        """
        results = self.index_list(origin_urls, **kwargs)

        self.persist_index_computations(results, policy_update)
        self.results = results
        return self.next_step(results, task=next_step)

    def index_list(self, origins, **kwargs):
        results = []
        for origin in origins:
            try:
                res = self.index(origin, **kwargs)
                if res:  # If no results, skip it
                    results.append(res)
            except Exception:
                if not self.catch_exceptions:
                    raise
                self.log.exception(
                    'Problem when processing origin %s',
                    origin['id'])
        return results


class RevisionIndexer(BaseIndexer):
    """An object type indexer, inherits from the :class:`BaseIndexer` and
    implements Revision indexing using the run method

    Note: the :class:`RevisionIndexer` is not an instantiable object.
    To use it in another context one should inherit from this class
    and override the methods mentioned in the :class:`BaseIndexer`
    class.

    """
    def run(self, ids, policy_update, next_step=None):
        """Given a list of sha1_gits:

        - retrieve revisions from storage
        - execute the indexing computations
        - store the results (according to policy_update)

        Args:
            ids ([bytes or str]): sha1_git's identifier list
            policy_update (str): either 'update-dups' or 'ignore-dups' to
              respectively update duplicates or ignore them

        """
        results = []
        ids = [hashutil.hash_to_bytes(id_) if isinstance(id_, str) else id_
               for id_ in ids]
        revs = self.storage.revision_get(ids)

        for rev in revs:
            if not rev:
                self.log.warning('Revisions %s not found in storage' %
                                 list(map(hashutil.hash_to_hex, ids)))
                continue
            try:
                res = self.index(rev)
                if res:  # If no results, skip it
                    results.append(res)
            except Exception:
                if not self.catch_exceptions:
                    raise
                self.log.exception(
                        'Problem when processing revision')
        self.persist_index_computations(results, policy_update)
        self.results = results
        return self.next_step(results, task=next_step)
