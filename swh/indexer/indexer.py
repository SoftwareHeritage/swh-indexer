# Copyright (C) 2016-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import abc
import os
import logging
import shutil
import tempfile

from swh.storage import get_storage
from swh.core.config import SWHConfig
from swh.objstorage import get_objstorage
from swh.objstorage.exc import ObjNotFoundError
from swh.model import hashutil
from swh.scheduler.utils import get_task
from swh.indexer.storage import get_indexer_storage, INDEXER_CFG_KEY


class DiskIndexer:
    """Mixin intended to be used with other SomethingIndexer classes.

       Indexers inheriting from this class are a category of indexers
       which needs the disk for their computations.

       Note:
           This expects `self.working_directory` variable defined at
           runtime.

    """
    def write_to_temp(self, filename, data):
        """Write the sha1's content in a temporary file.

        Args:
            sha1 (str): the sha1 name
            filename (str): one of sha1's many filenames
            data (bytes): the sha1's content to write in temporary
            file

        Returns:
            The path to the temporary file created. That file is
            filled in with the raw content's data.

        """
        os.makedirs(self.working_directory, exist_ok=True)
        temp_dir = tempfile.mkdtemp(dir=self.working_directory)
        content_path = os.path.join(temp_dir, filename)

        with open(content_path, 'wb') as f:
            f.write(data)

        return content_path

    def cleanup(self, content_path):
        """Remove content_path from working directory.

        Args:
            content_path (str): the file to remove

        """
        temp_dir = os.path.dirname(content_path)
        shutil.rmtree(temp_dir)


class BaseIndexer(SWHConfig,
                  metaclass=abc.ABCMeta):
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

    :func:`run`:
      object_ids are different depending on object. For example: sha1 for
      content, sha1_git for revision, directory, release, and id for origin

    To implement a new concrete indexer, inherit from the object level
    classes: :class:`ContentIndexer`, :class:`RevisionIndexer`,
    :class:`OriginIndexer`.

    Then you need to implement the following functions:

    :func:`filter`:
      filter out data already indexed (in storage). This function is used by
      the orchestrator and not directly by the indexer
      (cf. swh.indexer.orchestrator.BaseOrchestratorIndexer).

    :func:`index_object`:
      compute index on id with data (retrieved from the storage or the
      objstorage by the id key) and return the resulting index computation.

    :func:`persist_index_computations`:
      persist the results of multiple index computations in the storage.

    The new indexer implementation can also override the following functions:

    :func:`prepare`:
      Configuration preparation for the indexer.  When overriding, this must
      call the `super().prepare()` instruction.

    :func:`check`:
      Configuration check for the indexer.  When overriding, this must call the
      `super().check()` instruction.

    :func:`register_tools`:
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

        # queue to reschedule if problem (none for no rescheduling,
        # the default)
        'rescheduling_task': ('str', None),
        'storage': ('dict', {
            'cls': 'remote',
            'args': {
                'url': 'http://localhost:5002/',
            }
        }),
        'objstorage': ('dict', {
            'cls': 'multiplexer',
            'args': {
                'objstorages': [{
                    'cls': 'filtered',
                    'args': {
                        'storage_conf': {
                            'cls': 'azure',
                            'args': {
                                'account_name': '0euwestswh',
                                'api_secret_key': 'secret',
                                'container_name': 'contents'
                            }
                        },
                        'filters_conf': [
                            {'type': 'readonly'},
                            {'type': 'prefix', 'prefix': '0'}
                        ]
                    }
                }, {
                    'cls': 'filtered',
                    'args': {
                        'storage_conf': {
                            'cls': 'azure',
                            'args': {
                                'account_name': '1euwestswh',
                                'api_secret_key': 'secret',
                                'container_name': 'contents'
                            }
                        },
                        'filters_conf': [
                            {'type': 'readonly'},
                            {'type': 'prefix', 'prefix': '1'}
                        ]
                    }
                }]
            },
        }),
    }

    ADDITIONAL_CONFIG = {}

    def __init__(self):
        """Prepare and check that the indexer is ready to run.

        """
        super().__init__()
        self.prepare()
        self.check()

    def prepare(self):
        """Prepare the indexer's needed runtime configuration.
           Without this step, the indexer cannot possibly run.

        """
        self.config = self.parse_config_file(
            additional_configs=[self.ADDITIONAL_CONFIG])
        if self.config['storage']:
            self.storage = get_storage(**self.config['storage'])
        objstorage = self.config['objstorage']
        self.objstorage = get_objstorage(objstorage['cls'], objstorage['args'])
        idx_storage = self.config[INDEXER_CFG_KEY]
        self.idx_storage = get_indexer_storage(**idx_storage)
        rescheduling_task = self.config['rescheduling_task']
        if rescheduling_task:
            self.rescheduling_task = get_task(rescheduling_task)
        else:
            self.rescheduling_task = None

        _log = logging.getLogger('requests.packages.urllib3.connectionpool')
        _log.setLevel(logging.WARN)
        self.log = logging.getLogger('swh.indexer')
        self.tools = list(self.register_tools(self.config['tools']))

    def check(self):
        """Check the indexer's configuration is ok before proceeding.
           If ok, does nothing. If not raise error.

        """
        if not self.tools:
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
            List of dict with additional id key.

        Raises:
            ValueError if not a list nor a dict.

        """
        tools = self.config['tools']
        if isinstance(tools, list):
            tools = map(self._prepare_tool, tools)
        elif isinstance(tools, dict):
            tools = [self._prepare_tool(tools)]
        else:
            raise ValueError('Configuration tool(s) must be a dict or list!')

        return self.idx_storage.indexer_configuration_add(tools)

    @abc.abstractmethod
    def filter(self, ids):
        """Filter missing ids for that particular indexer.

        Args:
            ids ([bytes]): list of ids

        Yields:
            iterator of missing ids

        """
        pass

    @abc.abstractmethod
    def index(self, id, data):
        """Index computation for the id and associated raw data.

        Args:
            id (bytes): identifier
            data (bytes): id's data from storage or objstorage depending on
                             object type

        Returns:
            a dict that makes sense for the persist_index_computations
        function.

        """
        pass

    @abc.abstractmethod
    def persist_index_computations(self, results, policy_update):
        """Persist the computation resulting from the index.

        Args:

            results ([result]): List of results. One result is the
                                result of the index function.
            policy_update ([str]): either 'update-dups' or 'ignore-dups' to
                                   respectively update duplicates or ignore
                                   them

        Returns:
            None

        """
        pass

    def next_step(self, results):
        """Do something else with computations results (e.g. send to another
        queue, ...).

        (This is not an abstractmethod since it is optional).

        Args:
            results ([result]): List of results (dict) as returned
                                by index function.

        Returns:
            None

        """
        pass

    @abc.abstractmethod
    def run(self, ids, policy_update, **kwargs):
        """Given a list of ids:

        - retrieves the data from the storage
        - executes the indexing computations
        - stores the results (according to policy_update)

        Args:
            ids ([bytes]): id's identifier list
            policy_update ([str]): either 'update-dups' or 'ignore-dups' to
            respectively update duplicates or ignore them
            **kwargs: passed to the `index` method

        """
        pass


class ContentIndexer(BaseIndexer):
    """An object type indexer, inherits from the :class:`BaseIndexer` and
    implements Content indexing using the run method

    Note: the :class:`ContentIndexer` is not an instantiable
    object. To use it in another context, one should inherit from this
    class and override the methods mentioned in the
    :class:`BaseIndexer` class.

    """

    def run(self, ids, policy_update, **kwargs):
        """Given a list of ids:

        - retrieve the content from the storage
        - execute the indexing computations
        - store the results (according to policy_update)

        Args:
            ids ([bytes]): sha1's identifier list
            policy_update ([str]): either 'update-dups' or 'ignore-dups' to
                                   respectively update duplicates or ignore
                                   them
            **kwargs: passed to the `index` method

        """
        results = []
        try:
            for sha1 in ids:
                try:
                    raw_content = self.objstorage.get(sha1)
                except ObjNotFoundError:
                    self.log.warn('Content %s not found in objstorage' %
                                  hashutil.hash_to_hex(sha1))
                    continue
                res = self.index(sha1, raw_content, **kwargs)
                if res:  # If no results, skip it
                    results.append(res)

            self.persist_index_computations(results, policy_update)
            self.results = results
            return self.next_step(results)
        except Exception:
            self.log.exception(
                'Problem when reading contents metadata.')
            if self.rescheduling_task:
                self.log.warn('Rescheduling batch')
                self.rescheduling_task.delay(ids, policy_update)


class OriginIndexer(BaseIndexer):
    """An object type indexer, inherits from the :class:`BaseIndexer` and
    implements Origin indexing using the run method

    Note: the :class:`OriginIndexer` is not an instantiable object.
    To use it in another context one should inherit from this class
    and override the methods mentioned in the :class:`BaseIndexer`
    class.

    """
    def run(self, ids, policy_update, parse_ids=False, **kwargs):
        """Given a list of origin ids:

        - retrieve origins from storage
        - execute the indexing computations
        - store the results (according to policy_update)

        Args:
            ids ([Union[int, Tuple[str, bytes]]]): list of origin ids or
                                                   (type, url) tuples.
            policy_update ([str]): either 'update-dups' or 'ignore-dups' to
                                   respectively update duplicates or ignore
                                   them
            parse_ids ([bool]: If `True`, will try to convert `ids`
                               from a human input to the valid type.
            **kwargs: passed to the `index` method

        """
        if parse_ids:
            ids = [
                    o.split('+', 1) if ':' in o else int(o)  # type+url or id
                    for o in ids]

        results = []

        for id_ in ids:
            if isinstance(id_, (tuple, list)):
                if len(id_) != 2:
                    raise TypeError('Expected a (type, url) tuple.')
                (type_, url) = id_
                params = {'type': type_, 'url': url}
            elif isinstance(id_, int):
                params = {'id': id_}
            else:
                raise TypeError('Invalid value in "ids": %r' % id_)
            origin = self.storage.origin_get(params)
            if not origin:
                self.log.warn('Origins %s not found in storage' %
                              list(ids))
                continue
            try:
                res = self.index(origin, **kwargs)
                if origin:  # If no results, skip it
                    results.append(res)
            except Exception:
                self.log.exception(
                        'Problem when processing origin %s' % id_)
        self.persist_index_computations(results, policy_update)
        self.results = results
        return self.next_step(results)


class RevisionIndexer(BaseIndexer):
    """An object type indexer, inherits from the :class:`BaseIndexer` and
    implements Revision indexing using the run method

    Note: the :class:`RevisionIndexer` is not an instantiable object.
    To use it in another context one should inherit from this class
    and override the methods mentioned in the :class:`BaseIndexer`
    class.

    """
    def run(self, ids, policy_update):
        """Given a list of sha1_gits:

        - retrieve revisions from storage
        - execute the indexing computations
        - store the results (according to policy_update)

        Args:
            ids ([bytes]): sha1_git's identifier list
            policy_update ([str]): either 'update-dups' or 'ignore-dups' to
                                   respectively update duplicates or ignore
                                   them

        """
        results = []
        revs = self.storage.revision_get(ids)

        for rev in revs:
            if not rev:
                self.log.warn('Revisions %s not found in storage' %
                              list(map(hashutil.hash_to_hex, ids)))
                continue
            try:
                res = self.index(rev)
                if res:  # If no results, skip it
                    results.append(res)
            except Exception:
                self.log.exception(
                        'Problem when processing revision')
        self.persist_index_computations(results, policy_update)
        self.results = results
        return self.next_step(results)
