# Copyright (C) 2016-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import abc
import os
import logging
import shutil
import tempfile

from swh.core.config import SWHConfig
from swh.objstorage import get_objstorage
from swh.objstorage.exc import ObjNotFoundError
from swh.model import hashutil
from swh.storage import get_storage
from swh.scheduler.utils import get_task


class DiskIndexer:
    """Mixin intended to be used with other *Indexer classes.

       Indexer* inheriting from this class are a category of indexers
       which needs the disk for their computations.

       Expects:
           self.working_directory variable defined at runtime.

    """
    def __init__(self):
        super().__init__()

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

    The main entry point is the `run` functions which is in charge to
    trigger the computations on the ids batch received.

    Indexers can:
    - filter out ids whose data has already been indexed.
    - retrieve ids data from storage or objstorage
    - index this data depending on the object and store the result in storage.

    To implement a new object type indexer, inherit from the BaseIndexer and
    implement the process of indexation :

        - def run(self, object_ids, policy_update): object_ids are different
        depending on object. For example: sha1 for content, sha1_git for
        revision, directorie, release, and id for origin

    To implement a new concrete indexer, inherit from the object level classes:
    ContentIndexer, RevisionIndexer
    (later on OriginIndexer will also be available)

    Then you need to implement the following functions:

      - def filter(self, ids): filter out data already
        indexed (in storage). This function is used by the
        orchestrator and not directly by the indexer
        (cf. swh.indexer.orchestrator.BaseOrchestratorIndexer).

      - def index_object(self, id, data): compute index on
        id with data (retrieved from the storage or the objstorage by the
        id key) and return the resulting index computation.

      - def persist_index_computations(self, results, policy_update):
        persist the results of multiple index computations in the
        storage.

    The new indexer implementation can also override the following functions:

      - def prepare(self): Configuration preparation for the indexer.
        When overriding, this must call the super().prepare() function.

      - def check(self): Configuration check for the indexer.
        When overriding, this must call the super().check() function.

      - def retrieve_tools_information(self): This should return a
        dict of the tool(s) to use when indexing or filtering.

    """
    CONFIG = 'indexer/base'

    DEFAULT_CONFIG = {
        'storage': ('dict', {
            'host': 'uffizi',
            'cls': 'remote',
            'args': {'root': '/tmp/softwareheritage/objects',
                     'slicing': '0:2/2:4/4:6'}
        }),
        # queue to reschedule if problem (none for no rescheduling,
        # the default)
        'rescheduling_task': ('str', None),
        'objstorage': ('dict', {
            'cls': 'multiplexer',
            'args': {
                'objstorages': [{
                    'cls': 'filtered',
                    'args': {
                        'storage_conf': {
                            'cls': 'azure-storage',
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
                            'cls': 'azure-storage',
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
        objstorage = self.config['objstorage']
        self.objstorage = get_objstorage(objstorage['cls'], objstorage['args'])
        storage = self.config['storage']
        self.storage = get_storage(storage['cls'], storage['args'])
        rescheduling_task = self.config['rescheduling_task']
        if rescheduling_task:
            self.rescheduling_task = get_task(rescheduling_task)
        else:
            self.rescheduling_task = None

        l = logging.getLogger('requests.packages.urllib3.connectionpool')
        l.setLevel(logging.WARN)
        self.log = logging.getLogger('swh.indexer')
        self.tools = self.retrieve_tools_information()

    def check(self):
        """Check the indexer's configuration is ok before proceeding.
           If ok, does nothing. If not raise error.

        """
        if not self.tools:
            raise ValueError('Tools %s is unknown, cannot continue' %
                             self.config['tools'])

    def retrieve_tools_information(self):
        """Permit to define how to retrieve tool information based on
           configuration.

           Add a sensible default which can be overridden if not
           sufficient.  (For now, all indexers use only one tool)

        """
        tool = {
            'tool_%s' % key: value for key, value
            in self.config['tools'].items()
        }
        return self.storage.indexer_configuration_get(tool)

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
        """Index computation for the sha1 and associated raw content.

        Args:
            id (bytes): sha1 identifier
            content (bytes): id's data from storage or objstorage depending on
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
            respectively update duplicates or ignore them

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
    def run(self, ids, policy_update):
        """Given a list of ids:
        - retrieves the data from the storage
        - executes the indexing computations
        - stores the results (according to policy_update)

        Args:
            ids ([bytes]): id's identifier list
            policy_update ([str]): either 'update-dups' or 'ignore-dups' to
            respectively update duplicates or ignore them

        """
        pass


class ContentIndexer(BaseIndexer):
    """
    An object type indexer, inherit from the BaseIndexer and
    implement the process of indexation for Contents with the run method

    Note: the ContentIndexer is not an instantiable object
    to use it in another context one should refer to the instructions in the
    BaseIndexer
    """

    def run(self, sha1s, policy_update):
        """Given a list of sha1s:
        - retrieve the content from the storage
        - execute the indexing computations
        - store the results (according to policy_update)

        Args:
            sha1s ([bytes]): sha1's identifier list
            policy_update ([str]): either 'update-dups' or 'ignore-dups' to
            respectively update duplicates or ignore them

        """
        results = []
        try:
            for sha1 in sha1s:
                try:
                    raw_content = self.objstorage.get(sha1)
                except ObjNotFoundError:
                    self.log.warn('Content %s not found in objstorage' %
                                  hashutil.hash_to_hex(sha1))
                    continue
                res = self.index(sha1, raw_content)
                if res:  # If no results, skip it
                    results.append(res)

            self.persist_index_computations(results, policy_update)
            self.next_step(results)
        except Exception:
            self.log.exception(
                'Problem when reading contents metadata.')
            if self.rescheduling_task:
                self.log.warn('Rescheduling batch')
                self.rescheduling_task.delay(sha1s, policy_update)


class RevisionIndexer(BaseIndexer):
    """
    An object type indexer, inherit from the BaseIndexer and
    implement the process of indexation for Revisions with the run method

    Note: the RevisionIndexer is not an instantiable object
    to use it in another context one should refer to the instructions in the
    BaseIndexer
    """

    def run(self, sha1_gits, policy_update):
        """
        Given a list of sha1_gits:
        - retrieve revsions from storage
        - execute the indexing computations
        - store the results (according to policy_update)
        Args:
            sha1_gits ([bytes]): sha1_git's identifier list
            policy_update ([str]): either 'update-dups' or 'ignore-dups' to
            respectively update duplicates or ignore them

        """
        results = []
        try:
            for sha1_git in sha1_gits:
                try:
                    revs = self.storage.revision_get([sha1_git])
                except ValueError:
                    self.log.warn('Revision %s not found in storage' %
                                  hashutil.hash_to_hex(sha1_git))
                    continue
                for rev in revs:
                    if rev:      # If no revision, skip it
                        res = self.index(rev)
                        print(res)
                        if res:  # If no results, skip it
                            results.append(res)
                self.persist_index_computations(results, policy_update)
        except Exception:
            self.log.exception(
                'Problem when processing revision')
