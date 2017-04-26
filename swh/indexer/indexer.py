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


class BaseIndexer(SWHConfig,
                  metaclass=abc.ABCMeta):
    """Base class for indexers to inherit from.

    The main entry point is the `run` functions which is in charge to
    trigger the computations on the sha1s batch receiived as
    parameter.

    Indexers can:
    - filter out sha1 whose data has already been indexed.
    - retrieve sha1's content from objstorage, index this content then
      store the result in storage.

    Thus the following interface to implement per inheriting class:
      - def filter_contents(self, sha1s): filter out data already
        indexed (in storage)

      - def index_content(self, sha1, content): compute index on sha1 with
        data content (stored by sha1 in objstorage) and store result
        in storage.

      - def persist_index_computations(self, results, policy_update):
        the function to store the results (as per index_content
        defined).

    """
    CONFIG_BASE_FILENAME = 'indexer/base'

    DEFAULT_CONFIG = {
        'storage': ('dict', {
            'host': 'uffizi',
            'cls': 'remote',
            'args': {'root': '/tmp/softwareheritage/objects',
                     'slicing': '0:2/2:4/4:6'}
        }),
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
        super().__init__()
        self.config = self.parse_config_file(
            additional_configs=[self.ADDITIONAL_CONFIG])
        objstorage = self.config['objstorage']
        self.objstorage = get_objstorage(objstorage['cls'], objstorage['args'])
        storage = self.config['storage']
        self.storage = get_storage(storage['cls'], storage['args'])
        l = logging.getLogger('requests.packages.urllib3.connectionpool')
        l.setLevel(logging.WARN)
        self.log = logging.getLogger('swh.indexer')

    @abc.abstractmethod
    def filter_contents(self, sha1s):
        """Filter missing sha1 for that particular indexer.

        Args:
            sha1s ([bytes]): list of contents' sha1

        Yields:
            iterator of missing sha1

        """
        pass

    @abc.abstractmethod
    def index_content(self, sha1, content):
        """Index computation for the sha1 and associated raw content.

        Args:
            sha1 (bytes): sha1 identifier
            content (bytes): sha1's raw content

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
            result of the index_content function.
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
            by index_content function.

        Returns:
            None

        """
        pass

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
        for sha1 in sha1s:
            try:
                raw_content = self.objstorage.get(sha1)
            except ObjNotFoundError:
                self.log.warn('Content %s not found in objstorage' %
                              hashutil.hash_to_hex(sha1))
                continue
            res = self.index_content(sha1, raw_content)
            if res:  # If no results, skip it
                results.append(res)

        self.persist_index_computations(results, policy_update)
        self.next_step(results)


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
